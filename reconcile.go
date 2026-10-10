package main

// Per-table reconciliation, cheapest check first:
//  1. metadata: row counts (sys.partitions); an empty target is reloaded
//  2. change probe: sys.dm_db_index_usage_stats says "not modified since the last sync" -> skipped
//     without reading the table (trusted for at most 24h, then verified by a real pass)
//  3. rowversion column, if the table has one: only rows changed since the last mark are compared
//  4. IBLT (invertible Bloom lookup table) over the whole table, or the newest PK window: one hashing
//     scan per side (the target side often comes from the cell cache), decoded by the worker into
//     the keys of inserted, updated and deleted rows
//  5. the differences are applied in short transactions located by primary-key index seeks

import (
	"database/sql"
	"fmt"
	"time"
)

// ibltCells: the only full scan per side for a changed table. Identical tables give identical
// cells, so there is no separate checksum pass. where/args restrict it to a PK window.
func (ds *dataSync) ibltCells(s *side, m *tableMeta, size int, scanRows int64, where string, args []any) (map[int]ibltCell, error) {
	q := s.scanPrefix() + fmt.Sprintf(`SELECT p.cell, COUNT_BIG(*), SUM(%s), SUM(%s), SUM(%s), SUM(%s), SUM(%s), SUM(%s), SUM(%s)
FROM %s AS t
%s
CROSS APPLY (SELECT (%s %% 2147483647 + 2147483647) %% 2147483647 AS a, (%s %% 2147483647 + 2147483647) %% 2147483647 AS b) y
CROSS APPLY (VALUES (y.a %% @s), (@s + y.b %% @s), (2 * @s + y.a * y.b %% 2147483647 %% @s)) p(cell)%s
GROUP BY p.cell`, i32("x.h", 1), i32("x.h", 5), i32("x.k", 1), i32("x.k", 5), i32("x.k", 9), i32("x.k", 13),
		"(y.a * y.a % 2147483647 + y.b * y.b % 2147483647) % 2147483647",
		s.ref(m), m.hashApply("t"), i32("x.h", 1), i32("x.h", 5), where) + fmt.Sprintf(" OPTION (MAXDOP %d)", s.scanDop(scanRows, m))
	ds.written(q, 4)
	rows, err := s.exec.QueryContext(ds.rc.ctx, q, append([]any{sql.Named("s", size)}, args...)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	cells := map[int]ibltCell{}
	for rows.Next() {
		var i int
		var c ibltCell
		if err := rows.Scan(&i, &c.n, &c.fh, &c.fl, &c.kh, &c.kl, &c.k2h, &c.k2l, &c.gs); err != nil {
			return nil, err
		}
		cells[i] = c
	}
	ds.in(int64(len(cells)) * 72)
	return cells, rows.Err()
}

func cellRows(c map[int]ibltCell) int64 {
	var n int64
	for _, v := range c {
		n += v.n
	}
	return n / ibltK
}

func cellsEqual(a, b map[int]ibltCell) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if b[i] != v {
			return false
		}
	}
	return true
}

// probe answers "was the source table modified since checkedAt?" from sys.dm_db_index_usage_stats
// without touching the table. checkpoint is the safe time to store after this run: the earlier of
// now and the start of the oldest open read/write transaction (its changes may commit later).
type probe struct {
	ok, unchanged bool
	checkpoint    string
}

func (ds *dataSync) probeChanges(m *tableMeta, checkedAt string) probe {
	if ds.sh.noProbe.Load() {
		return probe{}
	}
	var unchanged bool
	var cp string
	err := ds.src.meta.QueryRowContext(ds.rc.ctx, `
DECLARE @since datetime2 = TRY_CAST(@c AS datetime2);
SELECT CAST(CASE WHEN @since IS NOT NULL
        AND @since > (SELECT sqlserver_start_time FROM sys.dm_os_sys_info)
        AND NOT EXISTS (SELECT 1 FROM sys.dm_db_index_usage_stats u
                        WHERE u.database_id = DB_ID() AND u.object_id = OBJECT_ID(@t) AND u.last_user_update >= @since)
       THEN 1 ELSE 0 END AS bit),
       CONVERT(varchar(33), (SELECT MIN(v) FROM (VALUES (SYSDATETIME()),
           ((SELECT CAST(MIN(transaction_begin_time) AS datetime2) FROM sys.dm_tran_active_transactions WHERE transaction_type = 1))) x(v)), 126)`,
		sql.Named("c", checkedAt), sql.Named("t", qi(m.Schema)+"."+qi(m.Name))).Scan(&unchanged, &cp)
	if err != nil {
		ds.sh.noProbe.Store(true)
		ds.rc.log.Printf("Change detector off (needs the VIEW SERVER STATE permission): %v", err)
		return probe{}
	}
	return probe{ok: true, unchanged: unchanged, checkpoint: cp}
}

// rowVersionMark: COUNT + MAX(rowversion) — fallback detector when the probe is unavailable.
func (ds *dataSync) rowVersionMark(m *tableMeta) (string, error) {
	q := ds.src.scanPrefix() + fmt.Sprintf("SELECT CONCAT(COUNT_BIG(*), ':', CONVERT(varchar(20), MAX(%s), 1)) FROM %s",
		qi(m.RowVersion), ds.src.ref(m)) + maxdop()
	ds.written(q, 0)
	var mark string
	err := ds.src.exec.QueryRowContext(ds.rc.ctx, q).Scan(&mark)
	ds.in(int64(len(mark)))
	return mark, err
}

// windowFrom: smallest PK among the newest w rows (index seek, no hashing).
func (ds *dataSync) windowFrom(m *tableMeta, w int) (int64, error) {
	pk := m.colRef("t", m.PK[0])
	q := ds.src.scanPrefix() + fmt.Sprintf("SELECT ISNULL(MIN(v), 0) FROM (SELECT TOP (@w) CAST(%s AS bigint) AS v FROM %s AS t ORDER BY %s DESC) x",
		pk, ds.src.ref(m), pk)
	ds.written(q, 4)
	var from int64
	err := ds.src.exec.QueryRowContext(ds.rc.ctx, q, sql.Named("w", w)).Scan(&from)
	return from, err
}

func (ds *dataSync) fullCheckDue(ts TableState) bool {
	if ts.LastFull == "" {
		return true
	}
	expr := ds.rc.job.Config.Data.FullCheck
	if expr == "" {
		expr = defaultFullCheck
	}
	sched, err := parseCron(expr)
	last, perr := time.Parse(time.RFC3339, ts.LastFull)
	if err != nil || perr != nil {
		return true
	}
	return !sched.Next(last.In(ds.rc.cfg.TZ)).After(time.Now())
}

func (ds *dataSync) syncTable(t tableRef) error {
	ctx, log := ds.rc.ctx, ds.rc.log
	key := t.schema + "." + t.name
	sm, err := loadMeta(ctx, ds.src.meta, t.schema, t.name)
	if err != nil {
		return err
	}
	tm, err := loadMeta(ctx, ds.tgt.meta, t.schema, t.name)
	if err != nil {
		return err
	}
	if err := checkSameColumns(sm, tm); err != nil {
		return err
	}
	sm.HasTriggers = tm.HasTriggers // triggers matter on the side we write to
	ts := ds.getTS(key)
	// probe first (one cheap DMV query): even a reload records the checkpoint it started from, so
	// the next run can skip the table without reading it
	pr := ds.probeChanges(sm, ts.CheckedAt)
	ds.lastProbe = pr
	ds.srcOps, ds.curMeta = ds.rowOps(ds.src, sm), sm // before reading: later changes count next time

	switch {
	case ts.Reloading:
		return ds.reload(sm, "resuming an interrupted reload")
	case ds.rc.job.Config.Data.Mode == "replace":
		return ds.reload(sm, "replace mode")
	}

	srcRows, tgtRows, err := both(func(s *side) (int64, error) { return partitionRows(ctx, s, sm) }, ds.src, ds.tgt)
	if err != nil {
		return err
	}
	if srcRows > 0 && tgtRows == 0 {
		return ds.reload(sm, "target is empty")
	}

	// 1) cheapest: has the table been touched since the last successful sync?
	// usage stats can be reset (restart, failover, database offline/restore): trust "unchanged" for
	// at most 24h, then verify with a real IBLT scan anyway
	if pr.ok && pr.unchanged && srcRows == ts.Rows && recently(ts.LastVerified, 24*time.Hour) {
		log.Printf("%s: not modified since %s — skipped without reading the table", key, ts.CheckedAt)
		ds.cur.Action, ds.cur.Note = "skipped", "not modified since the last run"
		return nil
	}
	mark := ""
	if !pr.ok && sm.RowVersion != "" {
		if mark, err = ds.rowVersionMark(sm); err != nil {
			return err
		}
		if mark == ts.SrcMark {
			log.Printf("%s: rowversion and row count unchanged — skipped without hashing", key)
			ds.cur.Action, ds.cur.Note = "skipped", "rowversion unchanged"
			return nil
		}
	}

	// 2) full reconciliation or only the newest PK window (ascending integer keys)
	// window mode is opt-in (windowRows > 0): it trades freshness of old-row edits for load
	window := ds.rc.job.Config.Data.WindowRows
	full := window <= 0 || !sm.IntPK || srcRows <= int64(2*window) || ds.fullCheckDue(ts)
	where, args, scope := "", []any(nil), srcRows
	if !full {
		from, err := ds.windowFrom(sm, window)
		if err != nil {
			return err
		}
		where = fmt.Sprintf("\nWHERE %s >= @from", sm.colRef("t", sm.PK[0]))
		args, scope = []any{sql.Named("from", from)}, int64(window)
		log.Printf("%s: window check — last %d rows (%s >= %d); full check on its schedule", key, window, sm.Cols[sm.PK[0]].Name, from)
	}

	// 2b) a rowversion column already in the table (no database setting needed): only rows with
	// rowversion >= the mark of the last reconciliation are read, hashed and compared by key with the
	// target. Deletes leave no rowversion; they show up in the row counts and fall back to the IBLT.
	if full && sm.RowVersion != "" && (sm.IntPK || sm.RawKey) && ts.RvMark != "" && ts.TgtOps > 0 &&
		recently(ts.IBLTAt, 24*time.Hour) && ds.rowOps(ds.tgt, sm) == ts.TgtOps {
		ds.scanRows = scope
		ok, n, err := ds.rowVersionSync(sm, &ts, key)
		if err != nil {
			return err
		}
		if ok {
			log.Printf("%s: by rowversion — %d changed rows compared by key, the table was not hashed", key, n)
			if ds.cur.Action == "" {
				ds.cur.Action = "unchanged"
			}
			ds.cur.RowsChecked = int64(n)
			ds.done(key, ts, n, pr, mark, srcRows, full)
			return nil
		}
		log.Printf("%s: by rowversion the row counts differ (rows were deleted) — full IBLT check", key)
	}
	rvMark := ""
	if full && sm.RowVersion != "" && (sm.IntPK || sm.RawKey) {
		if rvMark, err = ds.minActiveRowVersion(); err != nil {
			return err
		}
	}

	// 3) IBLT size. A too small table costs a second full scan of both sides while cells are cheap
	// (72 bytes), so the estimate is an upper bound: growth of the row operation counters on both
	// sides since the last sync (an updated row is 2 elements, so 2 × operations). Unknown counters
	// (restart, metadata evicted, no permission): twice the last run's differences.
	d0 := max(2*ts.LastDiff, 8)
	tgtOps := ds.rowOps(ds.tgt, sm)
	if ds.srcOps >= ts.SrcOps && tgtOps >= ts.TgtOps && ts.SrcOps > 0 && ts.TgtOps > 0 {
		d0 = max(8, int(2*(ds.srcOps-ts.SrcOps+tgtOps-ts.TgtOps)))
	}
	if full {
		d0 = max(d0, int(abs64(srcRows-tgtRows)))
	}
	// 3*size cells ≈ 2 × differences + 60: decoding fails ~1 in 300 at 1.75×, ~never at 2×
	// (measured), and small tables need relatively more room
	size := max(16, d0*2/3+20)
	size = min(size, maxCells/3, max(16, int(scope/12))) // beyond that a reload is cheaper anyway

	// target cell cache (see cellCache): only the source is scanned when nobody but us wrote to
	// the target since the last sync (operation counter unchanged) and its last real scan is <24h old
	cacheable := full && (sm.IntPK || sm.RawKey) && scope >= bigScanRows && ds.rc.loadCells != nil && size <= cacheL
	var tgtBig []ibltCell
	fromCache, tgtScanned := false, false
	if cacheable {
		size = nextPow2(size)
		if ts.TgtCached && tgtOps > 0 && tgtOps == ts.TgtOps && recently(ts.TgtScanned, 24*time.Hour) {
			if b, err := ds.rc.loadCells(key); err == nil && len(b) == 3*cacheL*cellBytes {
				tgtBig, fromCache = decodeCells(b), true
			}
		}
	}
	ts.TgtCached = false // set again below only when the cache is saved
	var elems []ibltElem
	for attempt := 1; ; attempt++ {
		if cacheable {
			size = nextPow2(size)
			if size > cacheL {
				cacheable, tgtBig, fromCache = false, nil, false
			}
		}
		// the first round always runs: for an unchanged table it is the (cheap) equality check
		if attempt > 1 && (3*size > maxCells || int64(3*size) > scope/4) {
			if !full {
				return fmt.Errorf("too many changes in the window; enlarge the window or wait for the full check")
			}
			return ds.reload(sm, "too many changes for the IBLT, a full reload is cheaper")
		}
		ds.phase(fmt.Sprintf("checking (IBLT) %d rows, both sides", srcRows))
		ds.scanRows = scope
		var sc, tc map[int]ibltCell
		switch {
		case cacheable && tgtBig != nil: // target from the cache: one scan instead of two
			ds.phase(fmt.Sprintf("checking (IBLT) %d rows, source; target from the cell cache", srcRows))
			if sc, err = ds.ibltCells(ds.src, sm, size, scope, where, args); err != nil {
				return err
			}
			tc = foldCells(tgtBig, cacheL, size)
		case cacheable: // build the cache: the target is scanned at the cache size, in parallel
			var tm map[int]ibltCell
			sc, tm, err = bothFn(func() (map[int]ibltCell, error) { return ds.ibltCells(ds.src, sm, size, scope, where, args) },
				func() (map[int]ibltCell, error) { return ds.ibltCells(ds.tgt, sm, cacheL, scope, where, args) })
			if err != nil {
				return err
			}
			tgtBig, fromCache, tgtScanned = denseCells(tm, 3*cacheL), false, true
			tc = foldCells(tgtBig, cacheL, size)
		default:
			if sc, tc, err = both(func(s *side) (map[int]ibltCell, error) { return ds.ibltCells(s, sm, size, scope, where, args) }, ds.src, ds.tgt); err != nil {
				return err
			}
			tgtScanned = true
		}
		rows := cellRows(sc)
		if cellsEqual(sc, tc) {
			log.Printf("%s: no changes (%d rows checked)", key, rows)
			ds.cur.Action, ds.cur.RowsChecked = "unchanged", rows
			ds.saveCellCache(key, &ts, tgtBig, tgtScanned)
			ts.RvMark, ts.IBLTAt = rvMark, time.Now().UTC().Format(time.RFC3339)
			ds.done(key, ts, 0, pr, mark, srcRows, full)
			return nil
		}
		if attempt == 1 && full && rows < smallTableRows {
			return ds.reload(sm, fmt.Sprintf("small table (%d rows)", rows))
		}
		var ok bool
		var est int
		if elems, est, ok = ibltDecode(sc, tc, size); ok {
			if fromCache {
				// every target-side element must be on the target exactly as decoded (index seeks);
				// otherwise the cache is stale and both sides are scanned for real
				good, err := ds.verifyTargetElems(sm, elems)
				if err != nil {
					return err
				}
				if !good {
					log.Printf("%s: the target cell cache does not match the target — full scan of both sides", key)
					tgtBig, fromCache = nil, false
					continue
				}
			}
			src := "both sides"
			if fromCache {
				src = "target from cache"
			}
			log.Printf("%s: IBLT %d cells → %d differences (%d rows; %s)", key, 3*size, len(elems), rows, src)
			ds.cur.RowsChecked = rows
			break
		}
		size = max(size*2, (est*3+3)/4) // ≈1.5 × estimated differences, at least double
		log.Printf("%s: IBLT too small (≈%d differences estimated), retrying with %d cells", key, est, 3*size)
	}

	ds.phase(fmt.Sprintf("applying %d differences", len(elems)))
	if err := ds.apply(sm, elems, &ts, where, args); err != nil {
		return err
	}
	if tgtBig != nil {
		// target cells after our writes: minus the replaced/deleted target versions, plus the rows
		// as actually written (read back by key: the source may have changed since its scan)
		ins := map[key128]bool{}
		for _, e := range elems {
			if e.Count < 0 {
				addElem(tgtBig, cacheL, e.F, e.K, e.K2, int64(e.Count))
			} else {
				ins[key128{e.K, e.K2}] = true
			}
		}
		written, err := ds.targetRowHashes(sm, ins)
		if err != nil {
			return err
		}
		for _, e := range written {
			addElem(tgtBig, cacheL, e.F, e.K, e.K2, 1)
		}
		ds.saveCellCache(key, &ts, tgtBig, tgtScanned)
	}
	// no verification pass: the next run's first scan verifies (and catches rows changed meanwhile)
	ts.RvMark, ts.IBLTAt = rvMark, time.Now().UTC().Format(time.RFC3339)
	ds.done(key, ts, len(elems), pr, mark, srcRows, full)
	return nil
}

func (ds *dataSync) done(key string, ts TableState, diff int, pr probe, mark string, rows int64, full bool) {
	ts.LastDiff, ts.SrcMark, ts.Rows = diff, mark, rows
	ts.LastVerified = time.Now().UTC().Format(time.RFC3339)
	ts.CheckedAt = ""
	if pr.ok {
		ts.CheckedAt = pr.checkpoint
	}
	if full {
		ts.LastFull = time.Now().UTC().Format(time.RFC3339)
	}
	// counters after our own writes: the next run sees only foreign changes on the target
	ts.SrcOps, ts.TgtOps = ds.srcOps, 0
	if ds.srcOps > 0 {
		ts.TgtOps = ds.rowOps(ds.tgt, ds.curMeta)
	}
	ds.setTS(key, ts)
}

// rowOps: inserts + updates + deletes on the table's rows (heap or clustered index) since its
// metadata was cached; 0 when unknown.
func (ds *dataSync) rowOps(s *side, m *tableMeta) int64 {
	if m == nil {
		return 0
	}
	var n sql.NullInt64
	err := s.meta.QueryRowContext(ds.rc.ctx, `SELECT SUM(leaf_insert_count + leaf_update_count + leaf_delete_count)
FROM sys.dm_db_index_operational_stats(DB_ID(), OBJECT_ID(@t), NULL, NULL) WHERE index_id IN (0, 1)`,
		sql.Named("t", qi(m.Schema)+"."+qi(m.Name))).Scan(&n)
	if err != nil || !n.Valid {
		return 0
	}
	return n.Int64
}
