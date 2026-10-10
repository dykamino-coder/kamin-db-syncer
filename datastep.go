package main

// The data step of a run: table list, change prepass, worker pool, foreign keys.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type dataSync struct {
	rc         *runCtx
	src, tgt   *side
	limits     *limiterPair
	sh         *dsShared
	cur        *TableResult // table being synced by this worker (run dashboard)
	idx, total int
	lastProbe  probe // probe of the table being synced
	srcOps     int64 // source row operation counter read before the scan (0 = unknown)
	scanRows   int64 // rows the current reconciliation scans (sizes MAXDOP of follow-up scans)
	curMeta    *tableMeta
}

// dsShared: state shared by the table workers of one run.
type dsShared struct {
	mu      sync.Mutex
	fks     [][2]string // [table 3-part, constraint]
	fksOff  bool
	noProbe atomic.Bool // change probe unavailable (no VIEW SERVER STATE)
}

type tableRef struct{ schema, name string }

func parseTableName(s string) tableRef {
	s = strings.TrimSpace(s)
	if strings.HasPrefix(s, "[") {
		if i := strings.Index(s, "].["); i > 0 && strings.HasSuffix(s, "]") {
			return tableRef{s[1:i], strings.ReplaceAll(s[i+3:len(s)-1], "]]", "]")}
		}
	}
	if sch, n, ok := strings.Cut(s, "."); ok {
		return tableRef{sch, n}
	}
	return tableRef{"dbo", s}
}

func (rc *runCtx) dataStep() error {
	c := &rc.job.Config
	src, err := openSide(rc.ctx, c.Source)
	if err != nil {
		return err
	}
	defer src.close()
	tgt, err := openSide(rc.ctx, c.Target)
	if err != nil {
		return err
	}
	defer tgt.close()
	base := &dataSync{rc: rc, src: src, tgt: tgt, limits: newLimiterPair(c.Data.MaxMbit), sh: &dsShared{}}
	if m := base.effectiveMbit(); m > 0 {
		rc.log.Printf("Bandwidth limit: %g Mbit/s in and out (sync %g, pod %g)", m, c.Data.MaxMbit, podMaxMbit)
	}
	par := c.Data.ParallelTables
	if par <= 0 {
		par = defaultParallelTables
	}
	rc.log.Printf("Source reads: %s; MAXDOP %d; tables in parallel %d; target ~%s per statement", src.isolationName(), scanMaxDop, par, batchTarget)
	src.decideDop(rc.ctx, par)
	tgt.decideDop(rc.ctx, par)
	rc.log.Printf("MAXDOP for scans of tables from %d rows: source %d (%s), target %d (%s)", bigScanRows, src.bigDop, src.loadNote, tgt.bigDop, tgt.loadNote)
	if src.compat < 130 || tgt.compat < 130 {
		rc.log.Printf("Database compatibility level %d/%d < 130: OPENJSON runs in the tempdb context", src.compat, tgt.compat)
	}

	tables, err := base.resolveTables(c.Data.Tables)
	if err != nil {
		return err
	}
	if err := base.loadFKs(tables); err != nil {
		return err
	}
	defer base.restoreFKs()

	// 1) one query per side decides which tables were not touched since their last sync: they are
	// recorded as skipped without any per-table metadata queries
	todo := base.prepass(tables)

	// 2) the rest is processed by a small worker pool (each worker: its own table, shared FK state)
	var (
		mu      sync.Mutex
		failed  []string
		started atomic.Int64
		wg      sync.WaitGroup
	)
	queue := make(chan tableRef)
	worker := func() {
		defer wg.Done()
		for t := range queue {
			ds := &dataSync{rc: rc, src: src, tgt: tgt, limits: base.limits, sh: base.sh}
			ds.cur = &TableResult{Table: t.schema + "." + t.name}
			ds.idx, ds.total = len(tables)-len(todo)+int(started.Add(1))-1, len(tables)
			ds.phase("metadata")
			t0, before := time.Now(), rc.stats.read.Load()+rc.stats.written.Load()
			err := ds.syncTable(t)
			ds.cur.Millis = time.Since(t0).Milliseconds()
			ds.cur.Bytes = rc.stats.read.Load() + rc.stats.written.Load() - before
			if err != nil {
				ds.cur.Action, ds.cur.Error = "error", err.Error()
			} else if ds.cur.Action == "" {
				ds.cur.Action = "unchanged"
			}
			rc.addTable(*ds.cur)
			rc.saveState()
			if err != nil && rc.ctx.Err() == nil {
				rc.log.Printf("ERROR %s.%s: %v", t.schema, t.name, err)
				mu.Lock()
				failed = append(failed, fmt.Sprintf("%s.%s: %v", t.schema, t.name, err))
				mu.Unlock()
			}
		}
	}
	for range min(par, max(1, len(todo))) {
		wg.Add(1)
		go worker()
	}
	for _, t := range todo {
		if rc.ctx.Err() != nil {
			break
		}
		queue <- t
	}
	close(queue)
	wg.Wait()
	if rc.ctx.Err() != nil {
		return rc.ctx.Err()
	}
	st := rc.stats.snapshot()
	rc.notes = append(rc.notes, fmt.Sprintf("data: %d of %d tables changed, %d rows, traffic ≈%s",
		st.TablesChanged, len(tables), st.RowsChanged, humanBytes(st.BytesRead+st.BytesWritten)))
	if len(failed) > 0 {
		return errors.New(strings.Join(failed, "; "))
	}
	return nil
}

// prepass applies the "not modified since last sync" rule to all tables with one DMV query on the
// source and one metadata query on the target. Returns the tables that still need processing.
func (ds *dataSync) prepass(tables []tableRef) []tableRef {
	if ds.rc.job.Config.Data.Mode == "replace" || len(tables) == 0 {
		return tables
	}
	ctx := ds.rc.ctx
	const layout = "2006-01-02T15:04:05.9999999"
	type srcInfo struct {
		rows    int64
		lastUpd sql.NullTime
		pages   int64
	}
	srcMap := map[string]srcInfo{}
	var started time.Time
	rows, err := ds.src.meta.QueryContext(ctx, `
SELECT s.name + N'.' + t.name,
       ISNULL((SELECT SUM(p.rows) FROM sys.partitions p WHERE p.object_id = t.object_id AND p.index_id IN (0, 1)), 0),
       (SELECT MAX(u.last_user_update) FROM sys.dm_db_index_usage_stats u WHERE u.database_id = DB_ID() AND u.object_id = t.object_id),
       (SELECT sqlserver_start_time FROM sys.dm_os_sys_info),
       ISNULL((SELECT SUM(a.used_pages) FROM sys.partitions p JOIN sys.allocation_units a ON a.container_id = p.partition_id
               WHERE p.object_id = t.object_id AND p.index_id IN (0, 1)), 0)
FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE t.is_ms_shipped = 0`)
	if err != nil {
		ds.rc.log.Printf("Batch change check unavailable (needs VIEW SERVER STATE), tables are checked one by one: %v", err)
		return tables
	}
	for rows.Next() {
		var name string
		var si srcInfo
		if err := rows.Scan(&name, &si.rows, &si.lastUpd, &started, &si.pages); err != nil {
			rows.Close()
			return tables
		}
		srcMap[strings.ToLower(name)] = si
	}
	rows.Close()
	tgtRows := map[string]int64{}
	if r2, err := ds.tgt.meta.QueryContext(ctx, `SELECT s.name + N'.' + t.name,
       ISNULL((SELECT SUM(p.rows) FROM sys.partitions p WHERE p.object_id = t.object_id AND p.index_id IN (0, 1)), 0)
FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE t.is_ms_shipped = 0`); err == nil {
		for r2.Next() {
			var name string
			var n int64
			if r2.Scan(&name, &n) == nil {
				tgtRows[strings.ToLower(name)] = n
			}
		}
		r2.Close()
	} else {
		return tables
	}

	var todo []tableRef
	skipped := 0
	for _, t := range tables {
		key := t.schema + "." + t.name
		ts := ds.getTS(key)
		si, ok := srcMap[strings.ToLower(key)]
		checked, perr := time.Parse(layout, ts.CheckedAt)
		tr, tok := tgtRows[strings.ToLower(key)]
		unchanged := ok && tok && perr == nil && !ts.Reloading && checked.After(started) &&
			(!si.lastUpd.Valid || si.lastUpd.Time.Before(checked)) &&
			si.rows == ts.Rows && (si.rows == 0 || tr > 0) && recently(ts.LastVerified, 24*time.Hour)
		if !unchanged {
			todo = append(todo, t)
			continue
		}
		skipped++
		ds.rc.addTable(TableResult{Table: key, Action: "skipped", Note: "not modified since the last run"})
	}
	// largest first: the biggest table started last ran alone at the end of the run (Wide, 12s,
	// while the other worker was idle); with the size order it overlaps the smaller ones
	sort.SliceStable(todo, func(i, j int) bool {
		return srcMap[strings.ToLower(todo[i].schema+"."+todo[i].name)].pages > srcMap[strings.ToLower(todo[j].schema+"."+todo[j].name)].pages
	})
	ds.rc.log.Printf("Change check: %d of %d tables not modified since the last run — skipped without reading; %d to process (largest first)",
		skipped, len(tables), len(todo))
	ds.rc.setProgress("data", "", "change check", skipped, len(tables))
	return todo
}

func (ds *dataSync) getTS(key string) TableState {
	ds.rc.stateMu.Lock()
	defer ds.rc.stateMu.Unlock()
	return ds.rc.job.State.Tables[key]
}

func (ds *dataSync) setTS(key string, ts TableState) {
	ds.rc.stateMu.Lock()
	ds.rc.job.State.Tables[key] = ts
	ds.rc.stateMu.Unlock()
}

func (ds *dataSync) resolveTables(names []string) ([]tableRef, error) {
	var out []tableRef
	for _, n := range names {
		if strings.TrimSpace(n) != "*" {
			out = append(out, parseTableName(n))
			continue
		}
		rows, err := ds.src.meta.QueryContext(ds.rc.ctx, `SELECT s.name, t.name FROM sys.tables t
JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE t.is_ms_shipped = 0 AND t.name <> N'sysdiagrams' ORDER BY 1, 2`)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var t tableRef
			if err := rows.Scan(&t.schema, &t.name); err != nil {
				rows.Close()
				return nil, err
			}
			out = append(out, t)
		}
		rows.Close()
	}
	return out, nil
}

func sqlStr(s string) string { return "N'" + strings.ReplaceAll(s, "'", "''") + "'" }

// FKs touching synced tables are disabled only once something is actually written.
func (ds *dataSync) loadFKs(tables []tableRef) error {
	if len(tables) == 0 {
		return nil
	}
	ids := make([]string, len(tables))
	for i, t := range tables {
		ids[i] = "OBJECT_ID(" + sqlStr(qi(t.schema)+"."+qi(t.name)) + ")"
	}
	in := strings.Join(ids, ", ")
	rows, err := ds.tgt.meta.QueryContext(ds.rc.ctx, `SELECT QUOTENAME(OBJECT_SCHEMA_NAME(parent_object_id)) + N'.' +
QUOTENAME(OBJECT_NAME(parent_object_id)), QUOTENAME(name) FROM sys.foreign_keys
WHERE is_disabled = 0 AND (parent_object_id IN (`+in+`) OR referenced_object_id IN (`+in+`))`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var t, c string
		if err := rows.Scan(&t, &c); err != nil {
			return err
		}
		ds.sh.fks = append(ds.sh.fks, [2]string{qi(ds.tgt.ep.Database) + "." + t, c})
	}
	return rows.Err()
}

func (ds *dataSync) disableFKs() error {
	ds.sh.mu.Lock()
	defer ds.sh.mu.Unlock()
	if ds.sh.fksOff {
		return nil
	}
	for _, fk := range ds.sh.fks {
		if _, err := ds.tgt.exec.ExecContext(ds.rc.ctx, "ALTER TABLE "+fk[0]+" NOCHECK CONSTRAINT "+fk[1]); err != nil {
			return err
		}
	}
	ds.sh.fksOff = true
	return nil
}

func (ds *dataSync) restoreFKs() {
	if !ds.sh.fksOff {
		return
	}
	ctx := context.WithoutCancel(ds.rc.ctx)
	if !ds.rc.job.Config.Data.VerifyForeignKeys {
		// WITH NOCHECK: metadata only, no table scan under a schema lock; the copy mirrors a source
		// where these constraints already hold
		for _, fk := range ds.sh.fks {
			if _, err := ds.tgt.exec.ExecContext(ctx, "ALTER TABLE "+fk[0]+" WITH NOCHECK CHECK CONSTRAINT "+fk[1]); err != nil {
				ds.rc.log.Printf("WARNING: could not re-enable %s on %s: %v", fk[1], fk[0], err)
			}
		}
		ds.sh.fksOff = false
		return
	}
	for _, fk := range ds.sh.fks {
		if _, err := ds.tgt.exec.ExecContext(ctx, "ALTER TABLE "+fk[0]+" WITH CHECK CHECK CONSTRAINT "+fk[1]); err != nil {
			ds.rc.log.Printf("WARNING: %s on %s re-enabled without checking (data violates the constraint): %v", fk[1], fk[0], err)
			_, _ = ds.tgt.exec.ExecContext(ctx, "ALTER TABLE "+fk[0]+" WITH NOCHECK CHECK CONSTRAINT "+fk[1])
		}
	}
	ds.sh.fksOff = false
}

func (ds *dataSync) written(sqlText string, extra int) {
	n := int64(len(sqlText)*2 + extra)
	ds.rc.stats.written.Add(n)
	ds.throttle(n, false)
}

func both[T any](f func(*side) (T, error), a, b *side) (T, T, error) {
	var ra, rb T
	var ea, eb error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); ra, ea = f(a) }()
	go func() { defer wg.Done(); rb, eb = f(b) }()
	wg.Wait()
	return ra, rb, errors.Join(ea, eb)
}

func recently(rfc3339 string, d time.Duration) bool {
	t, err := time.Parse(time.RFC3339, rfc3339)
	return err == nil && time.Since(t) < d
}

func (ds *dataSync) phase(p string) {
	if ds.cur != nil {
		ds.rc.setProgress("data", ds.cur.Table, p, ds.idx, ds.total)
	}
}

func abs64(v int64) int64 {
	if v < 0 {
		return -v
	}
	return v
}

func humanBytes(b int64) string {
	switch {
	case b >= 1<<30:
		return fmt.Sprintf("%.1f GB", float64(b)/(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.1f MB", float64(b)/(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.1f KB", float64(b)/(1<<10))
	}
	return fmt.Sprintf("%d B", b)
}

func bothFn[T any](fa, fb func() (T, error)) (T, T, error) {
	var ra, rb T
	var ea, eb error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); rb, eb = fb() }()
	ra, ea = fa()
	wg.Wait()
	if ea != nil {
		return ra, rb, ea
	}
	return ra, rb, eb
}
