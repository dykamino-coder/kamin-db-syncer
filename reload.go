package main

// Full reload: truncate and keyset-paged copy, pipelined, resumable, optionally several readers.

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"
)

// reload: truncate + keyset-paged compressed copy. Chunk size adapts to batchTarget / maxBlobBytes
// and is remembered per table; each chunk is a single autocommitted statement.
func (ds *dataSync) reload(m *tableMeta, reason string) error {
	ctx, log := ds.rc.ctx, ds.rc.log
	key := m.Schema + "." + m.Name
	log.Printf("%s: full reload (%s)", key, reason)
	ds.cur.Action, ds.cur.Note = "reload", reason
	ds.phase("reload: " + reason)
	if err := ds.disableFKs(); err != nil {
		return err
	}
	tref, sref := ds.tgt.ref(m), ds.src.ref(m)
	tx := ds.tgt.exec
	if m.HasTriggers {
		if _, err := tx.ExecContext(ctx, "ALTER TABLE "+tref+" DISABLE TRIGGER ALL"); err != nil {
			return err
		}
		defer tx.ExecContext(context.WithoutCancel(ctx), "ALTER TABLE "+tref+" ENABLE TRIGGER ALL")
	}
	ts := ds.getTS(key)
	var last *string
	if ts.Reloading && len(m.PK) > 0 && ts.ResumeKey != "" {
		// resume after the last key of the contiguous committed prefix; parallel inserts may have
		// committed later chunks too — remove them (index range) so nothing is duplicated or skipped
		decl, where := m.keyVars(ts.ResumeKey)
		q := lowImpact + decl + fmt.Sprintf("DELETE t FROM %s AS t%s", tref, where)
		r, err := tx.ExecContext(ctx, q, sql.Named("last", ts.ResumeKey))
		if err != nil {
			return err
		}
		extra, _ := r.RowsAffected()
		last = &ts.ResumeKey
		log.Printf("%s: resuming after key %s (%d rows outside the contiguous prefix removed)", key, ts.ResumeKey, extra)
	} else {
		ts.Reloading, ts.ResumeKey = true, ""
		ds.setTS(key, ts)
		ds.rc.saveState()
		if _, err := tx.ExecContext(ctx, lowImpact+"TRUNCATE TABLE "+tref); err != nil {
			if _, err := tx.ExecContext(ctx, lowImpact+"DELETE FROM "+tref); err != nil { // referenced by a FK
				return err
			}
		}
	}
	var total int64
	if len(m.PK) == 0 {
		// no key to page by: split into buckets by BINARY_CHECKSUM (cheap, deterministic) —
		// one plain scan per bucket, worker memory bounded by the bucket size
		rows, err := partitionRows(ctx, ds.src, m)
		if err != nil {
			return err
		}
		size := ts.BatchRows
		if size <= 0 {
			size = 20000
		}
		if c := ds.rc.job.Config.Data.ChunkRows; c > 0 {
			size = c
		}
		buckets := max(1, int((rows+int64(size)-1)/int64(size)))
		for b := 0; b < buckets; b++ {
			q := ds.src.readPrefix() + "SELECT " + ds.src.dataJSON(fmt.Sprintf("SELECT %s FROM %s AS t WHERE (BINARY_CHECKSUM(*) %% @n + @n) %% @n = @b FOR JSON PATH",
				m.encSelect("t"), sref)) + maxdop()
			ds.written(q, 8)
			var blob []byte
			if err := ds.src.exec.QueryRowContext(ctx, q, sql.Named("n", buckets), sql.Named("b", b)).Scan(&blob); err != nil {
				return err
			}
			n, err := ds.insertBlob(tx, m, tref, blob, true)
			if err != nil {
				return err
			}
			total += n
		}
		if buckets > 1 {
			log.Printf("%s: no PK — %d chunks of ~%d rows", key, buckets, size)
		}
	} else {
		n, err := ds.pipelinedReload(m, key, sref, tref, last, &ts)
		if err != nil {
			return err
		}
		total = n
		log.Printf("%s: tuned batch size %d rows", key, ts.BatchRows)
	}
	ds.rc.stats.reloads.Add(1)
	ts.LastDiff, ts.Reloading, ts.SrcMark, ts.ResumeKey, ts.TgtCached, ts.RvMark = 0, false, "", "", false, ""
	ts.CheckedAt = ""
	if ds.lastProbe.ok {
		ts.CheckedAt = ds.lastProbe.checkpoint
	}
	ts.SrcOps, ts.TgtOps = ds.srcOps, 0
	if ds.srcOps > 0 {
		ts.TgtOps = ds.rowOps(ds.tgt, m)
	}
	if n, err := partitionRows(ctx, ds.src, m); err == nil {
		ts.Rows = n
	}
	ts.LastVerified = time.Now().UTC().Format(time.RFC3339)
	ds.setTS(key, ts)
	ds.rc.stats.rows.Add(total)
	ds.rc.stats.tables.Add(1)
	log.Printf("%s: %d rows loaded", key, total)
	ds.cur.Inserted, ds.cur.RowsChecked = total, total
	return nil
}

func (ds *dataSync) fetchChunk(m *tableMeta, sref string, last *string, size int, r keyRange) ([]byte, *string, int64, error) {
	// The last key is unpacked into typed variables first: a predicate on variables is an index
	// seek, while comparing against OPENJSON columns made SQL Server rescan from the start.
	decl, where := "", ""
	args := []any{sql.Named("n", size)}
	if last != nil {
		decl, where = m.keyVars(*last)
		args = append(args, sql.Named("last", *last))
	}
	var bounds []string
	if r.lo != nil {
		bounds = append(bounds, m.colRef("t", m.PK[0])+" >= @lo")
		args = append(args, sql.Named("lo", *r.lo))
	}
	if r.hi != nil {
		bounds = append(bounds, m.colRef("t", m.PK[0])+" < @hi")
		args = append(args, sql.Named("hi", *r.hi))
	}
	if len(bounds) > 0 {
		if where == "" {
			where = " WHERE " + strings.Join(bounds, " AND ")
		} else {
			where += " AND " + strings.Join(bounds, " AND ")
		}
	}
	q := ds.src.readPrefix() + decl + fmt.Sprintf(`WITH c AS (SELECT TOP (@n) t.* FROM %s AS t%s ORDER BY %s)
SELECT %s,
       (SELECT TOP (1) %s FROM c ORDER BY %s FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
       (SELECT COUNT_BIG(*) FROM c)`,
		sref, where, m.pkOrder("t", ""), ds.src.dataJSON(fmt.Sprintf("SELECT %s FROM c ORDER BY %s FOR JSON PATH", m.encSelect("c"), m.pkOrder("c", ""))),
		m.pkSelect("c"), m.pkOrder("c", " DESC")) + maxdop()
	ds.written(q, 0)
	var blob []byte
	var next sql.NullString
	var n int64
	if err := ds.src.exec.QueryRowContext(ds.rc.ctx, q, args...).Scan(&blob, &next, &n); err != nil {
		return nil, nil, 0, err
	}
	if !next.Valid {
		return blob, nil, n, nil
	}
	ds.in(int64(len(next.String)))
	return blob, &next.String, n, nil
}

// reloadInserters: RELOAD_PARALLEL, raised to 1.5 x readers within the target's quiet-core budget
// when several readers feed it (4 readers + 3 inserters: readers waited 28s for the target;
// + 6 inserters: 39-41s instead of 47-50s).
func (ds *dataSync) reloadInserters(readers int) int {
	if readers <= 1 || ds.tgt.quietCores <= 0 {
		return reloadParallel
	}
	return max(reloadParallel, min((3*readers+1)/2, ds.tgt.quietCores))
}

// pipelinedReload: the source prepares chunk N+1 while the target inserts chunk N (and N-1...).
// ts.ResumeKey only advances over a contiguous prefix of committed chunks.
func (ds *dataSync) pipelinedReload(m *tableMeta, key, sref, tref string, last *string, ts *TableState) (int64, error) {
	ctx := ds.rc.ctx
	b := ds.batcher(max(ts.BatchRows, 2000), 100, 500000, ds.rc.job.Config.Data.ChunkRows)
	type chunk struct {
		seq     int
		blob    []byte
		n       int64
		lastKey string
		fetched time.Duration
	}
	ranges := ds.reloadRanges(m, last)
	ins := ds.reloadInserters(len(ranges))
	ch := make(chan chunk, ins)
	var (
		mu       sync.Mutex
		total    int64
		firstErr error
		done     = map[int]string{}
		nextSeq  int
		wg       sync.WaitGroup
		// where the time goes: source reads, target inserts (summed over workers), reader blocked
		// on busy inserters — shows which side bounds the reload
		readT, insT, waitT time.Duration
		savedKey           = ts.ResumeKey
		savedAt            time.Time
	)
	for range ins {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for c := range ch {
				started := time.Now()
				_, err := ds.insertBlob(ds.tgt.exec, m, tref, c.blob, ins == 1)
				mu.Lock()
				insT += time.Since(started)
				if err != nil {
					if firstErr == nil {
						firstErr = err
					}
				} else {
					total += c.n
					// concurrent readers share the source: a read's own share of the
					// time is fetched/readers (whole read times made the batch collapse to
					// ~140 rows with 4 readers)
					b.observe(int(c.n), max(c.fetched/time.Duration(len(ranges)), time.Since(started)), len(c.blob))
				}
				if err == nil && c.seq >= 0 {
					done[c.seq] = c.lastKey
					advanced := false
					for k, ok := done[nextSeq]; ok; k, ok = done[nextSeq] {
						delete(done, nextSeq)
						nextSeq++
						if k != "" {
							ts.ResumeKey, advanced = k, true
						}
					}
					if advanced {
						ts.BatchRows = b.size
						ds.setTS(key, *ts)
					}
				}
				// the resume point goes to MongoDB at most every 2s and outside mu: saving the
				// whole job state after every chunk under the lock serialized readers and inserters
				save := ts.ResumeKey != savedKey && time.Since(savedAt) > 2*time.Second
				if save {
					savedKey, savedAt = ts.ResumeKey, time.Now()
				}
				mu.Unlock()
				if save {
					ds.rc.saveState()
				}
			}
		}()
	}
	var fetchErr error
	var readers sync.WaitGroup
	for ri, r := range ranges {
		readers.Add(1)
		go func() {
			defer readers.Done()
			last := last
			if ri > 0 {
				last = nil
			}
			for seq := 0; ; seq++ {
				mu.Lock()
				size, failed := b.size, firstErr != nil || fetchErr != nil
				mu.Unlock()
				if failed || ctx.Err() != nil {
					return
				}
				started := time.Now()
				blob, next, n, err := ds.fetchChunk(m, sref, last, size, r)
				fetched := time.Since(started)
				mu.Lock()
				readT += fetched
				if err != nil && fetchErr == nil {
					fetchErr = err
				}
				mu.Unlock()
				if err != nil {
					return
				}
				lastKey := ""
				if next != nil {
					lastKey = *next
				}
				if n > 0 {
					s := seq
					if ri > 0 {
						s = -1 // only the first range advances the resume key; later ranges are
						// beyond it and are removed and re-read if the reload is interrupted
					}
					w := time.Now()
					ch <- chunk{seq: s, blob: blob, n: n, lastKey: lastKey, fetched: fetched}
					mu.Lock()
					waitT += time.Since(w)
					mu.Unlock()
				}
				if n == 0 || next == nil {
					return
				}
				last = next
			}
		}()
	}
	readers.Wait()
	close(ch)
	wg.Wait()
	ts.BatchRows = b.size
	ds.rc.log.Printf("%s: %d readers, source reads %.0fs, inserts %.0fs (summed over threads; %d inserters), waiting for the target %.0fs",
		key, len(ranges), readT.Seconds(), insT.Seconds(), ins, waitT.Seconds())
	if firstErr != nil {
		return total, firstErr
	}
	if fetchErr != nil {
		return total, fetchErr
	}
	return total, ctx.Err()
}

type keyRange struct{ lo, hi *int64 }

// first PK column: lo inclusive, hi exclusive; nil = open

// reloadRanges: on a quiet source (same rule and budget as the scan MAXDOP) a big table with an
// integer PK is read by several keyset readers over equal key ranges — the reload is bound by
// FOR JSON + COMPRESS on the source, which runs on one core per statement. Otherwise one reader.
// Only when source and target are different instances: 1M rows x 34 columns, separate servers
// 54s -> 46-48s with 2 readers; source and target on one instance 63s -> 64-78s (that server,
// not a single core, is then the limit). RELOAD_READERS caps it (1 disables).
func (ds *dataSync) reloadRanges(m *tableMeta, last *string) []keyRange {
	n := min(reloadReaders, ds.src.quietCores)
	if ds.src.instance == "" || ds.src.instance == ds.tgt.instance {
		n = 1
	}
	if !m.IntPK || last != nil || n <= 1 {
		return []keyRange{{}}
	}
	rows, err := partitionRows(ds.rc.ctx, ds.src, m)
	if err != nil || rows < bigScanRows {
		return []keyRange{{}}
	}
	// equal row counts per reader from the PK statistics histogram (no table access); keys are
	// often skewed (e.g. a jump in identity values), so splitting MIN..MAX evenly does not work
	rs, err := ds.src.meta.QueryContext(ds.rc.ctx, `SELECT CAST(h.range_high_key AS bigint), h.range_rows + h.equal_rows
FROM sys.indexes i
CROSS APPLY sys.dm_db_stats_histogram(i.object_id, i.index_id) h
WHERE i.object_id = OBJECT_ID(@t) AND i.is_primary_key = 1
ORDER BY h.step_number`, sql.Named("t", qi(m.Schema)+"."+qi(m.Name)))
	if err != nil {
		return []keyRange{{}}
	}
	type step struct{ key, rows int64 }
	var steps []step
	var sum int64
	for rs.Next() {
		var st step
		if rs.Scan(&st.key, &st.rows) == nil {
			steps = append(steps, st)
			sum += st.rows
		}
	}
	rs.Close()
	if len(steps) < n || sum <= 0 {
		return []keyRange{{}}
	}
	out := make([]keyRange, n)
	var acc int64
	part := 1
	for _, st := range steps {
		acc += st.rows
		if part < n && acc >= sum*int64(part)/int64(n) {
			b := st.key + 1 // range i ends after this histogram step
			out[part-1].hi, out[part].lo = &b, &b
			part++
		}
	}
	if part < n {
		return []keyRange{{}}
	}
	return out
}
