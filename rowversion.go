package main

import (
	"database/sql"
	"encoding/binary"
	"fmt"
)

func (ds *dataSync) minActiveRowVersion() (string, error) {
	var mark string
	err := ds.src.meta.QueryRowContext(ds.rc.ctx, "SELECT CONVERT(varchar(18), CAST(MIN_ACTIVE_ROWVERSION() AS binary(8)), 1)").Scan(&mark)
	return mark, err
}

// rowVersionSync: the target equals the source as of the stored mark (no foreign target writes,
// checked by the caller), so the differences are exactly the rows with rowversion >= mark whose
// hash differs from the target's row with that key — plus deletes, which the row counts expose
// (source = target + new keys, else ok=false and the caller runs the full IBLT pass).
func (ds *dataSync) rowVersionSync(m *tableMeta, ts *TableState, key string) (ok bool, changed int, err error) {
	ctx := ds.rc.ctx
	newMark, err := ds.minActiveRowVersion() // before reading: later commits are caught next time
	if err != nil {
		return false, 0, err
	}
	q := ds.src.scanPrefix() + fmt.Sprintf("SELECT x.h, x.k FROM %s AS t %s WHERE t.%s >= CONVERT(binary(8), @m, 1)",
		ds.src.ref(m), m.hashApply("t"), qi(m.RowVersion)) + fmt.Sprintf(" OPTION (MAXDOP %d)", ds.src.scanDop(ds.scanRows, m))
	ds.written(q, 20)
	rows, err := ds.src.exec.QueryContext(ctx, q, sql.Named("m", ts.RvMark))
	if err != nil {
		return false, 0, err
	}
	src := map[key128]uint64{}
	for rows.Next() {
		var h, k []byte
		if err := rows.Scan(&h, &k); err != nil {
			rows.Close()
			return false, 0, err
		}
		var kb [16]byte
		copy(kb[:], k)
		src[key128{int64(binary.BigEndian.Uint64(kb[:8])), binary.BigEndian.Uint64(kb[8:])}] = binary.BigEndian.Uint64(h[:8])
	}
	rows.Close()
	ds.in(int64(len(src) * 48))
	keys := map[key128]bool{}
	for k := range src {
		keys[k] = true
	}
	tgtRows, err := ds.targetRowHashes(m, keys)
	if err != nil {
		return false, 0, err
	}
	tgt := map[key128]uint64{}
	for _, e := range tgtRows {
		tgt[key128{e.K, e.K2}] = e.F
	}
	var elems []ibltElem
	newKeys := 0
	for k, f := range src {
		tf, there := tgt[k]
		if !there {
			newKeys++
		}
		if there && tf == f {
			continue
		}
		elems = append(elems, ibltElem{F: f, K: k.k, K2: k.k2, Count: 1})
		if there {
			elems = append(elems, ibltElem{F: tf, K: k.k, K2: k.k2, Count: -1})
		}
	}
	// deletes: exact counts (narrowest index, no hashing) — source must be target + new keys
	cnt := func(s *side) (int64, error) {
		var n int64
		err := s.exec.QueryRowContext(ctx, s.scanPrefix()+"SELECT COUNT_BIG(*) FROM "+s.ref(m)).Scan(&n)
		return n, err
	}
	sn, tn, err := both(cnt, ds.src, ds.tgt)
	if err != nil {
		return false, 0, err
	}
	if sn != tn+int64(newKeys) {
		return false, 0, nil
	}
	if len(elems) > 0 {
		var tgtBig []ibltCell
		if ts.TgtCached {
			if b, err := ds.rc.loadCells(key); err == nil && len(b) == 3*cacheL*cellBytes {
				tgtBig = decodeCells(b)
			}
		}
		if err := ds.apply(m, elems, ts, "", nil); err != nil {
			return false, 0, err
		}
		if tgtBig != nil { // keep the target cell cache exact (same update as after an IBLT pass)
			ins := map[key128]bool{}
			for _, e := range elems {
				if e.Count < 0 {
					addElem(tgtBig, cacheL, e.F, e.K, e.K2, -1)
				} else {
					ins[key128{e.K, e.K2}] = true
				}
			}
			written, err := ds.targetRowHashes(m, ins)
			if err != nil {
				return false, 0, err
			}
			for _, e := range written {
				addElem(tgtBig, cacheL, e.F, e.K, e.K2, 1)
			}
			ds.saveCellCache(key, ts, tgtBig, false)
		} else {
			ts.TgtCached = false
		}
	}
	ts.RvMark = newMark
	return true, len(elems), nil
}
