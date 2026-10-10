package main

import (
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"time"
)

//
// The target changes only through this service. Its cells are kept at a fixed large size cacheL
// (per sub-table); an IBLT of any power-of-two size s <= cacheL is a fold of them, because every
// cell position is x mod s and x mod cacheL mod s = x mod s. After applying, the cache is updated
// by the decoded target versions (removed) and the written rows read back by key (added), so the
// next run scans only the source: half the hashing work. Safety: used only when the target's row
// operation counter did not move since our last write, every target-side element is verified by
// key before anything is applied, and the target is scanned for real at least every 24h.

const (
	cacheL    = 4096 // cells per sub-table in the cache (decodes up to ~6000 differences)
	cellBytes = 64   // 8 x int64
)

func nextPow2(n int) int {
	p := 16
	for p < n {
		p *= 2
	}
	return p
}

func encodeCells(c []ibltCell) []byte {
	b := make([]byte, len(c)*cellBytes)
	for i, x := range c {
		o := b[i*cellBytes:]
		for j, v := range [8]int64{x.n, x.fh, x.fl, x.kh, x.kl, x.k2h, x.k2l, x.gs} {
			binary.LittleEndian.PutUint64(o[j*8:], uint64(v))
		}
	}
	return b
}

func decodeCells(b []byte) []ibltCell {
	c := make([]ibltCell, len(b)/cellBytes)
	for i := range c {
		o := b[i*cellBytes:]
		v := func(j int) int64 { return int64(binary.LittleEndian.Uint64(o[j*8:])) }
		c[i] = ibltCell{v(0), v(1), v(2), v(3), v(4), v(5), v(6), v(7)}
	}
	return c
}

func denseCells(m map[int]ibltCell, n int) []ibltCell {
	c := make([]ibltCell, n)
	for i, x := range m {
		c[i] = x
	}
	return c
}

// foldCells: the IBLT of size s (power of two <= L) from cells of size L.
func foldCells(big []ibltCell, L, s int) map[int]ibltCell {
	out := map[int]ibltCell{}
	for i, x := range big {
		if x.zero() {
			continue
		}
		t := (i/L)*s + (i%L)%s
		o := out[t]
		o.n, o.fh, o.fl, o.kh, o.kl, o.k2h, o.k2l, o.gs = o.n+x.n, o.fh+x.fh, o.fl+x.fl, o.kh+x.kh, o.kl+x.kl, o.k2h+x.k2h, o.k2l+x.k2l, o.gs+x.gs
		out[t] = o
	}
	return out
}

// addElem adds cnt copies of a row (F, K, K2) to cells of size L, exactly as the SQL aggregation.
func addElem(cells []ibltCell, L int, f uint64, k int64, k2 uint64, cnt int64) {
	h32 := func(v uint64) (int64, int64) { return int64(int32(uint32(v >> 32))), int64(int32(uint32(v))) }
	hi, lo := h32(f)
	kh, kl := h32(uint64(k))
	k2h, k2l := h32(k2)
	pos, g := ibltHash(f, L)
	for _, p := range pos {
		c := &cells[p]
		c.n += cnt
		c.fh += cnt * hi
		c.fl += cnt * lo
		c.kh += cnt * kh
		c.kl += cnt * kl
		c.k2h += cnt * k2h
		c.k2l += cnt * k2l
		c.gs += cnt * int64(g)
	}
}

func (ds *dataSync) saveCellCache(key string, ts *TableState, big []ibltCell, scanned bool) {
	if big == nil || ds.rc.saveCells == nil {
		return
	}
	if err := ds.rc.saveCells(key, encodeCells(big)); err != nil {
		ds.rc.log.Printf("%s: target cell cache not saved: %v", key, err)
		return
	}
	ts.TgtCached = true
	if scanned {
		ts.TgtScanned = time.Now().UTC().Format(time.RFC3339)
	}
}

// keyObjs: PK objects (JSON, as pkWith reads them) for keys that are the PK itself.
func (ds *dataSync) keyObjs(m *tableMeta, keys map[key128]bool) ([]json.RawMessage, error) {
	if m.RawKey {
		return ds.rawPKs(m, keys)
	}
	out := make([]json.RawMessage, 0, len(keys))
	for k := range keys {
		out = append(out, json.RawMessage(fmt.Sprintf(`{"c%d":%d}`, m.PK[0], k.k)))
	}
	return out, nil
}

// targetRowHashes: (F, K, K2) of the target rows with the given keys (index seeks).
func (ds *dataSync) targetRowHashes(m *tableMeta, keys map[key128]bool) ([]ibltElem, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	objs, err := ds.keyObjs(m, keys)
	if err != nil {
		return nil, err
	}
	var out []ibltElem
	for i := 0; i < len(objs); i += 20000 {
		p, _ := json.Marshal(objs[i:min(i+20000, len(objs))])
		q := lowImpact + fmt.Sprintf("SELECT x.h, x.k FROM OPENJSON(@p) WITH (%s) AS j JOIN %s AS t ON %s\n%s",
			m.pkWith(), ds.tgt.ref(m), m.pkJoin("t", "j"), m.hashApply("t"))
		ds.written(q, len(p)*2)
		rows, err := ds.tgt.exec.QueryContext(ds.rc.ctx, q, sql.Named("p", string(p)))
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var h, k []byte
			if err := rows.Scan(&h, &k); err != nil {
				rows.Close()
				return nil, err
			}
			var kb [16]byte
			copy(kb[:], k)
			out = append(out, ibltElem{F: binary.BigEndian.Uint64(h[:8]), K: int64(binary.BigEndian.Uint64(kb[:8])),
				K2: binary.BigEndian.Uint64(kb[8:]), Count: 1})
		}
		rows.Close()
		ds.in(int64(len(out) * 48))
	}
	return out, nil
}

// verifyTargetElems: every target-side element exists on the target with exactly that hash.
func (ds *dataSync) verifyTargetElems(m *tableMeta, elems []ibltElem) (bool, error) {
	want := map[key128]uint64{}
	keys := map[key128]bool{}
	for _, e := range elems {
		if e.Count < 0 {
			if e.Count != -1 {
				return false, nil
			}
			want[key128{e.K, e.K2}] = e.F
			keys[key128{e.K, e.K2}] = true
		}
	}
	got, err := ds.targetRowHashes(m, keys)
	if err != nil {
		return false, err
	}
	if len(got) != len(want) {
		return false, nil
	}
	for _, g := range got {
		if f, ok := want[key128{g.K, g.K2}]; !ok || f != g.F {
			return false, nil
		}
	}
	return true, nil
}
