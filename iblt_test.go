package main

import (
	"encoding/binary"
	"math/rand/v2"
	"testing"
)

// build mimics the SQL aggregation: each element is added to its k cells.
func build(rows map[int64]uint64, s int) map[int]ibltCell {
	cells := map[int]ibltCell{}
	for k, f := range rows {
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], f)
		hi, lo := int64(int32(binary.BigEndian.Uint32(b[0:4]))), int64(int32(binary.BigEndian.Uint32(b[4:8])))
		binary.BigEndian.PutUint64(b[:], uint64(k))
		kh, kl := int64(int32(binary.BigEndian.Uint32(b[0:4]))), int64(int32(binary.BigEndian.Uint32(b[4:8])))
		pos, g := ibltHash(f, s)
		for _, p := range pos {
			c := cells[p]
			c.n++
			c.fh += hi
			c.fl += lo
			c.kh += kh
			c.kl += kl
			c.gs += int64(g)
			cells[p] = c
		}
	}
	return cells
}

func TestIBLTDecodesInsertUpdateDelete(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	src := map[int64]uint64{}
	for range 200_000 {
		src[int64(r.Uint64())] = r.Uint64()
	}
	tgt := map[int64]uint64{}
	for k, v := range src {
		tgt[k] = v
	}
	want := map[int64]bool{}
	i := 0
	for k := range src {
		switch {
		case i < 40: // updated in source
			src[k] = r.Uint64()
			want[k] = true
		case i < 70: // deleted in source
			delete(src, k)
			want[k] = true
		case i < 100: // only in target? (= extra row on target)
		default:
		}
		i++
		if i >= 70 {
			break
		}
	}
	for range 30 { // inserted in source
		k := int64(r.Uint64())
		src[k] = r.Uint64()
		want[k] = true
	}

	s := 64 // 192 cells for 140 differing elements (40*2 + 30 + 30)
	elems, _, ok := ibltDecode(build(src, s), build(tgt, s), s)
	for !ok {
		s *= 2
		elems, _, ok = ibltDecode(build(src, s), build(tgt, s), s)
	}
	got := map[int64]bool{}
	for _, e := range elems {
		got[e.K] = true
	}
	if len(got) != len(want) {
		t.Fatalf("decoded %d keys, want %d (s=%d)", len(got), len(want), s)
	}
	for k := range want {
		if !got[k] {
			t.Fatalf("key %d not decoded", k)
		}
	}
	t.Logf("decoded %d elements with %d cells", len(elems), 3*s)
}

func TestIBLTIdenticalIsEmpty(t *testing.T) {
	rows := map[int64]uint64{1: 10, 2: 20, 3: 30}
	elems, _, ok := ibltDecode(build(rows, 8), build(rows, 8), 8)
	if !ok || len(elems) != 0 {
		t.Fatalf("ok=%v elems=%d", ok, len(elems))
	}
}

// tables without a primary key: rows are a multiset keyed by their own hash
func TestIBLTMultiset(t *testing.T) {
	type bag map[uint64]int
	cells := func(b bag, s int) map[int]ibltCell {
		out := map[int]ibltCell{}
		for f, cnt := range b {
			var x [8]byte
			binary.BigEndian.PutUint64(x[:], f)
			hi, lo := int64(int32(binary.BigEndian.Uint32(x[0:4]))), int64(int32(binary.BigEndian.Uint32(x[4:8])))
			pos, g := ibltHash(f, s)
			for _, p := range pos {
				c := out[p]
				c.n += int64(cnt)
				c.fh += int64(cnt) * hi
				c.fl += int64(cnt) * lo
				c.kh += int64(cnt) * hi
				c.kl += int64(cnt) * lo
				c.gs += int64(cnt) * int64(g)
				out[p] = c
			}
		}
		return out
	}
	src := bag{11: 3, 22: 1, 33: 2, 44: 1}
	tgt := bag{11: 1, 22: 1, 33: 2, 55: 2}
	elems, _, ok := ibltDecode(cells(src, 16), cells(tgt, 16), 16)
	if !ok {
		t.Fatal("not decoded")
	}
	got := map[uint64]int{}
	for _, e := range elems {
		got[e.F] += e.Count
	}
	want := map[uint64]int{11: 2, 44: 1, 55: -2}
	for f, c := range want {
		if got[f] != c {
			t.Fatalf("F=%d: count %d, want %d (all: %v)", f, got[f], c, got)
		}
	}
	if len(got) != len(want) {
		t.Fatalf("unexpected elements: %v", got)
	}
}

// Many random diffs around and below the decoding threshold: a successful decode must return
// exactly the true difference (a false "pure" cell would show up as a wrong or extra element).
func TestIBLTNeverDecodesWrong(t *testing.T) {
	r := rand.New(rand.NewPCG(7, 9))
	okRuns := 0
	for trial := range 3000 {
		d := 5 + r.IntN(200)
		elemsN := d + d/3                       // updates give two elements
		s := max(4, elemsN*(30+r.IntN(40))/100) // 3*s cells: 0.9..2.07 per element, around the ~1.23 threshold
		src, tgt := map[int64]uint64{}, map[int64]uint64{}
		for range 500 {
			k, f := int64(r.Uint64()), r.Uint64()
			src[k], tgt[k] = f, f
		}
		want := map[int64]int{}
		for i := range d {
			k := int64(r.Uint64())
			switch i % 3 {
			case 0:
				src[k] = r.Uint64()
				want[k] = 1
			case 1:
				tgt[k] = r.Uint64()
				want[k] = -1
			default:
				src[k], tgt[k] = r.Uint64(), r.Uint64()
				want[k] = 2 // update: +1 and -1
			}
		}
		elems, _, ok := ibltDecode(build(src, s), build(tgt, s), s)
		if !ok {
			continue
		}
		okRuns++
		got := map[int64]int{}
		for _, e := range elems {
			if e.Count > 0 && src[e.K] != e.F || e.Count < 0 && tgt[e.K] != e.F {
				t.Fatalf("trial %d: wrong element %+v", trial, e)
			}
			got[e.K] += 1
		}
		for k, w := range want {
			if n := map[int]int{1: 1, -1: 1, 2: 2}[w]; got[k] != n {
				t.Fatalf("trial %d: key %d decoded %d times, want %d", trial, k, got[k], n)
			}
		}
		if len(got) != len(want) {
			t.Fatalf("trial %d: %d keys decoded, want %d", trial, len(got), len(want))
		}
	}
	if okRuns < 1500 {
		t.Fatalf("only %d successful decodes", okRuns)
	}
	t.Logf("%d of 3000 decodes succeeded, all exact", okRuns)
}

func TestCellCacheFoldAndUpdate(t *testing.T) {
	r := rand.New(rand.NewPCG(3, 4))
	rows := map[int64]uint64{}
	for range 5000 {
		rows[int64(r.Uint64())] = r.Uint64()
	}
	big := make([]ibltCell, 3*cacheL)
	for k, f := range rows {
		addElem(big, cacheL, f, k, 0, 1)
	}
	for _, s := range []int{16, 256, 1024, cacheL} {
		if !cellsEqual(foldCells(big, cacheL, s), build(rows, s)) {
			t.Fatalf("fold to %d differs from a direct build", s)
		}
	}
	// update: replace 100 rows, delete 50, insert 70 — must equal a rebuild of the new set
	n := 0
	for k, f := range rows {
		switch {
		case n < 100:
			addElem(big, cacheL, f, k, 0, -1)
			nf := r.Uint64()
			addElem(big, cacheL, nf, k, 0, 1)
			rows[k] = nf
		case n < 150:
			addElem(big, cacheL, f, k, 0, -1)
			delete(rows, k)
		}
		n++
		if n >= 150 {
			break
		}
	}
	for range 70 {
		k, f := int64(r.Uint64()), r.Uint64()
		rows[k] = f
		addElem(big, cacheL, f, k, 0, 1)
	}
	if !cellsEqual(foldCells(big, cacheL, 512), build(rows, 512)) {
		t.Fatal("updated cache differs from a rebuild")
	}
	if got := decodeCells(encodeCells(big)); len(got) != len(big) || got[17] != big[17] {
		t.Fatal("encode/decode")
	}
}
