package main

// Additive Invertible Bloom Lookup Table (Goodrich & Mitzenmacher, arXiv:1101.2245 — cells hold sums,
// XOR is optional) computed set-based inside SQL Server 2019 by GROUP BY, decoded here.
//
// Element = one row, identified by F = first 8 bytes of SHA-256(canonical row) (includes the PK).
// Cell positions and the purity checksum come from F by integer arithmetic modulo the prime
// p = 2^31-1 (a, b = the two halves of F mod p): positions a mod s, b mod s, (a*b mod p) mod s and
// checksum g = (a² + b²) mod p. A second HASHBYTES per row used to do this and cost as much as the
// row hash itself (~7 µs per call on SQL Server, whatever the input size). g is a quadratic form
// with no non-trivial zero mod p (p ≡ 3 mod 4, so -1 is not a square): a cell mixing several
// elements passes the check only with probability ~1/p, the same strength as 32 bits of SHA-256.
// Each cell stores count and sums of F, K (PK) and checksum, with 64-bit values split into signed
// 32-bit halves so SUM(bigint) is exact (SQL Server errors on overflow).
//
// diff = source cells - target cells: rows present on both sides with identical content cancel out,
// what remains decodes into +1 (only/changed in source) and -1 (only/changed in target) elements.
// An updated row yields one +1 and one -1 element with the same K. Tables without a primary key are
// multisets (K = F): identical rows add up, so a cell holding c copies of one element is still
// decodable (all sums divisible by c) and Count says how many copies to insert or delete.

import (
	"encoding/binary"
	"math"
)

const ibltK = 3

// k2h/k2l carry key bytes 9..16: a uniqueidentifier PK travels whole, so changed rows are found
// by index seek instead of an extra scan that maps key hashes back to keys.
type ibltCell struct{ n, fh, fl, kh, kl, k2h, k2l, gs int64 }

func (c *ibltCell) zero() bool { return *c == ibltCell{} }

type ibltElem struct {
	F     uint64
	K     int64  // matches CAST(SUBSTRING(keyhash,1,8) AS bigint) or the integer PK
	K2    uint64 // key bytes 9..16 (whole GUID together with K)
	Count int    // > 0: extra copies in source, < 0: extra copies in target (±1 with a PK)
}

const ibltP = 2147483647 // 2^31-1

// ibltHash reproduces in Go what the SQL computes from F (same truncating % semantics).
func ibltHash(f uint64, s int) (pos [ibltK]int, g int32) {
	a := (int64(int32(f>>32))%ibltP + ibltP) % ibltP
	b := (int64(int32(f))%ibltP + ibltP) % ibltP
	g = int32((a*a%ibltP + b*b%ibltP) % ibltP)
	s64 := int64(s)
	pos[0] = int(a % s64)
	pos[1] = s + int(b%s64)
	pos[2] = 2*s + int(a*b%ibltP%s64)
	return
}

func join32(hi, lo int64) uint64 { return uint64(uint32(int32(hi)))<<32 | uint64(uint32(int32(lo))) }

func fitsInt32(v int64) bool { return v >= math.MinInt32 && v <= math.MaxInt32 }

// ibltDecode peels the difference table. ok=false means it was too small for the difference; est is
// then an estimate of the number of differences (each element adds |1| to k cells).
func ibltDecode(src, tgt map[int]ibltCell, s int) (elems []ibltElem, est int, ok bool) {
	cells := make([]ibltCell, ibltK*s)
	for i, c := range src {
		cells[i] = c
	}
	for i, c := range tgt {
		d := &cells[i]
		d.n, d.fh, d.fl, d.kh, d.kl, d.k2h, d.k2l, d.gs = d.n-c.n, d.fh-c.fh, d.fl-c.fl, d.kh-c.kh, d.kl-c.kl, d.k2h-c.k2h, d.k2l-c.k2l, d.gs-c.gs
	}
	var abs int64
	for _, c := range cells {
		abs += max(c.n, -c.n)
	}
	est = int(abs / ibltK)

	pure := func(i int) (ibltElem, bool) {
		c := cells[i]
		n := c.n
		if n == 0 || c.fh%n != 0 || c.fl%n != 0 || c.kh%n != 0 || c.kl%n != 0 || c.k2h%n != 0 || c.k2l%n != 0 || c.gs%n != 0 {
			return ibltElem{}, false
		}
		hi, lo, kh, kl, k2h, k2l, g := c.fh/n, c.fl/n, c.kh/n, c.kl/n, c.k2h/n, c.k2l/n, c.gs/n
		if !fitsInt32(hi) || !fitsInt32(lo) || !fitsInt32(kh) || !fitsInt32(kl) || !fitsInt32(k2h) || !fitsInt32(k2l) || !fitsInt32(g) {
			return ibltElem{}, false
		}
		f := join32(hi, lo)
		pos, gg := ibltHash(f, s)
		if int64(gg) != g || pos[i/s] != i {
			return ibltElem{}, false
		}
		return ibltElem{F: f, K: int64(join32(kh, kl)), K2: join32(k2h, k2l), Count: int(n)}, true
	}

	queue := make([]int, 0, len(cells))
	for i := range cells {
		queue = append(queue, i)
	}
	for len(queue) > 0 {
		i := queue[len(queue)-1]
		queue = queue[:len(queue)-1]
		e, isPure := pure(i)
		if !isPure {
			continue
		}
		elems = append(elems, e)
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], e.F)
		hi, lo := int64(int32(binary.BigEndian.Uint32(b[0:4]))), int64(int32(binary.BigEndian.Uint32(b[4:8])))
		binary.BigEndian.PutUint64(b[:], uint64(e.K))
		kh, kl := int64(int32(binary.BigEndian.Uint32(b[0:4]))), int64(int32(binary.BigEndian.Uint32(b[4:8])))
		binary.BigEndian.PutUint64(b[:], e.K2)
		k2h, k2l := int64(int32(binary.BigEndian.Uint32(b[0:4]))), int64(int32(binary.BigEndian.Uint32(b[4:8])))
		pos, g := ibltHash(e.F, s)
		sg := int64(e.Count)
		for _, p := range pos {
			c := &cells[p]
			c.n -= sg
			c.fh -= sg * hi
			c.fl -= sg * lo
			c.kh -= sg * kh
			c.kl -= sg * kl
			c.k2h -= sg * k2h
			c.k2l -= sg * k2l
			c.gs -= sg * int64(g)
			queue = append(queue, p)
		}
	}
	for i := range cells {
		if !cells[i].zero() {
			return elems, est, false
		}
	}
	return elems, est, true
}
