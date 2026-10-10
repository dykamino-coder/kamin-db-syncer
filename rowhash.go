package main

// Row and key hashes computed inside SQL Server from a binary canonical form of the row.

import (
	"fmt"
	"strings"
)

// binBytes: upper bound of the binary form (-1 = max).
func (c colMeta) binBytes() int {
	switch {
	case fixedTypes[c.Type]:
		return 20
	case c.Type == "char" || c.Type == "varchar" || c.Type == "nchar" || c.Type == "nvarchar" ||
		c.Type == "binary" || c.Type == "varbinary":
		return c.MaxLen // bytes; -1 for (max)
	}
	return -1 // text, ntext, image, xml, geography, geometry, hierarchyid
}

// binPart: 4-byte length prefix (0xFFFFFFFF for NULL) + raw bytes of the value. Unambiguous,
// lossless and ~10x cheaper for SQL Server than converting values to canonical strings.
//
// Long and LOB values are hashed on their own first (32 bytes): a varbinary(max) anywhere in the
// concatenation turns the whole row expression into a LOB, which measured ~8x slower per row.
func (c colMeta) binPart(ref string) string {
	v := ref
	if c.Type == "geography" || c.Type == "geometry" || c.Type == "hierarchyid" {
		v = "CAST(" + ref + " AS varbinary(max))"
	}
	if fixedTypes[c.Type] {
		// fixed-size types: the value length is constant for the column type, so a 1-byte NULL
		// marker replaces the 4-byte length prefix (~30% less hashing CPU); NOT NULL needs no marker
		if !c.Nullable {
			return "CAST(" + v + " AS varbinary(20))"
		}
		return "ISNULL(0x01 + CAST(" + v + " AS varbinary(20)), 0x00)"
	}
	if n := c.binBytes(); n < 0 || n > inlineBytes {
		return fmt.Sprintf("ISNULL(CAST(DATALENGTH(%s) AS binary(4)), 0xFFFFFFFF) + ISNULL(HASHBYTES('SHA2_256', CAST(%s AS varbinary(max))), 0x)", v, v)
	}
	return fmt.Sprintf("ISNULL(CAST(DATALENGTH(%s) AS binary(4)), 0xFFFFFFFF) + ISNULL(CAST(%s AS varbinary(%d)), 0x)", v, v, c.binBytes())
}

// inlineBytes: values up to this size go into the row hash input as is, longer ones as their hash.
// HASHBYTES costs ~7 µs per call whatever the input size, so every value that can be inlined is:
// only (max)/LOB types get their own hash. Parts are packed into groups of at most 8000 bytes
// (a longer non-max concatenation would be silently truncated by SQL Server).
const inlineBytes = 7996

// partBytes: upper bound of binPart's output.
func (c colMeta) partBytes() int {
	if n := c.binBytes(); n >= 0 && n <= inlineBytes {
		return 4 + n
	}
	return 4 + 32
}

func (m *tableMeta) hashOf(alias string, idx []int) string {
	// groups of parts that fit in varbinary(8000); several groups are hashed and the hashes hashed
	var groups []string
	var cur []string
	size := 0
	for _, i := range idx {
		c := m.Cols[i]
		if size+c.partBytes() > 8000 && len(cur) > 0 {
			groups = append(groups, concatBin(cur))
			cur, size = nil, 0
		}
		cur = append(cur, c.binPart(m.colRef(alias, i)))
		size += c.partBytes()
	}
	groups = append(groups, concatBin(cur))
	if len(groups) == 1 {
		return "HASHBYTES('SHA2_256', " + groups[0] + ")"
	}
	for g := range groups {
		groups[g] = "HASHBYTES('SHA2_256', " + groups[g] + ")"
	}
	return "HASHBYTES('SHA2_256', " + strings.Join(groups, "\n + ") + ")"
}

func (m *tableMeta) all() []int {
	idx := make([]int, len(m.Cols))
	for i := range idx {
		idx[i] = i
	}
	return idx
}

func (m *tableMeta) rowHash(alias string) string { return m.hashOf(alias, m.all()) }

// keyBin: 8+ bytes identifying the row. A single integer PK is used as is (no hashing, and changed
// rows are then found by index seek); other keys are hashed.
func (m *tableMeta) keyBin(alias string) string {
	if m.IntPK {
		return "CAST(CAST(" + m.colRef(alias, m.PK[0]) + " AS bigint) AS binary(8))"
	}
	if m.RawKey {
		parts := make([]string, len(m.PK))
		for n, i := range m.PK {
			parts[n] = m.Cols[i].rawKeyEnc(m.colRef(alias, i))
		}
		return "CAST(" + strings.Join(parts, " + ") + " AS binary(16))" // zero-padded to K + K2
	}
	return m.hashOf(alias, m.PK)
}

// hashApply yields x.h (row hash) and x.k (key bytes); for heaps the key is the row hash itself.
func (m *tableMeta) hashApply(alias string) string {
	// MAX() over a one-row VALUES pins the hash as an aggregate result: SQL Server otherwise
	// re-evaluates the whole row-hash expression for every reference to it (measured 13x slower
	// with 5 references on a 200-column table, 3.3x for the full IBLT query).
	if len(m.PK) == 0 {
		return "CROSS APPLY (SELECT MAX(v.h) AS h, MAX(v.h) AS k FROM (VALUES (" + m.rowHash(alias) + ")) v(h)) x"
	}
	return "CROSS APPLY (SELECT MAX(v.h) AS h, MAX(v.k) AS k FROM (VALUES (" + m.rowHash(alias) + ", " + m.keyBin(alias) + ")) v(h, k)) x"
}

func (m *tableMeta) kh8(alias string) string {
	if len(m.PK) == 0 {
		return "CAST(SUBSTRING(" + m.rowHash(alias) + ", 1, 8) AS bigint)"
	}
	if m.IntPK {
		return "CAST(" + m.colRef(alias, m.PK[0]) + " AS bigint)"
	}
	if m.RawKey {
		return "CAST(SUBSTRING(" + m.keyBin(alias) + ", 1, 8) AS bigint)"
	}
	return "CAST(SUBSTRING(" + m.hashOf(alias, m.PK) + ", 1, 8) AS bigint)"
}

func i32(expr string, from int) string {
	return fmt.Sprintf("CAST(CAST(SUBSTRING(%s, %d, 4) AS int) AS bigint)", expr, from)
}

// concatBin joins binary parts (a group is at most 8000 bytes). For wide rows one CONCAT is ~9%
// cheaper for SQL Server than a chain of "+" that builds an intermediate value per operator; for
// a dozen columns it is ~3% slower, so short groups keep "+". Same bytes, same hash either way.
func concatBin(parts []string) string {
	if len(parts) < 40 {
		return strings.Join(parts, "\n  + ")
	}
	for len(parts) > 254 { // CONCAT takes 2..254 arguments
		var nested []string
		for i := 0; i < len(parts); i += 254 {
			nested = append(nested, concatBin(parts[i:min(i+254, len(parts))]))
		}
		parts = nested
	}
	return "CAST(CONCAT(" + strings.Join(parts, ",\n  ") + ") AS varbinary(8000))"
}
