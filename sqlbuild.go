package main

// SQL fragments for column lists, primary-key JSON and keyset paging.

import (
	"fmt"
	"strings"
)

func (m *tableMeta) colRef(alias string, i int) string { return alias + "." + qi(m.Cols[i].Name) }

func (m *tableMeta) colList() string {
	out := make([]string, len(m.Cols))
	for i, c := range m.Cols {
		out[i] = qi(c.Name)
	}
	return strings.Join(out, ", ")
}

func (m *tableMeta) encSelect(alias string) string {
	out := make([]string, len(m.Cols))
	for i, c := range m.Cols {
		out[i] = fmt.Sprintf("%s AS [c%d]", c.srcEnc(m.colRef(alias, i)), i)
	}
	return strings.Join(out, ", ")
}

func (m *tableMeta) pkOrder(alias, dir string) string {
	out := make([]string, len(m.PK))
	for n, i := range m.PK {
		out[n] = m.colRef(alias, i) + dir
	}
	return strings.Join(out, ", ")
}

const keysFromJSON = "(SELECT CAST([value] AS bigint) FROM OPENJSON(@k))"

func (m *tableMeta) pkSelect(alias string) string {
	out := make([]string, len(m.PK))
	for n, i := range m.PK {
		out[n] = fmt.Sprintf("%s AS [c%d]", m.Cols[i].srcEnc(m.colRef(alias, i)), i)
	}
	return strings.Join(out, ", ")
}

func (m *tableMeta) pkWith() string {
	out := make([]string, len(m.PK))
	for n, i := range m.PK {
		out[n] = fmt.Sprintf("[c%d] %s '$.c%d'", i, m.Cols[i].withType(), i)
	}
	return strings.Join(out, ", ")
}

func (m *tableMeta) pkJoin(t, j string) string {
	out := make([]string, len(m.PK))
	for n, i := range m.PK {
		out[n] = m.colRef(t, i) + " = " + m.Cols[i].insExpr(fmt.Sprintf("%s.[c%d]", j, i))
	}
	return strings.Join(out, " AND ")
}

// keyVars unpacks a JSON key (@last) into typed variables and returns the keyset predicate on
// them: a predicate on variables is an index seek, while comparing against OPENJSON columns made
// SQL Server rescan the table from the start for every chunk.
func (m *tableMeta) keyVars(last string) (decl, where string) {
	vars, with, sets := make([]string, len(m.PK)), make([]string, len(m.PK)), make([]string, len(m.PK))
	for n, i := range m.PK {
		c := m.Cols[i]
		vars[n] = fmt.Sprintf("@k%d %s", n, c.decl())
		with[n] = fmt.Sprintf("[k%d] %s '$.c%d'", n, c.withType(), i)
		sets[n] = fmt.Sprintf("@k%d = %s", n, c.insExpr(fmt.Sprintf("l.[k%d]", n)))
	}
	decl = fmt.Sprintf("DECLARE %s;\nSELECT %s FROM OPENJSON(@last) WITH (%s) AS l;\n",
		strings.Join(vars, ", "), strings.Join(sets, ", "), strings.Join(with, ", "))
	where = " WHERE " + m.pkAfter(0)
	if len(m.PK) > 1 {
		where = fmt.Sprintf(" WHERE %s >= @k0 AND %s", m.colRef("t", m.PK[0]), m.pkAfter(0)) // seekable leading range
	}
	return decl, where
}

// pkAfter builds the keyset predicate (t.pk1, t.pk2, ...) > (@k0, @k1, ...).
func (m *tableMeta) pkAfter(n int) string {
	col, val := m.colRef("t", m.PK[n]), fmt.Sprintf("@k%d", n)
	if n == len(m.PK)-1 {
		return col + " > " + val
	}
	return fmt.Sprintf("(%s > %s OR (%s = %s AND %s))", col, val, col, val, m.pkAfter(n+1))
}
