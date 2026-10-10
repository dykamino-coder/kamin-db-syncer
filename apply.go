package main

// Applying decoded differences: rows located by key, moved as compressed JSON blobs.

import (
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"
	"unicode/utf16"
	"unicode/utf8"

	kgzip "github.com/klauspost/compress/gzip"
)

// apply: hash keys -> PK values with one scan per side (no locks), then batches located by PK
// index seeks. Each batch is its own short transaction: delete old versions + insert new ones,
// so a row is never missing on the target and locks are held for ~batchTarget at most.
func (ds *dataSync) apply(m *tableMeta, elems []ibltElem, ts *TableState, where string, args []any) error {
	if len(m.PK) == 0 {
		return ds.applyHeap(m, elems)
	}
	ctx, log := ds.rc.ctx, ds.rc.log
	// a row is identified by K and K2 (raw keys put the 9th..16th key bytes into K2)
	ins, delOnly := map[key128]bool{}, map[key128]bool{}
	for _, e := range elems {
		if e.Count > 0 {
			ins[key128{e.K, e.K2}] = true
		}
	}
	for _, e := range elems {
		if k := (key128{e.K, e.K2}); e.Count < 0 && !ins[k] {
			delOnly[k] = true
		}
	}
	var srcPKs, tgtPKs []json.RawMessage
	var err error
	if m.RawKey { // whole keys travelled in the cells: decoded by one query, no table scan
		if srcPKs, err = ds.rawPKs(m, ins); err != nil {
			return err
		}
		if tgtPKs, err = ds.rawPKs(m, delOnly); err != nil {
			return err
		}
	} else {
		if srcPKs, err = ds.resolvePKs(ds.src, m, k64(ins), where, args); err != nil {
			return err
		}
		if tgtPKs, err = ds.resolvePKs(ds.tgt, m, k64(delOnly), where, args); err != nil {
			return err
		}
	}
	if err := ds.disableFKs(); err != nil {
		return err
	}
	tref := ds.tgt.ref(m)
	if m.HasTriggers {
		if _, err := ds.tgt.exec.ExecContext(ctx, "ALTER TABLE "+tref+" DISABLE TRIGGER ALL"); err != nil {
			return err
		}
		defer ds.tgt.exec.ExecContext(context.WithoutCancel(ctx), "ALTER TABLE "+tref+" ENABLE TRIGGER ALL")
	}

	b := ds.batcher(max(ts.BatchRows, 500), 50, 100000, ds.rc.job.Config.Data.ChunkRows)
	var upserted, deleted int64
	// target-only keys go first: under a case/accent-insensitive or trailing-space-padded PK such a
	// key can equal a source key, and deleting it after the upsert would remove the row just written
	for i := 0; i < len(tgtPKs); {
		batch := tgtPKs[i:min(i+b.size, len(tgtPKs))]
		started := time.Now()
		p, _ := json.Marshal(batch)
		dq := lowImpact + fmt.Sprintf("DELETE t FROM %s AS t JOIN OPENJSON(@p) WITH (%s) AS j ON %s",
			tref, m.pkWith(), m.pkJoin("t", "j"))
		ds.written(dq, len(p)*2)
		if _, err := ds.tgt.exec.ExecContext(ctx, dq, sql.Named("p", string(p))); err != nil {
			return err
		}
		deleted += int64(len(batch)) // NOCOUNT ON: RowsAffected is 0; every key here exists only on the target
		b.observe(len(batch), time.Since(started), 0)
		i += len(batch)
	}
	for i := 0; i < len(srcPKs); {
		batch := srcPKs[i:min(i+b.size, len(srcPKs))]
		started := time.Now()
		p, _ := json.Marshal(batch)
		q := ds.src.readPrefix() + "SELECT " + ds.src.dataJSON(fmt.Sprintf("SELECT %s FROM %s AS t JOIN OPENJSON(@p) WITH (%s) AS j ON %s FOR JSON PATH",
			m.encSelect("t"), ds.src.ref(m), m.pkWith(), m.pkJoin("t", "j")))
		ds.written(q, len(p)*2)
		var blob []byte
		if err := ds.src.exec.QueryRowContext(ctx, q, sql.Named("p", string(p))).Scan(&blob); err != nil {
			return err
		}
		n, err := ds.inTx(func(tx *sql.Tx) (int64, error) {
			dq := lowImpact + fmt.Sprintf("DELETE t FROM %s AS t JOIN OPENJSON(@p) WITH (%s) AS j ON %s",
				tref, m.pkWith(), m.pkJoin("t", "j"))
			ds.written(dq, len(p)*2)
			if _, err := tx.ExecContext(ctx, dq, sql.Named("p", string(p))); err != nil {
				return 0, err
			}
			return ds.insertBlob(tx, m, tref, blob, false)
		})
		if err != nil {
			return err
		}
		upserted += n
		b.observe(len(batch), time.Since(started), len(blob))
		i += len(batch)
	}
	ts.BatchRows = b.size
	neg := map[key128]bool{}
	for _, e := range elems {
		if e.Count < 0 {
			neg[key128{e.K, e.K2}] = true
		}
	}
	var updated int64
	for k := range ins {
		if neg[k] {
			updated++
		}
	}
	ds.cur.Action = "changed"
	ds.cur.Updated, ds.cur.Inserted, ds.cur.Deleted = updated, int64(len(ins))-updated, int64(len(delOnly))
	ds.rc.stats.rows.Add(upserted + deleted)
	ds.rc.stats.tables.Add(1)
	log.Printf("%s.%s: applied — %d inserted/updated, %d deleted (batch %d rows)", m.Schema, m.Name, upserted, deleted, b.size)
	return nil
}

// applyHeap: a table without a primary key is a multiset of rows keyed by their hash. Exactly
// Count surplus copies are deleted on the target / copied from the source, in one transaction.
func (ds *dataSync) applyHeap(m *tableMeta, elems []ibltElem) error {
	ctx, log := ds.rc.ctx, ds.rc.log
	type hc struct {
		H int64 `json:"h"`
		C int   `json:"c"`
	}
	var ins, del []hc
	for _, e := range elems {
		if e.Count > 0 {
			ins = append(ins, hc{e.K, e.Count})
		} else {
			del = append(del, hc{e.K, -e.Count})
		}
	}
	if err := ds.disableFKs(); err != nil {
		return err
	}
	tref, sref := ds.tgt.ref(m), ds.src.ref(m)
	if m.HasTriggers {
		if _, err := ds.tgt.exec.ExecContext(ctx, "ALTER TABLE "+tref+" DISABLE TRIGGER ALL"); err != nil {
			return err
		}
		defer ds.tgt.exec.ExecContext(context.WithoutCancel(ctx), "ALTER TABLE "+tref+" ENABLE TRIGGER ALL")
	}
	numbered := func(ref string) string {
		return fmt.Sprintf(`SELECT t.*, x.[__h8], ROW_NUMBER() OVER (PARTITION BY x.[__h8] ORDER BY (SELECT NULL)) AS [__rn]
FROM %s AS t CROSS APPLY (SELECT MAX(v.h) AS [__h8] FROM (VALUES (%s)) v(h)) x
WHERE x.[__h8] IN (SELECT h FROM OPENJSON(@k) WITH (h bigint '$.h'))`, ref, m.kh8("t"))
	}
	const want = "JOIN OPENJSON(@k) WITH (h bigint '$.h', c int '$.c') AS j ON j.h = d.[__h8] WHERE d.[__rn] <= j.c"
	// the row hash above is pinned by MAX over a one-row VALUES (else SQL Server recomputes it for
	// the WHERE, the PARTITION BY and the join); the scans use the reconciliation MAXDOP
	opt := func(s *side) string { return fmt.Sprintf(" OPTION (MAXDOP %d)", s.scanDop(ds.scanRows, m)) }

	// source rows to copy are read before the target transaction and while the target deletes:
	// the two full scans overlap and no target lock is held during the source scan
	type fetched struct {
		blobs [][]byte
		err   error
	}
	srcDone := make(chan fetched, 1)
	go func() {
		var f fetched
		for i := 0; i < len(ins); i += 20000 {
			k, _ := json.Marshal(ins[i:min(i+20000, len(ins))])
			q := ds.src.readPrefix() + "SELECT " + ds.src.dataJSON(fmt.Sprintf("SELECT %s FROM (%s) d %s FOR JSON PATH",
				m.encSelect("d"), numbered(sref), want)) + opt(ds.src)
			ds.written(q, len(k)*2)
			var blob []byte
			if f.err = ds.src.exec.QueryRowContext(ctx, q, sql.Named("k", string(k))).Scan(&blob); f.err != nil {
				break
			}
			f.blobs = append(f.blobs, blob)
		}
		srcDone <- f
	}()
	var deleted, inserted int64
	n, err := ds.inTx(func(tx *sql.Tx) (int64, error) {
		for i := 0; i < len(del); i += 20000 {
			k, _ := json.Marshal(del[i:min(i+20000, len(del))])
			q := lowImpact + "WITH d AS (" + numbered(tref) + ")\nDELETE d FROM d " + want + opt(ds.tgt)
			ds.written(q, len(k)*2)
			r, err := tx.ExecContext(ctx, q, sql.Named("k", string(k)))
			if err != nil {
				return 0, err
			}
			_, _ = r.RowsAffected() // 0 under SET NOCOUNT ON: count from the decoded elements instead
			for _, x := range del[i:min(i+20000, len(del))] {
				deleted += int64(x.C)
			}
		}
		f := <-srcDone
		if f.err != nil {
			return 0, f.err
		}
		for _, blob := range f.blobs {
			c, err := ds.insertBlob(tx, m, tref, blob, false)
			if err != nil {
				return 0, err
			}
			inserted += c
		}
		return deleted + inserted, nil
	})
	if err != nil {
		return err
	}
	ds.rc.stats.rows.Add(n)
	ds.rc.stats.tables.Add(1)
	log.Printf("%s.%s (no PK): %d rows deleted, %d inserted", m.Schema, m.Name, deleted, inserted)
	ds.cur.Action, ds.cur.Inserted, ds.cur.Deleted, ds.cur.Note = "changed", inserted, deleted, "table without a primary key"
	return nil
}

func (ds *dataSync) inTx(f func(*sql.Tx) (int64, error)) (int64, error) {
	tx, err := ds.tgt.exec.BeginTx(ds.rc.ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return 0, err
	}
	n, err := f(tx)
	if err != nil {
		tx.Rollback()
		return 0, err
	}
	return n, tx.Commit()
}

// resolvePKs turns 64-bit key hashes into PK values (JSON objects) with one lock-free scan per
// 100k keys; later statements then use index seeks instead of rescanning the table.
func (ds *dataSync) resolvePKs(s *side, m *tableMeta, keys map[int64]bool, where string, args []any) ([]json.RawMessage, error) {
	var out []json.RawMessage
	list := make([]int64, 0, len(keys))
	for k := range keys {
		list = append(list, k)
	}
	if m.IntPK { // the key is the PK value itself: nothing to look up
		for _, k := range list {
			out = append(out, json.RawMessage(fmt.Sprintf(`{"c%d":%d}`, m.PK[0], k)))
		}
		return out, nil
	}
	if where != "" {
		where = " AND " + strings.TrimPrefix(strings.TrimSpace(where), "WHERE ")
	}
	for i := 0; i < len(list); i += 100000 {
		kj, _ := json.Marshal(list[i:min(i+100000, len(list))])
		q := s.scanPrefix() + fmt.Sprintf("SELECT COMPRESS((SELECT %s FROM %s AS t WHERE %s IN %s%s FOR JSON PATH))",
			m.pkSelect("t"), s.ref(m), m.kh8("t"), keysFromJSON, where) + fmt.Sprintf(" OPTION (MAXDOP %d)", s.scanDop(ds.scanRows, m))
		ds.written(q, len(kj)*2)
		var blob []byte
		if err := s.exec.QueryRowContext(ds.rc.ctx, q, append([]any{sql.Named("k", string(kj))}, args...)...).Scan(&blob); err != nil {
			return nil, err
		}
		if len(blob) == 0 {
			continue
		}
		ds.in(int64(len(blob)))
		var part []json.RawMessage
		if err := gunzipJSON(blob, &part); err != nil {
			return nil, err
		}
		out = append(out, part...)
	}
	return out, nil
}

func gunzipJSON(blob []byte, v any) error {
	zr, err := gzip.NewReader(bytes.NewReader(blob))
	if err != nil {
		return err
	}
	defer zr.Close()
	raw, err := io.ReadAll(zr)
	if err != nil {
		return err
	}
	// COMPRESS() of nvarchar is gzip of UTF-16LE text
	u := make([]uint16, len(raw)/2)
	for i := range u {
		u[i] = uint16(raw[2*i]) | uint16(raw[2*i+1])<<8
	}
	return json.Unmarshal([]byte(string(utf16.Decode(u))), v)
}

type execer interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func (ds *dataSync) insertBlob(x execer, m *tableMeta, tref string, blob []byte, tablock bool) (int64, error) {
	if len(blob) == 0 {
		return 0, nil
	}
	ds.in(int64(len(blob)))
	if ds.src.utf8 {
		var err error
		if blob, err = toTargetBlob(blob); err != nil {
			return 0, err
		}
	}
	with := make([]string, len(m.Cols))
	sel := make([]string, len(m.Cols))
	for i, c := range m.Cols {
		with[i] = fmt.Sprintf("[c%d] %s '$.c%d'", i, c.withType(), i)
		sel[i] = c.insExpr(fmt.Sprintf("j.[c%d]", i))
	}
	hint := ""
	if tablock {
		hint = " WITH (TABLOCK)"
	}
	var b strings.Builder
	b.WriteString(lowImpact + "DECLARE @r bigint;\n")
	if m.HasIdentity {
		b.WriteString("SET IDENTITY_INSERT " + tref + " ON;\n")
	}
	fmt.Fprintf(&b, "INSERT INTO %s%s (%s) SELECT %s FROM OPENJSON(CAST(DECOMPRESS(@b) AS nvarchar(max))) WITH (%s) AS j;\nSET @r = @@ROWCOUNT;\n",
		tref, hint, m.colList(), strings.Join(sel, ", "), strings.Join(with, ", "))
	if m.HasIdentity {
		b.WriteString("SET IDENTITY_INSERT " + tref + " OFF;\n")
	}
	b.WriteString("SELECT @r;")
	q := b.String()
	ds.written(q, len(blob))
	var n int64
	err := x.QueryRowContext(ds.rc.ctx, q, sql.Named("b", blob)).Scan(&n)
	return n, err
}

type key128 struct {
	k  int64
	k2 uint64
}

func k64(m map[key128]bool) map[int64]bool {
	out := make(map[int64]bool, len(m))
	for k := range m {
		out[k.k] = true
	}
	return out
}

// rawPKs: PK objects (same JSON as resolvePKs) decoded from keys that travelled whole in the
// IBLT cells — one query without table access instead of a scan per side.
func (ds *dataSync) rawPKs(m *tableMeta, keys map[key128]bool) ([]json.RawMessage, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	cols := make([]string, len(m.PK))
	off := 1
	for n, i := range m.PK {
		c := m.Cols[i]
		sz, _ := c.rawKeyBytes()
		cols[n] = fmt.Sprintf("%s AS [c%d]", c.srcEnc(c.rawKeyDec(fmt.Sprintf("SUBSTRING(k.b, %d, %d)", off, sz))), i)
		off += sz
	}
	list := make([]string, 0, len(keys))
	for k := range keys {
		list = append(list, fmt.Sprintf("%016X%016X", uint64(k.k), k.k2))
	}
	var out []json.RawMessage
	for i := 0; i < len(list); i += 100000 {
		kj, _ := json.Marshal(list[i:min(i+100000, len(list))])
		q := "SET NOCOUNT ON; SELECT COMPRESS((SELECT " + strings.Join(cols, ", ") +
			" FROM (SELECT CONVERT(binary(16), j.[value], 2) AS b FROM OPENJSON(@k) j) k FOR JSON PATH))"
		ds.written(q, len(kj)*2)
		var blob []byte
		if err := ds.src.exec.QueryRowContext(ds.rc.ctx, q, sql.Named("k", string(kj))).Scan(&blob); err != nil {
			return nil, err
		}
		ds.in(int64(len(blob)))
		var part []json.RawMessage
		if err := gunzipJSON(blob, &part); err != nil {
			return nil, err
		}
		out = append(out, part...)
	}
	return out, nil
}

// toTargetBlob: gzip(UTF-8 JSON) from the source -> gzip(UTF-16LE JSON) that the target reads with
// CAST(DECOMPRESS(@b) AS nvarchar(max)). Streamed in 64 KB pieces, so memory stays at the size of
// the output blob instead of the whole decompressed text twice.
func toTargetBlob(blob []byte) ([]byte, error) {
	zr, err := kgzip.NewReader(bytes.NewReader(blob))
	if err != nil {
		return nil, err
	}
	defer zr.Close()
	var out bytes.Buffer
	out.Grow(len(blob) * 5 / 4)
	zw, _ := kgzip.NewWriterLevel(&out, blobGzipLevel)
	in := make([]byte, 64<<10)
	u16 := make([]byte, 0, 2*len(in)+8)
	carry := 0 // bytes of an incomplete UTF-8 sequence kept from the previous read
	for {
		n, rerr := io.ReadFull(zr, in[carry:])
		n += carry
		end := n
		if rerr == nil { // more to come: keep a character cut by the buffer end for the next round
			i := n - 1
			for i > 0 && n-i < utf8.UTFMax && !utf8.RuneStart(in[i]) {
				i--
			}
			if !utf8.FullRune(in[i:n]) {
				end = i
			}
		}
		u16 = u16[:0]
		for _, r := range string(in[:end]) {
			if r >= 0x10000 {
				r1, r2 := utf16.EncodeRune(r)
				u16 = append(u16, byte(r1), byte(r1>>8), byte(r2), byte(r2>>8))
				continue
			}
			u16 = append(u16, byte(r), byte(r>>8))
		}
		if _, err := zw.Write(u16); err != nil {
			return nil, err
		}
		carry = copy(in, in[end:n])
		if rerr == io.EOF || rerr == io.ErrUnexpectedEOF {
			break
		}
		if rerr != nil {
			return nil, rerr
		}
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}
