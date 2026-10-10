package main

// One side of a sync (source or target): connections, isolation, parallelism and blob format.

import (
	"context"
	"database/sql"
	"fmt"
)

type side struct {
	ep         Endpoint
	meta       *sql.DB // database context: metadata
	exec       *sql.DB // OPENJSON needs compat level >= 130; older databases are driven from tempdb
	compat     int
	snapshot   bool   // ALLOW_SNAPSHOT_ISOLATION on: consistent reads without any shared locks
	rcsi       bool   // READ_COMMITTED_SNAPSHOT on: READ COMMITTED takes no shared locks
	utf8       bool   // SQL Server 2019+: row blobs are compressed as UTF-8 (see dataJSON)
	bigDop     int    // MAXDOP for hashing scans of big tables, decided per run from the server load
	instance   string // @@SERVERNAME + start time: tells whether source and target share one server
	quietCores int    // cores a reload may use on this side (1/SCAN_CORE_SHARE of a quiet server), 0 = busy/unknown
	loadNote   string
}

// scanDop: parallelism of a hashing scan over rows rows of m. Wide rows are hashing-bound and get
// twice the base (200 columns, 20 cores: MAXDOP 5 16-18s, 8 13-14s; whole round 27-30s -> 26s,
// the same as giving every table all cores).
func (s *side) scanDop(rows int64, m *tableMeta) int {
	if rows < bigScanRows || s.bigDop <= scanMaxDop {
		return scanMaxDop
	}
	if len(m.Cols) >= 50 {
		return min(scanMaxDopMax, 2*s.bigDop)
	}
	return s.bigDop
}

// decideDop: more cores for big scans only when the server is quiet — busiest minute of the last
// five (scheduler monitor ring buffer, needs VIEW SERVER STATE) of SQL Server CPU and, where the OS
// reports it (Windows), of all processes. Unknown load or a busy server keeps SCAN_MAXDOP.
// Budget: all concurrent scans of this run on one server get at most a quarter of its cores.
func (s *side) decideDop(ctx context.Context, parallelTables int) {
	s.bigDop = scanMaxDop
	if scanMaxDopMax <= scanMaxDop {
		return
	}
	var cpus, busy sql.NullInt64
	s.meta.QueryRowContext(ctx, `SELECT CONCAT(@@SERVERNAME, '|', (SELECT sqlserver_start_time FROM sys.dm_os_sys_info))`).Scan(&s.instance)
	err := s.meta.QueryRowContext(ctx, `SET QUOTED_IDENTIFIER ON;
SELECT (SELECT cpu_count FROM sys.dm_os_sys_info),
       (SELECT MAX(IIF(v.idle > 0, 100 - v.idle, v.sqlcpu)) FROM (
          SELECT TOP (5) x.r.value('(Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int') AS idle,
                 x.r.value('(Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'int') AS sqlcpu
          FROM sys.dm_os_ring_buffers b CROSS APPLY (SELECT CAST(b.record AS xml)) x(r)
          WHERE b.ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR' AND b.record LIKE N'%<SystemHealth>%'
          ORDER BY b.[timestamp] DESC) v)`).Scan(&cpus, &busy)
	if err != nil || !cpus.Valid || !busy.Valid {
		s.loadNote = "server load unknown"
		return
	}
	if busy.Int64 >= quietCPUPct {
		s.loadNote = fmt.Sprintf("server busy (CPU up to %d%%)", busy.Int64)
		return
	}
	s.bigDop = max(scanMaxDop, min(scanMaxDopMax, int(cpus.Int64)/(scanCoreShare*max(1, parallelTables))))
	s.quietCores = int(cpus.Int64) / scanCoreShare
	s.loadNote = fmt.Sprintf("CPU up to %d%% of %d cores", busy.Int64, cpus.Int64)
}

// dataJSON wraps a "SELECT ... FOR JSON PATH" reading rows from this side into the compressed blob
// expression. FOR JSON yields UTF-16; on 2019+ it is converted to UTF-8 before COMPRESS: ~40% less
// CPU for the source (COMPRESS cost follows input size) and ~20% fewer bytes on the wire. The
// worker turns the blob back into gzip of UTF-16 for the target (toTargetBlob): SQL Server cannot
// read raw bytes as UTF-8 unless the database itself has a UTF-8 default collation.
func (s *side) dataJSON(inner string) string {
	if s.utf8 {
		return "COMPRESS(CAST((" + inner + ") COLLATE Latin1_General_100_BIN2_UTF8 AS varchar(max)))"
	}
	return "COMPRESS((" + inner + "))"
}

// session settings prepended to every statement (pooled connections keep whatever was set last):
// lose deadlocks, never wait long for locks, never block writers more than necessary.
const lowImpact = "SET NOCOUNT ON; SET DEADLOCK_PRIORITY LOW; SET LOCK_TIMEOUT 30000; "

// scanPrefix: hashing scans only detect differences (results are re-verified), so without row
// versioning they read uncommitted data instead of taking shared locks on the whole table.
func (s *side) scanPrefix() string {
	switch {
	case s.snapshot:
		return lowImpact + "SET TRANSACTION ISOLATION LEVEL SNAPSHOT; "
	case s.rcsi:
		return lowImpact + "SET TRANSACTION ISOLATION LEVEL READ COMMITTED; "
	}
	return lowImpact + "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED; "
}

// readPrefix: rows that get copied are read committed (row versioned when the database allows it).
func (s *side) readPrefix() string {
	if s.snapshot {
		return lowImpact + "SET TRANSACTION ISOLATION LEVEL SNAPSHOT; "
	}
	return lowImpact + "SET TRANSACTION ISOLATION LEVEL READ COMMITTED; "
}

func (s *side) isolationName() string {
	switch {
	case s.snapshot:
		return "SNAPSHOT"
	case s.rcsi:
		return "READ COMMITTED SNAPSHOT"
	}
	return "no row versioning: scans READ UNCOMMITTED, copies READ COMMITTED"
}

func maxdop() string { return fmt.Sprintf(" OPTION (MAXDOP %d)", scanMaxDop) }

func openSide(ctx context.Context, ep Endpoint) (*side, error) {
	db, err := openDB(ctx, ep, ep.Database)
	if err != nil {
		return nil, err
	}
	s := &side{ep: ep, meta: db, exec: db}
	if err := db.QueryRowContext(ctx, `SELECT compatibility_level, CAST(IIF(snapshot_isolation_state = 1, 1, 0) AS bit),
is_read_committed_snapshot_on, CAST(IIF(CAST(SERVERPROPERTY('ProductMajorVersion') AS int) >= 15, 1, 0) AS bit)
FROM sys.databases WHERE database_id = DB_ID()`).Scan(&s.compat, &s.snapshot, &s.rcsi, &s.utf8); err != nil {
		db.Close()
		return nil, err
	}
	if s.compat < 130 {
		if s.exec, err = openDB(ctx, ep, "tempdb"); err != nil {
			db.Close()
			return nil, err
		}
	}
	return s, nil
}

func (s *side) close() {
	if s.exec != s.meta {
		s.exec.Close()
	}
	s.meta.Close()
}

func (s *side) ref(m *tableMeta) string {
	return qi(s.ep.Database) + "." + qi(m.Schema) + "." + qi(m.Name)
}

// partitionRows: row count from metadata (no scan).
func partitionRows(ctx context.Context, s *side, m *tableMeta) (int64, error) {
	var n int64
	err := s.meta.QueryRowContext(ctx, `SELECT ISNULL(SUM(rows), 0) FROM sys.partitions
WHERE object_id = OBJECT_ID(@t) AND index_id IN (0, 1)`, sql.Named("t", qi(m.Schema)+"."+qi(m.Name))).Scan(&n)
	return n, err
}
