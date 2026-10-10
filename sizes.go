package main

// Database and table sizes from metadata (no table reads) for the sync overview.

import (
	"context"
	"net/http"
	"time"
)

type tableSize struct {
	Rows   int64   `json:"rows"`
	SizeMB float64 `json:"size_mb"` // data + indexes
}

type dbSize struct {
	Error   string               `json:"error,omitempty"`
	Tables  int                  `json:"tables"`
	Rows    int64                `json:"rows"`
	DataMB  float64              `json:"data_mb"`  // used by tables and indexes
	FilesMB float64              `json:"files_mb"` // allocated data files
	PerTbl  map[string]tableSize `json:"per_table"`
}

func databaseSize(ctx context.Context, ep Endpoint) dbSize {
	out := dbSize{PerTbl: map[string]tableSize{}}
	db, err := openDB(ctx, ep, ep.Database)
	if err != nil {
		out.Error = err.Error()
		return out
	}
	defer db.Close()
	rows, err := db.QueryContext(ctx, `
SELECT s.name + N'.' + t.name,
       ISNULL(SUM(CASE WHEN p.index_id IN (0, 1) THEN p.rows END), 0),
       CAST(ISNULL((SELECT SUM(a.total_pages) FROM sys.partitions p2 JOIN sys.allocation_units a ON a.container_id = p2.partition_id
                    WHERE p2.object_id = t.object_id), 0) * 8 / 1024.0 AS float)
FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id
LEFT JOIN sys.partitions p ON p.object_id = t.object_id
WHERE t.is_ms_shipped = 0 AND t.name <> N'sysdiagrams'
GROUP BY s.name, t.name, t.object_id`)
	if err != nil {
		out.Error = err.Error()
		return out
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		var ts tableSize
		if err := rows.Scan(&name, &ts.Rows, &ts.SizeMB); err != nil {
			out.Error = err.Error()
			return out
		}
		out.PerTbl[name] = ts
		out.Tables++
		out.Rows += ts.Rows
		out.DataMB += ts.SizeMB
	}
	_ = db.QueryRowContext(ctx, `SELECT CAST(ISNULL(SUM(CAST(size AS bigint)), 0) * 8 / 1024.0 AS float) FROM sys.database_files WHERE type = 0`).Scan(&out.FilesMB)
	return out
}

func (a *API) jobSizes(w http.ResponseWriter, r *http.Request) {
	j := a.jobOr404(w, r)
	if j == nil {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()
	res := map[string]dbSize{}
	src, tgt := make(chan dbSize, 1), make(chan dbSize, 1)
	go func() { src <- databaseSize(ctx, j.Config.Source) }()
	if needsTarget(&j.Config) {
		go func() { tgt <- databaseSize(ctx, j.Config.Target) }()
		res["target"] = <-tgt
	}
	res["source"] = <-src
	writeJSON(w, 200, res)
}
