package main

// Discovery for the setup wizard: databases on a server, tables of a database (with row counts and
// key kind, so people pick tables instead of typing names) and target status before a job exists.

import (
	"context"
	"encoding/json"
	"net/http"
	"time"
)

type tableInfo struct {
	Schema string  `json:"schema"`
	Name   string  `json:"name"`
	Rows   int64   `json:"rows"`
	Cols   int     `json:"cols"`
	Key    string  `json:"key"` // int | guid | composite | other | none
	SizeMB float64 `json:"size_mb"`
}

func listDatabases(ctx context.Context, ep Endpoint) ([]string, error) {
	db, err := openDB(ctx, ep, "master")
	if err != nil {
		return nil, err
	}
	defer db.Close()
	rows, err := db.QueryContext(ctx, `SELECT name FROM sys.databases
WHERE database_id > 4 AND state = 0 AND HAS_DBACCESS(name) = 1 ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []string{}
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

func listTables(ctx context.Context, ep Endpoint) ([]tableInfo, error) {
	db, err := openDB(ctx, ep, ep.Database)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	rows, err := db.QueryContext(ctx, `
SELECT s.name, t.name,
       ISNULL((SELECT SUM(p.rows) FROM sys.partitions p WHERE p.object_id = t.object_id AND p.index_id IN (0, 1)), 0),
       (SELECT COUNT(*) FROM sys.columns c WHERE c.object_id = t.object_id),
       CASE
         WHEN NOT EXISTS (SELECT 1 FROM sys.indexes i WHERE i.object_id = t.object_id AND i.is_primary_key = 1) THEN 'none'
         WHEN (SELECT COUNT(*) FROM sys.indexes i JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
               WHERE i.object_id = t.object_id AND i.is_primary_key = 1) > 1 THEN 'composite'
         ELSE (SELECT CASE WHEN ty.name IN ('int', 'bigint', 'smallint', 'tinyint') THEN 'int'
                           WHEN ty.name = 'uniqueidentifier' THEN 'guid' ELSE 'other' END
               FROM sys.indexes i
               JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
               JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
               JOIN sys.types ty ON ty.user_type_id = c.user_type_id
               WHERE i.object_id = t.object_id AND i.is_primary_key = 1)
       END,
       CAST(ISNULL((SELECT SUM(a.total_pages) FROM sys.partitions p JOIN sys.allocation_units a ON a.container_id = p.partition_id
                    WHERE p.object_id = t.object_id), 0) * 8 / 1024.0 AS float)
FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id
WHERE t.is_ms_shipped = 0 AND t.name <> N'sysdiagrams'
ORDER BY s.name, t.name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []tableInfo{}
	for rows.Next() {
		var t tableInfo
		if err := rows.Scan(&t.Schema, &t.Name, &t.Rows, &t.Cols, &t.Key, &t.SizeMB); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func (a *API) discoverDatabases(w http.ResponseWriter, r *http.Request) {
	var ep Endpoint
	if err := json.NewDecoder(r.Body).Decode(&ep); err != nil {
		fail(w, 400, err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	dbs, err := listDatabases(ctx, ep)
	if err != nil {
		writeJSON(w, 200, map[string]any{"ok": false, "message": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "databases": dbs})
}

func (a *API) discoverTables(w http.ResponseWriter, r *http.Request) {
	var ep Endpoint
	if err := json.NewDecoder(r.Body).Decode(&ep); err != nil {
		fail(w, 400, err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()
	ts, err := listTables(ctx, ep)
	if err != nil {
		writeJSON(w, 200, map[string]any{"ok": false, "message": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "tables": ts})
}

// discoverTarget: target status for a job that is not saved yet (wizard).
func (a *API) discoverTarget(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Source Endpoint `json:"source"`
		Target Endpoint `json:"target"`
	}
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		fail(w, 400, err.Error())
		return
	}
	j := &Job{Config: JobConfig{Source: in.Source, Target: in.Target}}
	writeJSON(w, 200, targetStatus(r.Context(), j))
}
