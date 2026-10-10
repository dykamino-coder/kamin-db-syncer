package main

// Target database lifecycle: a job that writes to a target (schema publish or data) runs only after
// the target was approved in the UI — either created by us (empty, safe) or explicitly confirmed by a
// person when it already existed (its data and schema in the synced scope will be overwritten).

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type TargetStatus struct {
	Server     string     `json:"server"`
	Database   string     `json:"database"`
	Reachable  bool       `json:"reachable"`
	Exists     bool       `json:"exists"`
	Error      string     `json:"error,omitempty"`
	Collation  string     `json:"collation,omitempty"`
	Tables     int        `json:"tables"`
	SizeMB     float64    `json:"size_mb"`
	Approved   bool       `json:"approved"`
	ApprovedAt *time.Time `json:"approved_at,omitempty"`
	ApprovedBy string     `json:"approved_by,omitempty"`
	Created    bool       `json:"created_by_sync"`
	SrcColl    string     `json:"source_collation,omitempty"`
}

func needsTarget(c *JobConfig) bool { return c.Schema.Publish || len(c.Data.Tables) > 0 }

func targetStatus(ctx context.Context, j *Job) TargetStatus {
	t := j.Config.Target
	st := TargetStatus{Server: t.Server, Database: t.Database, Approved: j.TargetApproved,
		ApprovedAt: j.ApprovedAt, ApprovedBy: j.ApprovedBy, Created: j.TargetCreated}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	db, err := openDB(ctx, t, "master")
	if err != nil {
		st.Error = err.Error()
		return st
	}
	defer db.Close()
	st.Reachable = true
	var coll sql.NullString
	if err := db.QueryRowContext(ctx, `SELECT collation_name FROM sys.databases WHERE name = @db`,
		sql.Named("db", t.Database)).Scan(&coll); err == sql.ErrNoRows {
		st.SrcColl, _ = sourceCollation(ctx, j.Config.Source)
		return st
	} else if err != nil {
		st.Error = err.Error()
		return st
	}
	st.Exists, st.Collation = true, coll.String
	tdb, err := openDB(ctx, t, t.Database)
	if err != nil {
		st.Error = err.Error()
		return st
	}
	defer tdb.Close()
	_ = tdb.QueryRowContext(ctx, `SELECT (SELECT COUNT(*) FROM sys.tables WHERE is_ms_shipped = 0),
		CAST(ISNULL((SELECT SUM(size) FROM sys.database_files WHERE type = 0), 0) * 8 / 1024.0 AS float)`).Scan(&st.Tables, &st.SizeMB)
	return st
}

func sourceCollation(ctx context.Context, src Endpoint) (string, error) {
	db, err := openDB(ctx, src, "master")
	if err != nil {
		return "", err
	}
	defer db.Close()
	var c string
	err = db.QueryRowContext(ctx, `SELECT collation_name FROM sys.databases WHERE name = @db`, sql.Named("db", src.Database)).Scan(&c)
	return c, err
}

// createTargetDB creates the target database with the source database's collation.
func createTargetDB(ctx context.Context, j *Job) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	coll, err := sourceCollation(ctx, j.Config.Source)
	if err != nil {
		return "", fmt.Errorf("could not read the source collation: %w", err)
	}
	t := j.Config.Target
	db, err := openDB(ctx, t, "master")
	if err != nil {
		return "", err
	}
	defer db.Close()
	var exists int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sys.databases WHERE name = @db`, sql.Named("db", t.Database)).Scan(&exists); err != nil {
		return "", err
	}
	if exists > 0 {
		return "", fmt.Errorf("database %s already exists on %s — approve it instead of creating it", t.Database, t.Server)
	}
	if _, err := db.ExecContext(ctx, "CREATE DATABASE "+qi(t.Database)+" COLLATE "+coll); err != nil {
		return "", fmt.Errorf("CREATE DATABASE: %w (the target login needs the dbcreator role)", err)
	}
	return fmt.Sprintf("database %s created on %s (collation %s)", t.Database, t.Server, coll), nil
}
