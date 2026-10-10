package main

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	_ "github.com/microsoft/go-mssqldb"
)

func trustServerCert() bool { return !strings.EqualFold(os.Getenv("TRUST_SERVER_CERT"), "false") }

// splitServer accepts "host", "host,port" and "host\instance".
func splitServer(server string) (host, port, instance string) {
	host = strings.TrimSpace(server)
	if h, p, ok := strings.Cut(host, ","); ok {
		host, port = strings.TrimSpace(h), strings.TrimSpace(p)
	}
	if h, i, ok := strings.Cut(host, `\`); ok {
		host, instance = h, i
	}
	return
}

func endpointCreds(ep Endpoint) (string, string, error) {
	user, pass, ok := credential(ep.Credential)
	if !ok {
		k := credentialKey(ep.Credential)
		return "", "", fmt.Errorf("no credential %q: set CRED_%s_USER / CRED_%s_PASSWORD", ep.Credential, k, k)
	}
	return user, pass, nil
}

// goDSN builds a go-mssqldb URL connection string for the given database.
func goDSN(ep Endpoint, database string) (string, error) {
	user, pass, err := endpointCreds(ep)
	if err != nil {
		return "", err
	}
	host, port, inst := splitServer(ep.Server)
	if port != "" {
		host += ":" + port
	}
	q := url.Values{}
	q.Set("database", database)
	q.Set("encrypt", "true")
	q.Set("TrustServerCertificate", fmt.Sprint(trustServerCert()))
	q.Set("app name", "kamin-db-syncer")
	q.Set("dial timeout", "15")
	u := &url.URL{Scheme: "sqlserver", User: url.UserPassword(user, pass), Host: host, RawQuery: q.Encode()}
	if inst != "" {
		u.Path = "/" + inst
	}
	return u.String(), nil
}

// adoConnString builds an ADO.NET connection string (for SqlPackage).
func adoConnString(ep Endpoint) (string, error) {
	user, pass, err := endpointCreds(ep)
	if err != nil {
		return "", err
	}
	quote := func(v string) string {
		if strings.ContainsAny(v, `;'"= `) {
			return `"` + strings.ReplaceAll(v, `"`, `""`) + `"`
		}
		return v
	}
	return fmt.Sprintf("Server=%s;Database=%s;User ID=%s;Password=%s;Encrypt=True;TrustServerCertificate=%t;Application Name=kamin-db-syncer",
		quote(ep.Server), quote(ep.Database), quote(user), quote(pass), trustServerCert()), nil
}

func openDB(ctx context.Context, ep Endpoint, database string) (*sql.DB, error) {
	dsn, err := goDSN(ep, database)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlserver", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(8)
	db.SetConnMaxIdleTime(time.Minute)
	pctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	if err := db.PingContext(pctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("%s/%s: %w", ep.Server, database, err)
	}
	return db, nil
}

func testConnection(ctx context.Context, ep Endpoint) (string, error) {
	db, err := openDB(ctx, ep, "master")
	if err != nil {
		return "", err
	}
	defer db.Close()
	var version string
	var dbID sql.NullInt64
	var access sql.NullInt64
	err = db.QueryRowContext(ctx, `SELECT CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(50)) + N' ' +
		CAST(SERVERPROPERTY('Edition') AS nvarchar(100)), DB_ID(@db), HAS_DBACCESS(@db)`,
		sql.Named("db", ep.Database)).Scan(&version, &dbID, &access)
	if err != nil {
		return "", err
	}
	if !dbID.Valid {
		return "", fmt.Errorf("SQL Server %s: database %q not found", version, ep.Database)
	}
	if access.Int64 != 1 {
		return "", fmt.Errorf("SQL Server %s: no access to database %q", version, ep.Database)
	}
	return fmt.Sprintf("SQL Server %s, database %s is reachable", version, ep.Database), nil
}

func qi(name string) string { return "[" + strings.ReplaceAll(name, "]", "]]") + "]" }
