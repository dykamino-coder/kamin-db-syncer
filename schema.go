package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// schemaStep: one metadata extract (folder, one file per object) -> content hash.
// git push and dacpac publish run only when that hash differs from the last successful one,
// so an unchanged schema costs a single extract and no git / publish traffic.
func (rc *runCtx) schemaStep() error {
	c := &rc.job.Config
	st := &rc.job.State
	// fast path: a catalog fingerprint (object modify dates, permissions, principals, roles) is one
	// query; if nothing changed since the last real extract, SqlPackage is not started at all.
	// A real extract still runs every 6 hours to catch changes that do not touch modify_date.
	fp, fpErr := schemaFingerprint(rc.ctx, c.Source)
	if fpErr == nil && fp == st.SchemaFP && recently(st.SchemaFullAt, 6*time.Hour) &&
		(!c.Git.Enabled || st.GitHash != "") && (!c.Schema.Publish || !c.Schema.AlwaysPublish && st.PublishHash != "") {
		targetOK := true
		if c.Schema.Publish {
			id, err := rc.targetIdentity()
			targetOK = err == nil && id == st.PublishedTo
		}
		if targetOK {
			rc.log.Printf("Schema: catalog unchanged since the last extract — SqlPackage not started")
			return nil
		}
	}
	srcConn, err := adoConnString(c.Source)
	if err != nil {
		return err
	}
	snap := filepath.Join(rc.dir, "schema")
	rc.log.Printf("Extracting the schema of %s/%s", c.Source.Server, c.Source.Database)
	if err := rc.runCmd("sqlpackage: ", nil, rc.dir, "sqlpackage", "/Action:Extract",
		"/SourceConnectionString:"+srcConn, "/TargetFile:"+snap,
		"/p:ExtractTarget=SchemaObjectType", "/p:IgnorePermissions=False",
		"/p:IgnoreUserLoginMappings=True", "/p:VerifyExtraction=False"); err != nil {
		return err
	}
	hash, files, err := hashDir(snap)
	if err != nil {
		return err
	}
	rc.log.Printf("Schema: %d objects, hash %s", files, hash[:12])

	if c.Git.Enabled {
		if hash == st.GitHash {
			rc.log.Printf("git: schema unchanged since the last commit, skipping")
		} else {
			changed, err := rc.gitSync(snap)
			if err != nil {
				return fmt.Errorf("git: %w", err)
			}
			st.GitHash = hash
			if changed {
				rc.notes = append(rc.notes, "schema committed to git")
			}
		}
	}

	if c.Schema.Publish {
		// the target's create_date is part of the key: a recreated target database gets published again
		targetID, err := rc.targetIdentity()
		if err != nil {
			return err
		}
		key := sha256Hex(hash + "|" + targetID)
		if key == st.PublishHash && !c.Schema.AlwaysPublish {
			rc.log.Printf("Publish: source schema unchanged, skipping")
			st.PublishedTo = targetID
			rc.markSchemaFP(fp, fpErr)
			return nil
		}
		if err := rc.publish(); err != nil {
			return err
		}
		st.PublishHash, st.PublishedTo = key, targetID
		rc.notes = append(rc.notes, "schema published")
	}
	rc.markSchemaFP(fp, fpErr)
	return nil
}

func (rc *runCtx) targetIdentity() (string, error) {
	t := rc.job.Config.Target
	db, err := openDB(rc.ctx, t, t.Database)
	if err != nil {
		return "", err
	}
	defer db.Close()
	var created time.Time
	if err := db.QueryRowContext(rc.ctx, `SELECT create_date FROM sys.databases WHERE database_id = DB_ID()`).Scan(&created); err != nil {
		return "", err
	}
	return fmt.Sprintf("%s|%s|%s", strings.ToLower(t.Server), strings.ToLower(t.Database), created.Format(time.RFC3339Nano)), nil
}

func (rc *runCtx) publish() error {
	c := &rc.job.Config
	srcConn, err := adoConnString(c.Source)
	if err != nil {
		return err
	}
	tgtConn, err := adoConnString(c.Target)
	if err != nil {
		return err
	}
	dacpac := filepath.Join(rc.dir, "source.dacpac")
	rc.log.Printf("Extracting a dacpac from the source")
	if err := rc.runCmd("sqlpackage: ", nil, rc.dir, "sqlpackage", "/Action:Extract",
		"/SourceConnectionString:"+srcConn, "/TargetFile:"+dacpac,
		"/p:IgnorePermissions=False", "/p:IgnoreUserLoginMappings=True", "/p:VerifyExtraction=False"); err != nil {
		return err
	}
	block := c.Schema.BlockOnPossibleDataLoss == nil || *c.Schema.BlockOnPossibleDataLoss
	exclude := c.Schema.ExcludeObjectTypes
	if exclude == "" {
		exclude = "Logins;Users;RoleMembership"
	}
	rc.log.Printf("Publishing the schema to %s/%s", c.Target.Server, c.Target.Database)
	return rc.runCmd("sqlpackage: ", nil, rc.dir, "sqlpackage", "/Action:Publish",
		"/SourceFile:"+dacpac, "/TargetConnectionString:"+tgtConn,
		"/p:IgnorePermissions=False", "/p:IgnoreRoleMembership=False", "/p:ScriptDatabaseOptions=False",
		"/p:AllowIncompatiblePlatform=True",
		fmt.Sprintf("/p:BlockOnPossibleDataLoss=%t", block),
		fmt.Sprintf("/p:DropObjectsNotInSource=%t", c.Schema.DropObjectsNotInSource),
		"/p:ExcludeObjectTypes="+exclude)
}

// ---------- files ----------

func hashDir(root string) (string, int, error) {
	var paths []string
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			paths = append(paths, p)
		}
		return err
	})
	if err != nil {
		return "", 0, err
	}
	sort.Strings(paths)
	h := sha256.New()
	for _, p := range paths {
		rel, _ := filepath.Rel(root, p)
		fmt.Fprintf(h, "%s\x00", filepath.ToSlash(rel))
		f, err := os.Open(p)
		if err != nil {
			return "", 0, err
		}
		_, err = io.Copy(h, f)
		f.Close()
		if err != nil {
			return "", 0, err
		}
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil)), len(paths), nil
}

func copyDir(src, dst string) error {
	return filepath.WalkDir(src, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(src, p)
		target := filepath.Join(dst, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		in, err := os.Open(p)
		if err != nil {
			return err
		}
		defer in.Close()
		out, err := os.Create(target)
		if err != nil {
			return err
		}
		if _, err := io.Copy(out, in); err != nil {
			out.Close()
			return err
		}
		return out.Close()
	})
}

func sha256Hex(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

func (rc *runCtx) markSchemaFP(fp string, err error) {
	if err == nil {
		rc.job.State.SchemaFP = fp
		rc.job.State.SchemaFullAt = time.Now().UTC().Format(time.RFC3339)
	}
}

// schemaFingerprint hashes the catalog: user objects with their modify_date (tables change it on
// ALTER TABLE, modules on ALTER), permissions, principals, role membership, schemas, user types.
func schemaFingerprint(ctx context.Context, ep Endpoint) (string, error) {
	db, err := openDB(ctx, ep, ep.Database)
	if err != nil {
		return "", err
	}
	defer db.Close()
	var fp string
	err = db.QueryRowContext(ctx, `SELECT CONVERT(varchar(66), HASHBYTES('SHA2_256', (SELECT
  (SELECT o.object_id, o.name, o.schema_id, o.type, o.modify_date, o.parent_object_id FROM sys.objects o
     WHERE o.is_ms_shipped = 0 ORDER BY o.object_id FOR JSON PATH) AS o,
  (SELECT class, major_id, minor_id, grantee_principal_id, type, state FROM sys.database_permissions
     ORDER BY class, major_id, minor_id, grantee_principal_id, type, state FOR JSON PATH) AS p,
  (SELECT principal_id, name, type, modify_date FROM sys.database_principals ORDER BY principal_id FOR JSON PATH) AS u,
  (SELECT role_principal_id, member_principal_id FROM sys.database_role_members ORDER BY 1, 2 FOR JSON PATH) AS r,
  (SELECT schema_id, name, principal_id FROM sys.schemas ORDER BY schema_id FOR JSON PATH) AS s,
  (SELECT user_type_id, name, max_length, is_nullable FROM sys.types WHERE is_user_defined = 1 ORDER BY user_type_id FOR JSON PATH) AS t
  FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)), 2)`).Scan(&fp)
	return fp, err
}
