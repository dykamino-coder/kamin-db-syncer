package main

// Schema change history: every git commit of a job's schema snapshot is also recorded in MongoDB
// with the list of changed objects (added / modified / deleted / renamed) and the patch, so the UI
// can show what changed in which run without a git client.

import (
	"context"
	"os"
	"os/exec"
	"path"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

const maxPatchBytes = 1 << 20

type ObjChange struct {
	Status  string `json:"status" bson:"status"` // A M D R
	Path    string `json:"path" bson:"path"`     // relative to the job folder
	OldPath string `json:"old_path,omitempty" bson:"old_path,omitempty"`
	Kind    string `json:"kind" bson:"kind"` // Tables, Views, StoredProcedures, Functions, Security, ...
	Object  string `json:"object" bson:"object"`
}

type SchemaChange struct {
	ID             int64       `json:"id" bson:"_id"`
	JobID          int64       `json:"job_id" bson:"job_id"`
	JobName        string      `json:"job_name" bson:"job_name"`
	RunID          int64       `json:"run_id" bson:"run_id"`
	At             time.Time   `json:"at" bson:"at"`
	Commit         string      `json:"commit" bson:"commit"`
	Baseline       bool        `json:"baseline" bson:"baseline"` // first snapshot of the job
	Added          int         `json:"added" bson:"added"`
	Modified       int         `json:"modified" bson:"modified"`
	Deleted        int         `json:"deleted" bson:"deleted"`
	Changes        []ObjChange `json:"changes,omitempty" bson:"changes"`
	Patch          string      `json:"patch,omitempty" bson:"patch,omitempty"`
	PatchTruncated bool        `json:"patch_truncated,omitempty" bson:"patch_truncated,omitempty"`
}

func (s *Store) schemaChanges() *mongo.Collection {
	return s.client.Database(s.jobs.Database().Name()).Collection("schema_changes")
}

func (s *Store) EnsureSchemaChangeIndexes(ctx context.Context) error {
	_, err := s.schemaChanges().Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: "job_id", Value: 1}, {Key: "_id", Value: -1}}})
	return err
}

func (s *Store) SaveSchemaChange(ctx context.Context, c *SchemaChange) error {
	id, err := s.nextID(ctx, "schema_change")
	if err != nil {
		return err
	}
	c.ID = id
	_, err = s.schemaChanges().InsertOne(ctx, c)
	return err
}

// ListSchemaChanges returns history without patches (and without the object list for huge baselines).
func (s *Store) ListSchemaChanges(ctx context.Context, jobID, limit int64) ([]SchemaChange, error) {
	filter := bson.M{}
	if jobID > 0 {
		filter["job_id"] = jobID
	}
	cur, err := s.schemaChanges().Find(ctx, filter, options.Find().SetSort(bson.D{{Key: "_id", Value: -1}}).
		SetLimit(limit).SetProjection(bson.M{"patch": 0}))
	if err != nil {
		return nil, err
	}
	out := []SchemaChange{}
	if err := cur.All(ctx, &out); err != nil {
		return nil, err
	}
	for i := range out {
		if out[i].Baseline && len(out[i].Changes) > 200 {
			out[i].Changes = out[i].Changes[:200]
		}
	}
	return out, nil
}

func (s *Store) GetSchemaChange(ctx context.Context, id int64) (*SchemaChange, error) {
	return findOne[SchemaChange](ctx, s.schemaChanges(), bson.M{"_id": id})
}

func (s *Store) DeleteSchemaChanges(ctx context.Context, jobID int64) error {
	_, err := s.schemaChanges().DeleteMany(ctx, bson.M{"job_id": jobID})
	return err
}

// ---------- building the record from the commit just pushed ----------

func gitOutput(ctx context.Context, env []string, dir string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), env...)
	out, err := cmd.Output()
	return string(out), err
}

// describeCommit parses `git diff --name-status` of HEAD against its parent, limited to sub.
func describeCommit(ctx context.Context, env []string, repo, sub string, baseline bool) (*SchemaChange, error) {
	sha, err := gitOutput(ctx, env, repo, "rev-parse", "HEAD")
	if err != nil {
		return nil, err
	}
	ns, err := gitOutput(ctx, env, repo, "diff", "--name-status", "-M", "HEAD~1", "HEAD", "--", sub)
	if err != nil {
		return nil, err
	}
	c := &SchemaChange{Commit: strings.TrimSpace(sha), Baseline: baseline, At: utcNow()}
	for _, line := range strings.Split(strings.TrimSpace(ns), "\n") {
		f := strings.Split(line, "\t")
		if len(f) < 2 {
			continue
		}
		oc := ObjChange{Status: f[0][:1], Path: strings.TrimPrefix(f[len(f)-1], sub+"/")}
		if oc.Status == "R" && len(f) == 3 {
			oc.OldPath = strings.TrimPrefix(f[1], sub+"/")
		}
		oc.Kind, oc.Object = objectOf(oc.Path)
		switch oc.Status {
		case "A":
			c.Added++
		case "D":
			c.Deleted++
		default:
			c.Modified++
		}
		c.Changes = append(c.Changes, oc)
	}
	if !baseline {
		patch, err := gitOutput(ctx, env, repo, "diff", "-M", "HEAD~1", "HEAD", "--", sub)
		if err != nil {
			return nil, err
		}
		if len(patch) > maxPatchBytes {
			patch, c.PatchTruncated = patch[:maxPatchBytes], true
		}
		c.Patch = patch
	}
	return c, nil
}

// objectOf maps SqlPackage's SchemaObjectType layout to (kind, object):
// dbo/Tables/Orders.sql -> (Tables, dbo.Orders); Security/app_reader.sql -> (Security, app_reader)
func objectOf(p string) (string, string) {
	parts := strings.Split(p, "/")
	name := strings.TrimSuffix(parts[len(parts)-1], path.Ext(parts[len(parts)-1]))
	switch len(parts) {
	case 1:
		return "Other", name
	case 2:
		return parts[0], name
	}
	return parts[len(parts)-2], parts[0] + "." + name
}
