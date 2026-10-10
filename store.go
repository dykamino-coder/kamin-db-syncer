package main

// MongoDB state: jobs, runs, run logs.
// Safe for several replicas: queueing, claiming and schedule advancing are single atomic document
// operations, and a unique partial index allows at most one active run per job.

import (
	"context"
	"errors"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

const staleAfter = 2 * time.Minute // running run without heartbeat this long -> its pod is gone

type Store struct {
	client                     *mongo.Client
	jobs, runs, logs, counters *mongo.Collection
	cells                      *mongo.Collection // target IBLT cells per job+table (see dataSync.cellCache)
}

func utcNow() time.Time { return time.Now().UTC().Truncate(time.Millisecond) }

func newStore(ctx context.Context, uri, dbName string, retentionDays int) (*Store, error) {
	client, err := mongo.Connect(options.Client().ApplyURI(uri).SetAppName("kamin-db-syncer").
		SetServerSelectionTimeout(10 * time.Second))
	if err != nil {
		return nil, err
	}
	if err := client.Ping(ctx, nil); err != nil {
		return nil, err
	}
	db := client.Database(dbName)
	s := &Store{client: client, jobs: db.Collection("jobs"), runs: db.Collection("runs"),
		logs: db.Collection("run_logs"), counters: db.Collection("counters"), cells: db.Collection("iblt_cells")}

	ttl := int32(retentionDays * 86400)
	idx := []struct {
		c *mongo.Collection
		m mongo.IndexModel
	}{
		{s.jobs, mongo.IndexModel{Keys: bson.D{{Key: "name", Value: 1}}, Options: options.Index().SetUnique(true)}},
		{s.jobs, mongo.IndexModel{Keys: bson.D{{Key: "enabled", Value: 1}, {Key: "next_run_at", Value: 1}}}},
		{s.runs, mongo.IndexModel{Keys: bson.D{{Key: "job_id", Value: 1}, {Key: "_id", Value: -1}}}},
		{s.runs, mongo.IndexModel{Keys: bson.D{{Key: "status", Value: 1}, {Key: "_id", Value: 1}}}},
		{s.runs, mongo.IndexModel{Keys: bson.D{{Key: "job_id", Value: 1}}, Options: options.Index().
			SetName("one_active_run_per_job").SetUnique(true).SetPartialFilterExpression(bson.D{{Key: "active", Value: true}})}},
		// retention is done by MongoDB; active runs have finished_at = null and never expire
		{s.runs, mongo.IndexModel{Keys: bson.D{{Key: "finished_at", Value: 1}}, Options: options.Index().SetExpireAfterSeconds(ttl)}},
		{s.logs, mongo.IndexModel{Keys: bson.D{{Key: "run_id", Value: 1}, {Key: "seq", Value: 1}}, Options: options.Index().SetUnique(true)}},
		{s.logs, mongo.IndexModel{Keys: bson.D{{Key: "ts", Value: 1}}, Options: options.Index().SetExpireAfterSeconds(ttl)}},
	}
	for _, i := range idx {
		if _, err := i.c.Indexes().CreateOne(ctx, i.m); err != nil {
			return nil, err
		}
	}
	return s, s.EnsureSchemaChangeIndexes(ctx)
}

func (s *Store) Ping(ctx context.Context) error { return s.client.Ping(ctx, nil) }

func (s *Store) nextID(ctx context.Context, name string) (int64, error) {
	var doc struct {
		Seq int64 `bson:"seq"`
	}
	err := s.counters.FindOneAndUpdate(ctx, bson.M{"_id": name}, bson.M{"$inc": bson.M{"seq": 1}},
		options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After)).Decode(&doc)
	return doc.Seq, err
}

// ---------- jobs ----------

func (s *Store) ListJobs(ctx context.Context) ([]Job, error) {
	cur, err := s.jobs.Find(ctx, bson.M{}, options.Find().SetSort(bson.D{{Key: "name", Value: 1}}))
	if err != nil {
		return nil, err
	}
	var out []Job
	return out, cur.All(ctx, &out)
}

func (s *Store) GetJob(ctx context.Context, id int64) (*Job, error) {
	return findOne[Job](ctx, s.jobs, bson.M{"_id": id})
}

func (s *Store) GetJobByName(ctx context.Context, name string) (*Job, error) {
	return findOne[Job](ctx, s.jobs, bson.M{"name": name})
}

func findOne[T any](ctx context.Context, c *mongo.Collection, filter any, opts ...options.Lister[options.FindOneOptions]) (*T, error) {
	var v T
	err := c.FindOne(ctx, filter, opts...).Decode(&v)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &v, nil
}

// SaveJob inserts (id == 0) or updates a job definition; state is preserved on update.
func (s *Store) SaveJob(ctx context.Context, j *Job) (int64, error) {
	j.UpdatedAt = utcNow()
	if j.ID == 0 {
		id, err := s.nextID(ctx, "job")
		if err != nil {
			return 0, err
		}
		j.ID = id
		_, err = s.jobs.InsertOne(ctx, j)
		return id, err
	}
	_, err := s.jobs.UpdateOne(ctx, bson.M{"_id": j.ID}, bson.M{"$set": bson.M{
		"name": j.Name, "enabled": j.Enabled, "schedule": j.Schedule, "config": j.Config,
		"source": j.Source, "next_run_at": j.NextRunAt, "updated_at": j.UpdatedAt}})
	return j.ID, err
}

func (s *Store) DeleteJob(ctx context.Context, id int64) error {
	var ids []int64
	cur, err := s.runs.Find(ctx, bson.M{"job_id": id}, options.Find().SetProjection(bson.M{"_id": 1}))
	if err != nil {
		return err
	}
	var rs []Run
	if err := cur.All(ctx, &rs); err != nil {
		return err
	}
	for _, r := range rs {
		ids = append(ids, r.ID)
	}
	if _, err := s.jobs.DeleteOne(ctx, bson.M{"_id": id}); err != nil {
		return err
	}
	if _, err := s.runs.DeleteMany(ctx, bson.M{"job_id": id}); err != nil {
		return err
	}
	if _, err = s.logs.DeleteMany(ctx, bson.M{"run_id": bson.M{"$in": ids}}); err != nil {
		return err
	}
	return s.DeleteSchemaChanges(ctx, id)
}

func (s *Store) SetApproval(ctx context.Context, id int64, approved, created bool, by string) error {
	set := bson.M{"targetApproved": approved, "targetCreated": created, "approvedBy": by, "approvedAt": nil}
	if approved {
		set["approvedAt"] = utcNow()
	}
	_, err := s.jobs.UpdateOne(ctx, bson.M{"_id": id}, bson.M{"$set": set})
	return err
}

func (s *Store) SetNextRun(ctx context.Context, id int64, t *time.Time) error {
	_, err := s.jobs.UpdateOne(ctx, bson.M{"_id": id}, bson.M{"$set": bson.M{"next_run_at": t}})
	return err
}

// AdvanceNextRun is a compare-and-set: only one replica wins a given schedule occurrence.
func (s *Store) AdvanceNextRun(ctx context.Context, id int64, expected time.Time, next *time.Time) (bool, error) {
	r, err := s.jobs.UpdateOne(ctx, bson.M{"_id": id, "next_run_at": expected},
		bson.M{"$set": bson.M{"next_run_at": next}})
	return err == nil && r.ModifiedCount == 1, err
}

func (s *Store) DueJobs(ctx context.Context, at time.Time) ([]Job, error) {
	cur, err := s.jobs.Find(ctx, bson.M{"enabled": true, "next_run_at": bson.M{"$ne": nil, "$lte": at}})
	if err != nil {
		return nil, err
	}
	var out []Job
	return out, cur.All(ctx, &out)
}

func (s *Store) SaveJobState(ctx context.Context, id int64, st JobState) error {
	_, err := s.jobs.UpdateOne(ctx, bson.M{"_id": id}, bson.M{"$set": bson.M{"state": st}})
	return err
}

// ---------- runs ----------

// Enqueue returns the new run id, or 0 if the job already has a queued/running run.
func (s *Store) Enqueue(ctx context.Context, job *Job, trigger string) (int64, error) {
	id, err := s.nextID(ctx, "run")
	if err != nil {
		return 0, err
	}
	active := true
	_, err = s.runs.InsertOne(ctx, Run{ID: id, JobID: job.ID, JobName: job.Name, Status: "queued",
		Active: &active, TriggerType: trigger, QueuedAt: utcNow()})
	if mongo.IsDuplicateKeyError(err) {
		return 0, nil
	}
	return id, err
}

func (s *Store) Claim(ctx context.Context, worker string) (*Run, error) {
	now := utcNow()
	var r Run
	err := s.runs.FindOneAndUpdate(ctx, bson.M{"status": "queued"},
		bson.M{"$set": bson.M{"status": "running", "started_at": now, "heartbeat_at": now, "worker": worker}},
		options.FindOneAndUpdate().SetSort(bson.D{{Key: "_id", Value: 1}}).SetReturnDocument(options.After)).Decode(&r)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	return &r, err
}

// Heartbeat returns true if cancellation was requested.
func (s *Store) Heartbeat(ctx context.Context, id int64, stats RunStats, tables []TableResult, p *Progress) (bool, error) {
	var r Run
	err := s.runs.FindOneAndUpdate(ctx, bson.M{"_id": id},
		bson.M{"$set": bson.M{"heartbeat_at": utcNow(), "stats": stats, "tables": tables, "progress": p}},
		options.FindOneAndUpdate().SetProjection(bson.M{"cancel_requested": 1})).Decode(&r)
	return r.CancelRequested, err
}

func (s *Store) FailStale(ctx context.Context) (int64, error) {
	r, err := s.runs.UpdateMany(ctx,
		bson.M{"status": "running", "heartbeat_at": bson.M{"$lt": utcNow().Add(-staleAfter)}},
		bson.M{"$set": bson.M{"status": "failed", "finished_at": utcNow(),
			"summary": "the pod running this run disappeared (no heartbeat)"}, "$unset": bson.M{"active": ""}})
	if err != nil {
		return 0, err
	}
	return r.ModifiedCount, nil
}

func (s *Store) ListRuns(ctx context.Context, jobID int64, limit int64) ([]Run, error) {
	filter := bson.M{}
	if jobID > 0 {
		filter["job_id"] = jobID
	}
	cur, err := s.runs.Find(ctx, filter, options.Find().SetSort(bson.D{{Key: "_id", Value: -1}}).SetLimit(limit).
		SetProjection(bson.M{"tables": 0}))
	if err != nil {
		return nil, err
	}
	out := []Run{}
	return out, cur.All(ctx, &out)
}

func (s *Store) LastRun(ctx context.Context, jobID int64) (*Run, error) {
	return findOne[Run](ctx, s.runs, bson.M{"job_id": jobID}, options.FindOne().SetSort(bson.D{{Key: "_id", Value: -1}}))
}

func (s *Store) HasActiveRun(ctx context.Context, jobID int64) (bool, error) {
	n, err := s.runs.CountDocuments(ctx, bson.M{"job_id": jobID, "active": true})
	return n > 0, err
}

func (s *Store) GetRun(ctx context.Context, id int64) (*Run, error) {
	return findOne[Run](ctx, s.runs, bson.M{"_id": id})
}

func (s *Store) GetLogs(ctx context.Context, runID int64, after int) ([]LogLine, error) {
	cur, err := s.logs.Find(ctx, bson.M{"run_id": runID, "seq": bson.M{"$gt": after}},
		options.Find().SetSort(bson.D{{Key: "seq", Value: 1}}).SetLimit(5000))
	if err != nil {
		return nil, err
	}
	out := []LogLine{}
	return out, cur.All(ctx, &out)
}

func (s *Store) AppendLogs(ctx context.Context, runID int64, startSeq int, lines []logEntry) error {
	docs := make([]any, len(lines))
	for i, l := range lines {
		line := l.line
		if len(line) > 4000 {
			line = line[:4000]
		}
		docs[i] = bson.M{"run_id": runID, "seq": startSeq + i, "ts": l.at, "line": line}
	}
	_, err := s.logs.InsertMany(ctx, docs, options.InsertMany().SetOrdered(false))
	return err
}

func (s *Store) RequestCancel(ctx context.Context, id int64) error {
	if _, err := s.runs.UpdateOne(ctx, bson.M{"_id": id, "status": "queued"}, bson.M{
		"$set":   bson.M{"status": "cancelled", "finished_at": utcNow(), "summary": "cancelled before start"},
		"$unset": bson.M{"active": ""}}); err != nil {
		return err
	}
	_, err := s.runs.UpdateOne(ctx, bson.M{"_id": id, "status": "running"}, bson.M{"$set": bson.M{"cancel_requested": true}})
	return err
}

func (s *Store) Finish(ctx context.Context, id int64, status, summary string, stats RunStats, tables []TableResult) error {
	_, err := s.runs.UpdateOne(ctx, bson.M{"_id": id}, bson.M{
		"$set":   bson.M{"status": status, "summary": summary, "stats": stats, "tables": tables, "finished_at": utcNow()},
		"$unset": bson.M{"active": "", "progress": ""}})
	return err
}

func (s *Store) Stats(ctx context.Context) (map[string]int64, error) {
	dayAgo := utcNow().Add(-24 * time.Hour)
	q := map[string]bson.M{
		"running":       {"status": "running"},
		"queued":        {"status": "queued"},
		"failed_24h":    {"status": "failed", "queued_at": bson.M{"$gte": dayAgo}},
		"succeeded_24h": {"status": "succeeded", "queued_at": bson.M{"$gte": dayAgo}},
	}
	out := map[string]int64{}
	for k, f := range q {
		n, err := s.runs.CountDocuments(ctx, f)
		if err != nil {
			return nil, err
		}
		out[k] = n
	}
	return out, nil
}

// ---------- target IBLT cell cache ----------

func (s *Store) LoadCells(ctx context.Context, key string) ([]byte, error) {
	var doc struct {
		Cells []byte `bson:"cells"`
	}
	err := s.cells.FindOne(ctx, bson.M{"_id": key}).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	return doc.Cells, err
}

func (s *Store) SaveCells(ctx context.Context, key string, cells []byte) error {
	_, err := s.cells.ReplaceOne(ctx, bson.M{"_id": key}, bson.M{"_id": key, "cells": cells}, options.Replace().SetUpsert(true))
	return err
}
