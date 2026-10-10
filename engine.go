package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	cron "github.com/netresearch/go-cron"
)

// ---------- schedule helpers ----------

func parseCron(expr string) (cron.Schedule, error) { return cron.ParseStandard(expr) }

// manualOnly in a job's schedule means "no schedule", even when SYNC_SCHEDULE is set.
const manualOnly = "-"

func (e *Engine) effectiveSchedule(jobSchedule string) string {
	s := strings.TrimSpace(jobSchedule)
	if s == manualOnly {
		return ""
	}
	if s != "" {
		return s
	}
	return e.cfg.DefaultSchedule
}

func (e *Engine) nextRuns(expr string, n int, from time.Time) []time.Time {
	sched, err := parseCron(expr)
	if expr == "" || err != nil {
		return nil
	}
	var out []time.Time
	t := from.In(e.cfg.TZ)
	for range n {
		t = sched.Next(t)
		out = append(out, t.UTC().Truncate(time.Millisecond))
	}
	return out
}

func (e *Engine) nextRunFor(enabled bool, jobSchedule string) *time.Time {
	if !enabled {
		return nil
	}
	if r := e.nextRuns(e.effectiveSchedule(jobSchedule), 1, time.Now()); len(r) > 0 {
		return &r[0]
	}
	return nil
}

// ---------- engine ----------

type Engine struct {
	cfg   *Settings
	store *Store
	res   *Resources
	wg    sync.WaitGroup
}

func newEngine(cfg *Settings, st *Store, res *Resources) *Engine {
	return &Engine{cfg: cfg, store: st, res: res}
}

// envJob is the SYNC_CONFIG / SYNC_CONFIG_FILE format: {"jobs":[{name, enabled, schedule, source, target, ...}]}
type envJob struct {
	Name     string `json:"name"`
	Enabled  *bool  `json:"enabled,omitempty"`
	Schedule string `json:"schedule,omitempty"`
	// pre-approve writing to an existing target (otherwise approve it in the UI)
	TargetApproved bool `json:"targetApproved,omitempty"`
	JobConfig
}

// SeedFromEnv: SYNC_CONFIG_MODE=seed creates env jobs missing in MongoDB (UI edits are kept);
// enforce overwrites them from env on every start.
func (e *Engine) SeedFromEnv(ctx context.Context) error {
	raw := os.Getenv("SYNC_CONFIG")
	if raw == "" {
		if p := os.Getenv("SYNC_CONFIG_FILE"); p != "" {
			b, err := os.ReadFile(p)
			if err != nil && !os.IsNotExist(err) {
				return err
			}
			raw = string(b)
		}
	}
	if raw == "" {
		return nil
	}
	var doc struct {
		Jobs []envJob `json:"jobs"`
	}
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		return fmt.Errorf("SYNC_CONFIG: %w", err)
	}
	for _, ej := range doc.Jobs {
		enabled := ej.Enabled == nil || *ej.Enabled
		if errs := validateJob(ej.Name, ej.Schedule, &ej.JobConfig); len(errs) > 0 {
			slog.Error("env job skipped", "job", ej.Name, "errors", strings.Join(errs, "; "))
			continue
		}
		existing, err := e.store.GetJobByName(ctx, ej.Name)
		if err != nil {
			return err
		}
		if existing != nil && e.cfg.ConfigMode != "enforce" {
			continue
		}
		j := &Job{Name: ej.Name, Enabled: enabled, Schedule: ej.Schedule, Config: ej.JobConfig, Source: "env",
			NextRunAt: e.nextRunFor(enabled, ej.Schedule), TargetApproved: ej.TargetApproved}
		if ej.TargetApproved {
			now := utcNow()
			j.ApprovedAt, j.ApprovedBy = &now, "SYNC_CONFIG"
		}
		if existing != nil {
			j.ID = existing.ID
		}
		if _, err := e.store.SaveJob(ctx, j); err != nil {
			return err
		}
		slog.Info("env job applied", "job", ej.Name, "updated", existing != nil)
	}
	return nil
}

// RefreshNextRuns recomputes schedules (SYNC_SCHEDULE / timezone may have changed). Overdue
// occurrences are left alone: another replica's scheduler may be about to run them.
func (e *Engine) RefreshNextRuns(ctx context.Context) error {
	jobs, err := e.store.ListJobs(ctx)
	if err != nil {
		return err
	}
	now := time.Now()
	for _, j := range jobs {
		if j.Enabled && j.NextRunAt != nil && !j.NextRunAt.After(now) {
			continue
		}
		if err := e.store.SetNextRun(ctx, j.ID, e.nextRunFor(j.Enabled, j.Schedule)); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) Start(ctx context.Context) {
	e.wg.Add(1 + e.cfg.MaxParallel)
	go e.scheduler(ctx)
	for i := range e.cfg.MaxParallel {
		go e.worker(ctx, fmt.Sprintf("%s/%d", e.cfg.Pod, i))
	}
	if e.cfg.RunOnStart {
		jobs, _ := e.store.ListJobs(ctx)
		for i := range jobs {
			if jobs[i].Enabled {
				_, _ = e.store.Enqueue(ctx, &jobs[i], "startup")
			}
		}
	}
}

func (e *Engine) Wait() { e.wg.Wait() }

func (e *Engine) scheduler(ctx context.Context) {
	defer e.wg.Done()
	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()
	for {
		if n, err := e.store.FailStale(ctx); err != nil {
			slog.Error("fail stale", "err", err)
		} else if n > 0 {
			slog.Warn("stale runs marked failed", "count", n)
		}
		due, err := e.store.DueJobs(ctx, time.Now())
		if err != nil && ctx.Err() == nil {
			slog.Error("due jobs", "err", err)
		}
		for i := range due {
			j := &due[i]
			won, err := e.store.AdvanceNextRun(ctx, j.ID, *j.NextRunAt, e.nextRunFor(true, j.Schedule))
			if err != nil || !won {
				continue
			}
			if needsTarget(&j.Config) && !j.TargetApproved {
				slog.Info("job waits for target approval, occurrence skipped", "job", j.Name)
				continue
			}
			if id, err := e.store.Enqueue(ctx, j, "schedule"); err == nil && id == 0 {
				slog.Warn("previous run still active, occurrence skipped", "job", j.Name)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

func (e *Engine) worker(ctx context.Context, id string) {
	defer e.wg.Done()
	for ctx.Err() == nil {
		run, err := e.store.Claim(ctx, id)
		if err != nil && ctx.Err() == nil {
			slog.Error("claim", "err", err)
		}
		if run != nil {
			e.execute(ctx, run)
			continue
		}
		select {
		case <-ctx.Done():
		case <-time.After(3 * time.Second):
		}
	}
}

// ---------- run log ----------

type logEntry struct {
	at   time.Time
	line string
}

type runLog struct {
	mu  sync.Mutex
	buf []logEntry
}

// Printf records lines with their own timestamp (UTC); the UI shows it in the viewer's time zone.
func (l *runLog) Printf(format string, a ...any) {
	at := time.Now().UTC()
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, line := range strings.Split(strings.TrimRight(fmt.Sprintf(format, a...), "\n"), "\n") {
		l.buf = append(l.buf, logEntry{at, line})
	}
}

func (l *runLog) take() []logEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := l.buf
	l.buf = nil
	return out
}

// stats are updated from the data sync and read by the heartbeat loop.
type liveStats struct{ read, written, rows, tables, reloads atomic.Int64 }

func (s *liveStats) snapshot() RunStats {
	return RunStats{BytesRead: s.read.Load(), BytesWritten: s.written.Load(),
		RowsChanged: s.rows.Load(), TablesChanged: int(s.tables.Load()), TablesReloaded: int(s.reloads.Load())}
}

// ---------- execution ----------

type runCtx struct {
	stateMu  sync.Mutex // job.State is written by parallel table workers
	resMu    sync.Mutex
	tables   []TableResult
	progress *Progress

	saveState        func() // persists job state mid-run (long runs, resumable reloads)
	saveSchemaChange func(*SchemaChange) error
	loadCells        func(table string) ([]byte, error) // target IBLT cell cache (per job + table)
	saveCells        func(table string, cells []byte) error
	runID            int64
	ctx              context.Context
	cfg              *Settings
	job              *Job
	dir              string
	log              *runLog
	stats            *liveStats
	notes            []string // short facts for the run summary
}

func (e *Engine) execute(parent context.Context, run *Run) {
	bg := context.WithoutCancel(parent) // bookkeeping must survive shutdown
	job, err := e.store.GetJob(bg, run.JobID)
	if err != nil || job == nil {
		_ = e.store.Finish(bg, run.ID, "failed", "sync not found", RunStats{}, nil)
		return
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	rc := &runCtx{ctx: ctx, cfg: e.cfg, job: job, dir: filepath.Join(e.cfg.WorkDir, fmt.Sprintf("run-%d", run.ID)),
		log: &runLog{}, stats: &liveStats{}}
	rc.runID = run.ID
	rc.saveSchemaChange = func(c *SchemaChange) error { return e.store.SaveSchemaChange(bg, c) }
	rc.loadCells = func(t string) ([]byte, error) { return e.store.LoadCells(bg, fmt.Sprintf("%d/%s", job.ID, t)) }
	rc.saveCells = func(t string, c []byte) error { return e.store.SaveCells(bg, fmt.Sprintf("%d/%s", job.ID, t), c) }
	rc.saveState = func() {
		rc.stateMu.Lock()
		defer rc.stateMu.Unlock()
		if err := e.store.SaveJobState(bg, job.ID, job.State); err != nil {
			slog.Error("save state", "err", err)
		}
	}
	if rc.job.State.Tables == nil {
		rc.job.State.Tables = map[string]TableState{}
	}
	_ = os.MkdirAll(rc.dir, 0o755)
	defer os.RemoveAll(rc.dir)
	e.res.StartRun(run.ID)
	defer e.res.EndRun(run.ID)
	usage := func() RunStats {
		st := rc.stats.snapshot()
		e.res.RunUsage(run.ID, &st)
		return st
	}
	slog.Info("run started", "run", run.ID, "job", job.Name)

	// flush logs every 1.5s, heartbeat (and pick up cancellation) every 5s
	var cancelled atomic.Bool
	seq := 0
	flush := func() {
		if lines := rc.log.take(); len(lines) > 0 {
			if err := e.store.AppendLogs(bg, run.ID, seq, lines); err != nil {
				slog.Error("append logs", "err", err)
			}
			seq += len(lines)
		}
	}
	done := make(chan struct{})
	loopDone := make(chan struct{})
	go func() {
		defer close(loopDone)
		t := time.NewTicker(1500 * time.Millisecond)
		defer t.Stop()
		lastHB := time.Now()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				flush()
				if time.Since(lastHB) >= 5*time.Second {
					lastHB = time.Now()
					tables, prog := rc.snapshotTables()
					if c, err := e.store.Heartbeat(bg, run.ID, usage(), tables, prog); err == nil && c && !cancelled.Load() {
						cancelled.Store(true)
						rc.log.Printf("*** cancellation requested from the UI ***")
						cancel()
					}
				}
			}
		}
	}()

	err = rc.runSteps()
	close(done)
	<-loopDone
	if serr := e.store.SaveJobState(bg, job.ID, job.State); serr != nil {
		slog.Error("save state", "err", serr)
	}

	status, summary := "succeeded", strings.Join(rc.notes, "; ")
	switch {
	case cancelled.Load():
		status, summary = "cancelled", "cancelled by the user"
	case err != nil && parent.Err() != nil:
		status, summary = "failed", "pod stopped during the run"
	case err != nil:
		status, summary = "failed", err.Error()
		rc.log.Printf("FAILED: %v", err)
	default:
		rc.log.Printf("Done: %s", summary)
	}
	flush()
	tables, _ := rc.snapshotTables()
	if err := e.store.Finish(bg, run.ID, status, summary, usage(), tables); err != nil {
		slog.Error("finish run", "err", err)
	}
	slog.Info("run finished", "run", run.ID, "job", job.Name, "status", status)
}

func (rc *runCtx) setProgress(step, table, phase string, done, total int) {
	rc.resMu.Lock()
	rc.progress = &Progress{Step: step, Table: table, Phase: phase, Done: done, Total: total, Since: utcNow()}
	rc.resMu.Unlock()
}

func (rc *runCtx) addTable(t TableResult) {
	rc.resMu.Lock()
	rc.tables = append(rc.tables, t)
	rc.resMu.Unlock()
}

func (rc *runCtx) snapshotTables() ([]TableResult, *Progress) {
	rc.resMu.Lock()
	defer rc.resMu.Unlock()
	var p *Progress
	if rc.progress != nil {
		cp := *rc.progress
		p = &cp
	}
	return append([]TableResult(nil), rc.tables...), p
}

func (rc *runCtx) runSteps() error {
	c := &rc.job.Config
	if needsTarget(c) && !rc.job.TargetApproved {
		return fmt.Errorf("target %s/%s not approved: create the database or approve the sync in the UI",
			c.Target.Server, c.Target.Database)
	}
	if c.Git.Enabled || c.Schema.Publish {
		rc.setProgress("schema", "", "extracting and comparing the schema", 0, 0)
		if err := rc.schemaStep(); err != nil {
			return fmt.Errorf("schema: %w", err)
		}
	}
	if len(c.Data.Tables) > 0 {
		if err := rc.dataStep(); err != nil {
			return fmt.Errorf("data: %w", err)
		}
	}
	return nil
}

// runCmd streams combined stdout/stderr of an external tool into the run log.
func (rc *runCtx) runCmd(prefix string, extraEnv []string, dir, name string, args ...string) error {
	cmd := exec.CommandContext(rc.ctx, name, args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), extraEnv...)
	cmd.WaitDelay = 10 * time.Second
	pr, pw := io.Pipe()
	cmd.Stdout, cmd.Stderr = pw, pw
	scanDone := make(chan struct{})
	go func() {
		defer close(scanDone)
		sc := bufio.NewScanner(pr)
		sc.Buffer(make([]byte, 64*1024), 1024*1024)
		for sc.Scan() {
			if line := strings.TrimRight(sc.Text(), "\r"); line != "" {
				rc.log.Printf("  %s%s", prefix, line)
			}
		}
		_, _ = io.Copy(io.Discard, pr)
	}()
	err := cmd.Run()
	pw.Close()
	<-scanDone
	if err != nil {
		if rc.ctx.Err() != nil {
			return rc.ctx.Err()
		}
		return fmt.Errorf("%s %s: %w", filepath.Base(name), firstArg(args), err)
	}
	return nil
}

func firstArg(a []string) string {
	if len(a) == 0 {
		return ""
	}
	return a[0]
}
