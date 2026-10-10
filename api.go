package main

import (
	"context"
	"crypto/subtle"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

//go:embed ui/static
var staticFS embed.FS

type API struct {
	cfg   *Settings
	store *Store
	eng   *Engine
	res   *Resources
}

func (a *API) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", a.healthz)
	mux.HandleFunc("GET /metrics", a.metrics)
	mux.HandleFunc("GET /api/status", a.status)
	mux.HandleFunc("GET /api/jobs", a.listJobs)
	mux.HandleFunc("POST /api/jobs", a.saveJob)
	mux.HandleFunc("GET /api/jobs/{id}", a.getJob)
	mux.HandleFunc("PUT /api/jobs/{id}", a.saveJob)
	mux.HandleFunc("DELETE /api/jobs/{id}", a.deleteJob)
	mux.HandleFunc("POST /api/jobs/{id}/run", a.runJob)
	mux.HandleFunc("GET /api/runs", a.listRuns)
	mux.HandleFunc("GET /api/runs/{id}", a.getRun)
	mux.HandleFunc("GET /api/runs/{id}/logs", a.getLogs)
	mux.HandleFunc("POST /api/runs/{id}/cancel", a.cancelRun)
	mux.HandleFunc("GET /api/cron-preview", a.cronPreview)
	mux.HandleFunc("POST /api/test-connection", a.testConn)
	mux.HandleFunc("GET /api/export", a.export)
	mux.HandleFunc("POST /api/test-git", a.testGitAccess)
	mux.HandleFunc("GET /api/resources", a.resources)
	mux.HandleFunc("GET /api/sizing", a.sizing)
	mux.HandleFunc("GET /api/credentials", a.listCredentials)
	mux.HandleFunc("POST /api/credentials", a.saveCredential)
	mux.HandleFunc("DELETE /api/credentials/{name}", a.deleteCredential)
	mux.HandleFunc("GET /api/jobs/{id}/sizes", a.jobSizes)
	mux.HandleFunc("POST /api/discover/databases", a.discoverDatabases)
	mux.HandleFunc("POST /api/discover/tables", a.discoverTables)
	mux.HandleFunc("POST /api/discover/target", a.discoverTarget)
	mux.HandleFunc("GET /api/jobs/{id}/target", a.getTarget)
	mux.HandleFunc("POST /api/jobs/{id}/target/create", a.createTarget)
	mux.HandleFunc("POST /api/jobs/{id}/target/approve", a.approveTarget)
	mux.HandleFunc("POST /api/jobs/{id}/target/revoke", a.revokeTarget)
	mux.HandleFunc("GET /api/schema-changes", a.listSchemaChanges)
	mux.HandleFunc("GET /api/schema-changes/{id}", a.getSchemaChange)
	sub, _ := fs.Sub(staticFS, "ui/static")
	mux.Handle("GET /static/", http.StripPrefix("/static/", http.FileServerFS(sub)))
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFileFS(w, r, sub, "index.html")
	})
	return a.auth(mux)
}

func (a *API) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if a.cfg.UIPassword != "" && r.URL.Path != "/healthz" && r.URL.Path != "/metrics" {
			u, p, ok := r.BasicAuth()
			if !ok || subtle.ConstantTimeCompare([]byte(u), []byte(a.cfg.UIUser)) != 1 ||
				subtle.ConstantTimeCompare([]byte(p), []byte(a.cfg.UIPassword)) != 1 {
				w.Header().Set("WWW-Authenticate", `Basic realm="kamin-db-syncer"`)
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

// ---------- helpers ----------

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func fail(w http.ResponseWriter, code int, detail any) {
	writeJSON(w, code, map[string]any{"detail": detail})
}

func pathID(r *http.Request) int64 {
	id, _ := strconv.ParseInt(r.PathValue("id"), 10, 64)
	return id
}

func (a *API) ok(w http.ResponseWriter, err error) bool {
	if err != nil {
		fail(w, http.StatusInternalServerError, err.Error())
		return false
	}
	return true
}

// ---------- handlers ----------

func (a *API) healthz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	if err := a.store.Ping(ctx); err != nil {
		fail(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	writeJSON(w, 200, map[string]bool{"ok": true})
}

func (a *API) status(w http.ResponseWriter, r *http.Request) {
	st, err := a.store.Stats(r.Context())
	if !a.ok(w, err) {
		return
	}
	writeJSON(w, 200, map[string]any{
		"pod": a.cfg.Pod, "default_schedule": a.cfg.DefaultSchedule, "timezone": a.cfg.TZ.String(),
		"max_parallel": a.cfg.MaxParallel, "config_mode": a.cfg.ConfigMode, "git_repo": a.cfg.GitRepoURL,
		"git_branch": a.cfg.GitBranch, "credentials": credNames(), "credential_list": allCredentials(), "stats": st,
		"scan_maxdop": scanMaxDop, "pod_max_mbit": podMaxMbit, "secret_key_set": os.Getenv("SECRET_KEY") != "", "batch_target_seconds": batchTarget.Seconds(),
	})
}

type jobView struct {
	Job
	EffectiveSchedule string     `json:"effective_schedule"`
	LastRunID         *int64     `json:"last_run_id"`
	LastStatus        string     `json:"last_status"`
	LastStartedAt     *time.Time `json:"last_started_at"`
	LastSummary       string     `json:"last_summary"`
	Active            bool       `json:"active"`
}

func (a *API) view(ctx context.Context, j Job) (jobView, error) {
	j.Config.Git.HasSecret = j.Config.Git.SecretEnc != ""
	v := jobView{Job: j, EffectiveSchedule: a.eng.effectiveSchedule(j.Schedule)}
	last, err := a.store.LastRun(ctx, j.ID)
	if err != nil {
		return v, err
	}
	if last != nil {
		v.LastRunID, v.LastStatus, v.LastStartedAt, v.LastSummary = &last.ID, last.Status, last.StartedAt, last.Summary
		if v.LastStartedAt == nil {
			v.LastStartedAt = &last.QueuedAt
		}
	}
	v.Active, err = a.store.HasActiveRun(ctx, j.ID)
	return v, err
}

func (a *API) listJobs(w http.ResponseWriter, r *http.Request) {
	jobs, err := a.store.ListJobs(r.Context())
	if !a.ok(w, err) {
		return
	}
	out := make([]jobView, 0, len(jobs))
	for _, j := range jobs {
		v, err := a.view(r.Context(), j)
		if !a.ok(w, err) {
			return
		}
		out = append(out, v)
	}
	writeJSON(w, 200, out)
}

func (a *API) getJob(w http.ResponseWriter, r *http.Request) {
	j, err := a.store.GetJob(r.Context(), pathID(r))
	if !a.ok(w, err) {
		return
	}
	if j == nil {
		fail(w, 404, "not found")
		return
	}
	v, err := a.view(r.Context(), *j)
	if a.ok(w, err) {
		writeJSON(w, 200, v)
	}
}

func (a *API) saveJob(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Name     string    `json:"name"`
		Enabled  bool      `json:"enabled"`
		Schedule *string   `json:"schedule"`
		Config   JobConfig `json:"config"`
	}
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		fail(w, 400, err.Error())
		return
	}
	ctx := r.Context()
	id := pathID(r)
	var existing *Job
	if id > 0 {
		var err error
		if existing, err = a.store.GetJob(ctx, id); !a.ok(w, err) {
			return
		}
		if existing == nil {
			fail(w, 404, "not found")
			return
		}
	}
	sched := ""
	if in.Schedule != nil {
		sched = strings.TrimSpace(*in.Schedule)
	}
	in.Name = strings.TrimSpace(in.Name)
	errs := validateJob(in.Name, sched, &in.Config)
	if other, err := a.store.GetJobByName(ctx, in.Name); err == nil && other != nil && other.ID != id {
		errs = append(errs, fmt.Sprintf("name: a sync named %q already exists", in.Name))
	}
	if len(errs) > 0 {
		fail(w, http.StatusUnprocessableEntity, errs)
		return
	}
	// git secret: new value -> encrypt; empty -> keep stored; clearSecret -> drop
	g := &in.Config.Git
	switch {
	case g.Secret != "":
		enc, err := encryptSecret(g.Secret)
		if err != nil {
			fail(w, http.StatusUnprocessableEntity, []string{"git: " + err.Error()})
			return
		}
		g.SecretEnc = enc
	case g.ClearSecret:
		g.SecretEnc = ""
	case existing != nil:
		g.SecretEnc = existing.Config.Git.SecretEnc
	}
	g.Secret, g.ClearSecret = "", false
	j := &Job{ID: id, Name: in.Name, Enabled: in.Enabled, Schedule: sched, Config: in.Config, Source: "ui",
		NextRunAt: a.eng.nextRunFor(in.Enabled, sched)}
	if existing != nil {
		j.Source = existing.Source
	}
	newID, err := a.store.SaveJob(ctx, j)
	if !a.ok(w, err) {
		return
	}
	if existing != nil && (!strings.EqualFold(existing.Config.Target.Server, in.Config.Target.Server) ||
		!strings.EqualFold(existing.Config.Target.Database, in.Config.Target.Database)) && existing.TargetApproved {
		if !a.ok(w, a.store.SetApproval(ctx, newID, false, false, "")) {
			return
		}
	}
	saved, err := a.store.GetJob(ctx, newID)
	if !a.ok(w, err) {
		return
	}
	v, err := a.view(ctx, *saved)
	if a.ok(w, err) {
		code := 200
		if id == 0 {
			code = 201
		}
		writeJSON(w, code, v)
	}
}

func (a *API) deleteJob(w http.ResponseWriter, r *http.Request) {
	if a.ok(w, a.store.DeleteJob(r.Context(), pathID(r))) {
		w.WriteHeader(http.StatusNoContent)
	}
}

func (a *API) runJob(w http.ResponseWriter, r *http.Request) {
	j, err := a.store.GetJob(r.Context(), pathID(r))
	if !a.ok(w, err) {
		return
	}
	if j == nil {
		fail(w, 404, "not found")
		return
	}
	if needsTarget(&j.Config) && !j.TargetApproved {
		fail(w, http.StatusConflict, "target not approved: create the database or approve the sync on its page")
		return
	}
	id, err := a.store.Enqueue(r.Context(), j, "manual")
	if !a.ok(w, err) {
		return
	}
	if id == 0 {
		fail(w, http.StatusConflict, "the sync is already queued or running")
		return
	}
	writeJSON(w, 200, map[string]int64{"run_id": id})
}

func (a *API) listRuns(w http.ResponseWriter, r *http.Request) {
	jobID, _ := strconv.ParseInt(r.URL.Query().Get("job_id"), 10, 64)
	limit, _ := strconv.ParseInt(r.URL.Query().Get("limit"), 10, 64)
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	runs, err := a.store.ListRuns(r.Context(), jobID, limit)
	if a.ok(w, err) {
		writeJSON(w, 200, runs)
	}
}

func (a *API) getRun(w http.ResponseWriter, r *http.Request) {
	run, err := a.store.GetRun(r.Context(), pathID(r))
	if !a.ok(w, err) {
		return
	}
	if run == nil {
		fail(w, 404, "not found")
		return
	}
	writeJSON(w, 200, run)
}

func (a *API) getLogs(w http.ResponseWriter, r *http.Request) {
	after, err := strconv.Atoi(r.URL.Query().Get("after"))
	if err != nil {
		after = -1
	}
	lines, err := a.store.GetLogs(r.Context(), pathID(r), after)
	if a.ok(w, err) {
		writeJSON(w, 200, lines)
	}
}

func (a *API) cancelRun(w http.ResponseWriter, r *http.Request) {
	if a.ok(w, a.store.RequestCancel(r.Context(), pathID(r))) {
		writeJSON(w, 200, map[string]bool{"ok": true})
	}
}

func (a *API) cronPreview(w http.ResponseWriter, r *http.Request) {
	eff := a.eng.effectiveSchedule(r.URL.Query().Get("expr"))
	next := a.eng.nextRuns(eff, 3, time.Now())
	if next == nil {
		next = []time.Time{}
	}
	writeJSON(w, 200, map[string]any{"effective": eff, "timezone": a.cfg.TZ.String(), "next": next})
}

func (a *API) testConn(w http.ResponseWriter, r *http.Request) {
	var ep Endpoint
	if err := json.NewDecoder(r.Body).Decode(&ep); err != nil {
		fail(w, 400, err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	msg, err := testConnection(ctx, ep)
	if err != nil {
		writeJSON(w, 200, map[string]any{"ok": false, "message": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "message": msg})
}

func (a *API) export(w http.ResponseWriter, r *http.Request) {
	jobs, err := a.store.ListJobs(r.Context())
	if !a.ok(w, err) {
		return
	}
	out := make([]envJob, 0, len(jobs))
	for _, j := range jobs {
		enabled := j.Enabled
		out = append(out, envJob{Name: j.Name, Enabled: &enabled, Schedule: j.Schedule, JobConfig: j.Config})
	}
	w.Header().Set("Content-Disposition", `attachment; filename="sync-config.json"`)
	writeJSON(w, 200, map[string]any{"jobs": out})
}

func (a *API) metrics(w http.ResponseWriter, r *http.Request) {
	jobs, err := a.store.ListJobs(r.Context())
	if !a.ok(w, err) {
		return
	}
	var b strings.Builder
	b.WriteString("# TYPE kamin_db_syncer_last_run_success gauge\n# TYPE kamin_db_syncer_last_run_bytes gauge\n# TYPE kamin_db_syncer_runs gauge\n")
	for _, j := range jobs {
		last, err := a.store.LastRun(r.Context(), j.ID)
		if err != nil || last == nil || (last.Status != "succeeded" && last.Status != "failed") {
			continue
		}
		okv := 0
		if last.Status == "succeeded" {
			okv = 1
		}
		fmt.Fprintf(&b, "kamin_db_syncer_last_run_success{job=%q} %d\n", j.Name, okv)
		if last.Stats != nil {
			fmt.Fprintf(&b, "kamin_db_syncer_last_run_bytes{job=%q} %d\n", j.Name, last.Stats.BytesRead+last.Stats.BytesWritten)
		}
	}
	st, err := a.store.Stats(r.Context())
	if a.ok(w, err) {
		for k, v := range st {
			fmt.Fprintf(&b, "kamin_db_syncer_runs{state=%q} %d\n", k, v)
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = w.Write([]byte(b.String()))
	}
}

func (a *API) testGitAccess(w http.ResponseWriter, r *http.Request) {
	var in struct {
		JobID int64     `json:"job_id"`
		Git   GitConfig `json:"git"`
	}
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		fail(w, 400, err.Error())
		return
	}
	g := in.Git
	if g.Secret != "" {
		enc, err := encryptSecret(g.Secret)
		if err != nil {
			writeJSON(w, 200, map[string]any{"ok": false, "message": err.Error()})
			return
		}
		g.SecretEnc = enc
	} else if in.JobID > 0 && !g.ClearSecret {
		if j, err := a.store.GetJob(r.Context(), in.JobID); err == nil && j != nil {
			g.SecretEnc = j.Config.Git.SecretEnc
		}
	}
	msg, err := testGit(r.Context(), a.cfg, g)
	if err != nil {
		writeJSON(w, 200, map[string]any{"ok": false, "message": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "message": msg})
}

func (a *API) listSchemaChanges(w http.ResponseWriter, r *http.Request) {
	jobID, _ := strconv.ParseInt(r.URL.Query().Get("job_id"), 10, 64)
	list, err := a.store.ListSchemaChanges(r.Context(), jobID, 200)
	if a.ok(w, err) {
		writeJSON(w, 200, list)
	}
}

func (a *API) getSchemaChange(w http.ResponseWriter, r *http.Request) {
	c, err := a.store.GetSchemaChange(r.Context(), pathID(r))
	if !a.ok(w, err) {
		return
	}
	if c == nil {
		fail(w, 404, "not found")
		return
	}
	writeJSON(w, 200, c)
}

func (a *API) jobOr404(w http.ResponseWriter, r *http.Request) *Job {
	j, err := a.store.GetJob(r.Context(), pathID(r))
	if !a.ok(w, err) {
		return nil
	}
	if j == nil {
		fail(w, 404, "not found")
	}
	return j
}

func uiUser(r *http.Request) string {
	if u, _, ok := r.BasicAuth(); ok {
		return u
	}
	return "ui"
}

func (a *API) getTarget(w http.ResponseWriter, r *http.Request) {
	if j := a.jobOr404(w, r); j != nil {
		writeJSON(w, 200, targetStatus(r.Context(), j))
	}
}

func (a *API) createTarget(w http.ResponseWriter, r *http.Request) {
	j := a.jobOr404(w, r)
	if j == nil {
		return
	}
	msg, err := createTargetDB(r.Context(), j)
	if err != nil {
		fail(w, http.StatusConflict, err.Error())
		return
	}
	// a database we just created is empty: syncing into it cannot destroy anything
	if !a.ok(w, a.store.SetApproval(r.Context(), j.ID, true, true, uiUser(r))) {
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "message": msg})
}

func (a *API) approveTarget(w http.ResponseWriter, r *http.Request) {
	j := a.jobOr404(w, r)
	if j == nil {
		return
	}
	st := targetStatus(r.Context(), j)
	if !st.Exists {
		fail(w, http.StatusConflict, "the target database does not exist — create it with Create database")
		return
	}
	if a.ok(w, a.store.SetApproval(r.Context(), j.ID, true, j.TargetCreated, uiUser(r))) {
		writeJSON(w, 200, map[string]any{"ok": true})
	}
}

func (a *API) revokeTarget(w http.ResponseWriter, r *http.Request) {
	j := a.jobOr404(w, r)
	if j != nil && a.ok(w, a.store.SetApproval(r.Context(), j.ID, false, j.TargetCreated, "")) {
		writeJSON(w, 200, map[string]any{"ok": true})
	}
}

func credNames() []string {
	out := []string{}
	for _, c := range allCredentials() {
		out = append(out, c.Name)
	}
	return out
}
