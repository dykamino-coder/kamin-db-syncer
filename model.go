package main

import (
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// ---------- settings (environment) ----------

type Settings struct {
	Listen          string
	MongoURI        string
	MongoDB         string
	DefaultSchedule string
	TZ              *time.Location
	MaxParallel     int
	WorkDir         string
	RetentionDays   int
	ConfigMode      string // seed | enforce
	RunOnStart      bool
	UIUser          string
	UIPassword      string
	GitRepoURL      string
	GitBranch       string
	Pod             string
}

func env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if n, err := strconv.Atoi(os.Getenv(key)); err == nil && n > 0 {
		return n
	}
	return def
}

func loadSettings() (*Settings, error) {
	tz, err := time.LoadLocation(env("SYNC_TIMEZONE", "UTC"))
	if err != nil {
		return nil, err
	}
	host, _ := os.Hostname()
	return &Settings{
		Listen:          env("LISTEN", ":8080"),
		MongoURI:        env("MONGO_URI", "mongodb://localhost:27017"),
		MongoDB:         env("MONGO_DB", "kamin_db_syncer"),
		DefaultSchedule: env("SYNC_SCHEDULE", ""),
		TZ:              tz,
		MaxParallel:     envInt("MAX_PARALLEL", 2),
		WorkDir:         env("WORK_DIR", "/work"),
		RetentionDays:   envInt("RUN_RETENTION_DAYS", 30),
		ConfigMode:      env("SYNC_CONFIG_MODE", "seed"),
		RunOnStart:      strings.EqualFold(env("RUN_ON_START", ""), "true"),
		UIUser:          env("UI_USER", "admin"),
		UIPassword:      os.Getenv("UI_PASSWORD"),
		GitRepoURL:      env("GIT_REPO_URL", ""),
		GitBranch:       env("GIT_BRANCH", "main"),
		Pod:             env("HOSTNAME", host),
	}, nil
}

// ---------- job configuration (stored in MongoDB, edited in UI) ----------

type Endpoint struct {
	Server     string `json:"server" bson:"server"`
	Database   string `json:"database" bson:"database"`
	Credential string `json:"credential" bson:"credential"`
}

type GitConfig struct {
	Enabled  bool   `json:"enabled" bson:"enabled"`
	Path     string `json:"path,omitempty" bson:"path,omitempty"`       // folder inside the repo (default: job name)
	RepoURL  string `json:"repoUrl,omitempty" bson:"repoUrl,omitempty"` // empty: GIT_REPO_URL / GIT_TOKEN from env
	Branch   string `json:"branch,omitempty" bson:"branch,omitempty"`
	Username string `json:"username,omitempty" bson:"username,omitempty"`
	Auth     string `json:"auth,omitempty" bson:"auth,omitempty"` // token | ssh
	// write-only from the UI; stored only encrypted, never returned
	Secret      string `json:"secret,omitempty" bson:"-"`
	ClearSecret bool   `json:"clearSecret,omitempty" bson:"-"`
	SecretEnc   string `json:"-" bson:"secretEnc,omitempty"`
	HasSecret   bool   `json:"hasSecret" bson:"-"`
}

type SchemaConfig struct {
	Publish                 bool   `json:"publish" bson:"publish"`
	DropObjectsNotInSource  bool   `json:"dropObjectsNotInSource" bson:"dropObjectsNotInSource"`
	BlockOnPossibleDataLoss *bool  `json:"blockOnPossibleDataLoss,omitempty" bson:"blockOnPossibleDataLoss,omitempty"`
	ExcludeObjectTypes      string `json:"excludeObjectTypes,omitempty" bson:"excludeObjectTypes,omitempty"`
	AlwaysPublish           bool   `json:"alwaysPublish" bson:"alwaysPublish"`
}

type DataConfig struct {
	Tables    []string `json:"tables" bson:"tables"`
	Mode      string   `json:"mode,omitempty" bson:"mode,omitempty"`           // diff (default) | replace
	ChunkRows int      `json:"chunkRows,omitempty" bson:"chunkRows,omitempty"` // 0 = adaptive
	// bandwidth limit for this job in Mbit/s, each direction (0 = unlimited; POD_MAX_MBIT caps all jobs)
	MaxMbit float64 `json:"maxMbit,omitempty" bson:"maxMbit,omitempty"`
	// re-validate FKs after loading (scans the table under a schema lock); off = metadata-only re-enable
	VerifyForeignKeys bool `json:"verifyForeignKeys" bson:"verifyForeignKeys"`
	// tables processed at the same time (0 = 2); more = faster runs, more load on both servers
	ParallelTables int `json:"parallelTables,omitempty" bson:"parallelTables,omitempty"`
	// big tables with ascending integer PK: each run checks only the newest WindowRows rows,
	// the whole table is reconciled on the FullCheck cron (default nightly)
	WindowRows int    `json:"windowRows,omitempty" bson:"windowRows,omitempty"`
	FullCheck  string `json:"fullCheck,omitempty" bson:"fullCheck,omitempty"`
}

type JobConfig struct {
	Source Endpoint     `json:"source" bson:"source"`
	Target Endpoint     `json:"target" bson:"target"`
	Git    GitConfig    `json:"git" bson:"git"`
	Schema SchemaConfig `json:"schema" bson:"schema"`
	Data   DataConfig   `json:"data" bson:"data"`
}

// JobState is written by runs and read by the next run (skip unchanged work, size the IBLT).
type TableState struct {
	LastDiff  int    `json:"lastDiff" bson:"lastDiff"`                       // sizes the next IBLT
	BatchRows int    `json:"batchRows,omitempty" bson:"batchRows,omitempty"` // learned batch size
	SrcMark   string `json:"srcMark,omitempty" bson:"srcMark,omitempty"`     // COUNT:MAX(rowversion) at last sync
	Reloading bool   `json:"reloading,omitempty" bson:"reloading,omitempty"` // interrupted reload resumes
	CheckedAt string `json:"checkedAt,omitempty" bson:"checkedAt,omitempty"` // source server time covered by last sync
	Rows      int64  `json:"rows,omitempty" bson:"rows,omitempty"`           // source rows at last sync (metadata)
	LastFull  string `json:"lastFull,omitempty" bson:"lastFull,omitempty"`   // last full reconciliation (RFC3339)
	ResumeKey string `json:"resumeKey,omitempty" bson:"resumeKey,omitempty"` // reload: last key of the committed prefix
	// last IBLT verification (RFC3339); the cheap change probe is trusted for at most 24h after it
	LastVerified string `json:"lastVerified,omitempty" bson:"lastVerified,omitempty"`
	// row operation counters (sys.dm_db_index_operational_stats) at last sync: their growth bounds
	// the number of differences and sizes the next IBLT (0 = unknown)
	SrcOps int64 `json:"srcOps,omitempty" bson:"srcOps,omitempty"`
	TgtOps int64 `json:"tgtOps,omitempty" bson:"tgtOps,omitempty"`
	// target IBLT cells are cached (iblt_cells) and still describe the target; TgtScanned is the
	// last real scan of the target (the cache is trusted for at most 24h after it)
	TgtCached  bool   `json:"tgtCached,omitempty" bson:"tgtCached,omitempty"`
	TgtScanned string `json:"tgtScanned,omitempty" bson:"tgtScanned,omitempty"`
	// rowversion tables: MIN_ACTIVE_ROWVERSION before the last reconciliation (hex) and the time of
	// the last full IBLT pass; rows changed since the mark are found without hashing the table
	RvMark string `json:"rvMark,omitempty" bson:"rvMark,omitempty"`
	IBLTAt string `json:"ibltAt,omitempty" bson:"ibltAt,omitempty"`
}

type JobState struct {
	GitHash     string `json:"gitHash,omitempty" bson:"gitHash,omitempty"`
	PublishHash string `json:"publishHash,omitempty" bson:"publishHash,omitempty"`
	// cheap catalog fingerprint: unchanged -> SqlPackage is not run at all
	SchemaFP     string                `json:"schemaFp,omitempty" bson:"schemaFp,omitempty"`
	SchemaFullAt string                `json:"schemaFullAt,omitempty" bson:"schemaFullAt,omitempty"` // last real extract (RFC3339)
	PublishedTo  string                `json:"publishedTo,omitempty" bson:"publishedTo,omitempty"`   // target identity at last publish
	Tables       map[string]TableState `json:"tables,omitempty" bson:"tables,omitempty"`
}

type Job struct {
	ID        int64      `json:"id" bson:"_id"`
	Name      string     `json:"name" bson:"name"`
	Enabled   bool       `json:"enabled" bson:"enabled"`
	Schedule  string     `json:"schedule" bson:"schedule"`
	Config    JobConfig  `json:"config" bson:"config"`
	Source    string     `json:"source" bson:"source"` // ui | env
	NextRunAt *time.Time `json:"next_run_at" bson:"next_run_at"`
	UpdatedAt time.Time  `json:"updated_at" bson:"updated_at"`
	State     JobState   `json:"state" bson:"state"`
	// writes to the target are allowed only after approval (see target.go)
	TargetApproved bool       `json:"target_approved" bson:"targetApproved"`
	ApprovedAt     *time.Time `json:"approved_at" bson:"approvedAt,omitempty"`
	ApprovedBy     string     `json:"approved_by" bson:"approvedBy,omitempty"`
	TargetCreated  bool       `json:"target_created" bson:"targetCreated,omitempty"`
}

type RunStats struct {
	BytesRead      int64 `json:"bytes_read" bson:"bytes_read"`
	BytesWritten   int64 `json:"bytes_written" bson:"bytes_written"`
	RowsChanged    int64 `json:"rows_changed" bson:"rows_changed"`
	TablesChanged  int   `json:"tables_changed" bson:"tables_changed"`
	TablesReloaded int   `json:"tables_reloaded" bson:"tables_reloaded"` // full reloads (initial load etc.)
	// pod-level resources measured during the run (cgroup + /proc/net/dev)
	CPUSeconds float64 `json:"cpu_seconds" bson:"cpu_seconds"`
	PeakMem    int64   `json:"peak_mem" bson:"peak_mem"`
	NetIn      int64   `json:"net_in" bson:"net_in"`
	NetOut     int64   `json:"net_out" bson:"net_out"`
}

type Run struct {
	ID              int64         `json:"id" bson:"_id"`
	JobID           int64         `json:"job_id" bson:"job_id"`
	JobName         string        `json:"job_name" bson:"job_name"`
	Status          string        `json:"status" bson:"status"` // queued|running|succeeded|failed|cancelled
	Active          *bool         `json:"-" bson:"active,omitempty"`
	TriggerType     string        `json:"trigger_type" bson:"trigger_type"`
	QueuedAt        time.Time     `json:"queued_at" bson:"queued_at"`
	StartedAt       *time.Time    `json:"started_at" bson:"started_at"`
	FinishedAt      *time.Time    `json:"finished_at" bson:"finished_at"`
	HeartbeatAt     *time.Time    `json:"-" bson:"heartbeat_at,omitempty"`
	Worker          string        `json:"worker" bson:"worker"`
	CancelRequested bool          `json:"cancel_requested" bson:"cancel_requested"`
	Summary         string        `json:"summary" bson:"summary"`
	Stats           *RunStats     `json:"stats,omitempty" bson:"stats,omitempty"`
	Tables          []TableResult `json:"tables,omitempty" bson:"tables,omitempty"`
	Progress        *Progress     `json:"progress,omitempty" bson:"progress,omitempty"`
}

// TableResult: what one run did with one table (shown as the run dashboard).
type TableResult struct {
	Table       string `json:"table" bson:"table"`
	Action      string `json:"action" bson:"action"` // skipped | unchanged | changed | reload | error
	RowsChecked int64  `json:"rows_checked" bson:"rows_checked"`
	Inserted    int64  `json:"inserted" bson:"inserted"`
	Updated     int64  `json:"updated" bson:"updated"`
	Deleted     int64  `json:"deleted" bson:"deleted"`
	Bytes       int64  `json:"bytes" bson:"bytes"`
	Millis      int64  `json:"ms" bson:"ms"`
	Note        string `json:"note,omitempty" bson:"note,omitempty"`
	Error       string `json:"error,omitempty" bson:"error,omitempty"`
}

// Progress of a running run.
type Progress struct {
	Step  string    `json:"step" bson:"step"` // schema | data
	Table string    `json:"table,omitempty" bson:"table,omitempty"`
	Phase string    `json:"phase" bson:"phase"`
	Done  int       `json:"done" bson:"done"`
	Total int       `json:"total" bson:"total"`
	Since time.Time `json:"since" bson:"since"`
}

type LogLine struct {
	Seq  int       `json:"seq" bson:"seq"`
	TS   time.Time `json:"ts" bson:"ts"`
	Line string    `json:"line" bson:"line"`
}

// ---------- validation ----------

var nameRe = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,100}$`)

func validateJob(name, schedule string, c *JobConfig) []string {
	var errs []string
	if !nameRe.MatchString(name) {
		errs = append(errs, "name: latin letters, digits, _ . - only (up to 100 characters)")
	}
	if schedule != "" && schedule != "-" {
		if _, err := parseCron(schedule); err != nil {
			errs = append(errs, `schedule: invalid cron expression "`+schedule+`"`)
		}
	}
	req := func(prefix string, e Endpoint) {
		for k, v := range map[string]string{"server": e.Server, "database": e.Database, "credential": e.Credential} {
			if strings.TrimSpace(v) == "" {
				errs = append(errs, prefix+"."+k+": required")
			}
		}
	}
	req("source", c.Source)
	needsTarget := c.Schema.Publish || len(c.Data.Tables) > 0
	if needsTarget {
		req("target", c.Target)
	}
	if fc := strings.TrimSpace(c.Data.FullCheck); fc != "" {
		if _, err := parseCron(fc); err != nil {
			errs = append(errs, `data.fullCheck: invalid cron expression "`+fc+`"`)
		}
	}
	if m := c.Data.Mode; m != "" && m != "diff" && m != "replace" {
		errs = append(errs, "data.mode: diff or replace")
	}
	if !c.Git.Enabled && !needsTarget {
		errs = append(errs, "the sync does nothing: enable git, schema publishing or data copy")
	}
	sort.Strings(errs)
	return errs
}

// credentialNames lists credentials available as CRED_<NAME>_USER / CRED_<NAME>_PASSWORD.
func credentialNames() []string {
	var out []string
	for _, kv := range os.Environ() {
		k, _, _ := strings.Cut(kv, "=")
		if strings.HasPrefix(k, "CRED_") && strings.HasSuffix(k, "_USER") {
			key := strings.TrimSuffix(strings.TrimPrefix(k, "CRED_"), "_USER")
			if key != "" && os.Getenv("CRED_"+key+"_PASSWORD") != "" {
				out = append(out, strings.ReplaceAll(strings.ToLower(key), "_", "-"))
			}
		}
	}
	sort.Strings(out)
	return out
}

func credentialKey(name string) string {
	var b strings.Builder
	for _, r := range strings.ToUpper(name) {
		if (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	return b.String()
}
