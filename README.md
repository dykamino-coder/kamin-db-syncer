# kamin-db-syncer

kamin-db-syncer keeps a copy of a Microsoft SQL Server database up to date and records the
history of its schema in git. It works object by object rather than copying whole databases:
table data, procedures, functions, views and permissions.

What it is for:

- **A test or reporting copy of a production database** that is refreshed every few minutes
  instead of once a day from a backup, without the load of copying everything each time.
- **Schema history in git.** Every change to a table, procedure or permission becomes a commit
  with a diff, so you can see what changed and when, and review it in merge requests.
- **Nothing to enable on the database side.** No CDC, no Change Tracking, no replication, no
  recovery model changes and no agent on the server. A SQL login with read access is enough.

The service is written in Go and runs as a single container (a Kubernetes Deployment) with a
built-in scheduler, a worker pool and a web UI. Settings and history are kept in MongoDB.

## What happens on each run

A run synchronizes one source → target pair (a "sync" in the UI). Syncs run on a cron schedule,
for example every 10 minutes (`SYNC_SCHEDULE`, or a schedule set on the sync itself).

1. **Schema.** First a cheap check, one query: a fingerprint of the source catalog (object
   modification dates, permissions, users, roles). If nothing changed, the step ends here.
   Otherwise SqlPackage extracts the schema into files (one per object), the changes are
   committed to the sync's git repository and then published to the target with ALTER.
2. **Data.** For each selected table the service finds the rows that differ between source and
   target and copies only those. How it finds them is described below.

## How changed rows are found

The point is to find the differences without moving whole tables and without loading the
source. The checks go from cheapest to most expensive, and most tables stop at the first ones.

**1. Nobody touched the table.** SQL Server keeps usage statistics for every table
(`sys.dm_db_index_usage_stats`). If nothing was written to the table since the last sync, it is
skipped without reading a single row. For 500 tables this is one query and about a second. These
statistics are trusted for at most a day: every 24 hours each table is fully checked anyway.

**2. The table has a `rowversion` column.** SQL Server updates its value on every insert and
update. The service reads only the rows changed since the last sync and compares each one with
the target by key. `rowversion` does not reveal deletes, so the service also compares row counts:
if the source has fewer rows than "target + new keys", rows were deleted and the table gets a
full check (step 3).

**3. Full check with an IBLT.** An IBLT (invertible Bloom lookup table) is a data structure that
finds the difference between two large sets while exchanging an amount of data proportional to
the size of the difference, not the size of the sets. It works like this:

- On each side SQL Server hashes every row (SHA-256 over a binary form of all columns) and
  spreads the rows over a few hundred "cells": every row goes into 3 cells, and a cell holds the
  number of rows and the sums of their hashes and keys. This is a single `GROUP BY` query, and its
  result is a few hundred rows of 72 bytes rather than millions of table rows.
- The service subtracts the target's cells from the source's cells. Rows that are equal on both
  sides cancel out; only the differing ones remain. A cell left with exactly one row reveals that
  row's hash and key. Removing that row from its two other cells produces new single-row cells,
  and so on until every difference is revealed.
- As a result, a table of a million rows in which 500 rows changed is compared by exchanging a few
  hundred kilobytes instead of hundreds of megabytes.

The size of the IBLT is chosen up front from the row operation counters
(`sys.dm_db_index_operational_stats`): how many inserts, updates and deletes happened since the
last sync. If the cells still turn out too few, the service increases their number and recounts.

**Target cell cache.** Only this service writes to the target, so the target's cells do not
have to be recomputed: they are stored in MongoDB and updated after every write. Then only the
source is scanned, which halves the work. The cache is used only if nobody else wrote to the
target (its operation counter has not moved). On top of that, the target rows found as
differences are verified by key before anything is written, and once a day the target is
scanned in full anyway.

**4. Copying the differences.** The rows found are read from the source by key (an index seek,
not a scan), packed into compressed JSON (`COMPRESS(FOR JSON)`) and written to the target in
short transactions: the old version is deleted and the new one inserted. If there are too many
differences, or the target is empty, the table is reloaded in key order chunks; an interrupted
reload continues where it stopped.

### How different keys are handled

| Primary key | How rows are located |
|---|---|
| `int` / `bigint` / `smallint` | the key value is stored in the IBLT cells, rows are found by index seek |
| GUID and composite keys up to 16 bytes (`int`, `date`, `datetime`, `char`, `nchar`, `binary`, `bit`) | the whole key is stored in the cells, also an index seek |
| variable-length strings, `decimal`, long composite keys | the cells hold a hash of the key; the keys themselves are found with one extra scan |
| no key (heap) | a row is identified by its content; duplicates are counted |

### Why not Debezium or CDC

Debezium and CDC read the transaction log and deliver changes within seconds, but they require
CDC to be enabled in the database and a Kafka cluster. Tables without a primary key are a problem
for them: changes to such a table cannot be applied to a copy precisely, because there is no way
to find the row. Here the delay equals the cron interval (minutes), but nothing has to be enabled
on the server, and tables without a key are synchronized exactly, including duplicates.

## Load on the databases

- **Locks.** Checks read the source without shared locks: under SNAPSHOT or READ COMMITTED
  SNAPSHOT when the database allows them, otherwise under READ UNCOMMITTED. This is safe because
  the differences found are re-read under READ COMMITTED before they are copied.
- **Transaction length.** All statements run with `DEADLOCK_PRIORITY LOW` and
  `LOCK_TIMEOUT 30s`. Chunk sizes adapt so that one statement takes about 2 seconds, which keeps
  transactions and locks on the target short.
- **Parallelism.** By default a scan uses one SQL Server core. Tables from 200k rows are scanned
  in parallel only when the server was below 30% CPU over the last 5 minutes, and all scans
  together use at most half of the cores (`SCAN_CORE_SHARE`). On a busy server the service stays
  single-threaded.
- **Network.** Bandwidth can be limited in Mbit/s per sync (in the UI) and per pod
  (`POD_MAX_MBIT`).

## Web UI

- list of syncs with the state of their last run;
- creation wizard: source, target (with a Create database button if it does not exist yet),
  table selection, schedule, git;
- run page: progress, per-table results, log;
- schema change history with diffs;
- pod memory, CPU and network charts and advice on how many pods and resources your databases
  need.

Data is synchronized into a new target only after the target has been approved in the UI, so an
unrelated database cannot be overwritten by mistake.

## Running it

```bash
docker build -t registry.example.local/kamin-db-syncer:2.0.0 .
kubectl apply -f k8s/mongo-dev.yaml          # MongoDB for testing only
kubectl apply -f k8s/kamin-db-syncer.yaml    # fill in the Secret with your values first
```

The UI listens on port 8080 (basic auth: `UI_USER` / `UI_PASSWORD`); Prometheus metrics are at
`/metrics`. You can run several pods: each sync runs on exactly one of them at a time (the queue
lives in MongoDB).

### SQL permissions

- **Source:** `db_datareader`, `VIEW DEFINITION`. Skipping unchanged tables and adaptive
  parallelism also need `VIEW SERVER STATE`; without it those features turn off.
- **Target:** `db_owner`. Publishing the schema, disabling foreign keys and triggers during the
  load and `IDENTITY_INSERT` require owner rights.

SQL Server 2016 or newer is supported (tested on 2019). Databases with a compatibility level
below 130 work as well.

### Environment variables

| Variable | Default | Purpose |
|---|---|---|
| `MONGO_URI`, `MONGO_DB` | `mongodb://localhost:27017`, `kamin_db_syncer` | storage for settings and history |
| `SYNC_SCHEDULE`, `SYNC_TIMEZONE` | —, `UTC` | default cron for syncs without their own schedule |
| `MAX_PARALLEL` | `2` | how many syncs (databases) one pod runs at the same time |
| `CRED_<NAME>_USER`, `CRED_<NAME>_PASSWORD` | — | SQL logins; a sync refers to them as `"credential": "<name>"`. Logins can also be added in the UI |
| `SECRET_KEY` | — | encryption key (AES-256-GCM) for passwords and git keys entered in the UI |
| `UI_USER`, `UI_PASSWORD` | `admin`, — | UI login |
| `GIT_REPO_URL`, `GIT_BRANCH`, `GIT_TOKEN`, `GIT_USERNAME` | —, `main` | default git settings; each sync can have its own |
| `SYNC_CONFIG` / `SYNC_CONFIG_FILE`, `SYNC_CONFIG_MODE` | —, `seed` | syncs from JSON at startup: `seed` creates missing ones, `enforce` overwrites them |
| `POD_MAX_MBIT` | — | total bandwidth limit of the pod |
| `BATCH_TARGET_SECONDS` | `2` | target duration of one write statement |
| `SCAN_MAXDOP` | `1` | SQL Server cores per scan in normal mode |
| `SCAN_CORE_SHARE`, `SCAN_MAXDOP_MAX` | `2`, `8` | share of cores (1/N) and thread cap for scans on an idle server; `SCAN_CORE_SHARE=4` is more cautious, `SCAN_MAXDOP_MAX=1` turns parallelism off |
| `RELOAD_PARALLEL`, `RELOAD_READERS` | `2`, `8` | insert and read threads during a reload (several readers only when the target is on another server) |
| `BLOB_GZIP_LEVEL` | `7` | compression level for data the service recompresses for the target |
| `RUN_RETENTION_DAYS` | `30` | how many days of run history to keep |
| `TRUST_SERVER_CERT` | — | trust the SQL Server certificate without validation |

## Limitations

- The copy lags by the run interval (minutes); this is not streaming replication.
- A table that changed and has no `rowversion` column has to be read in full once per run, to
  hash every row. Tables that did not change are not read at all.
- Deletes in a table with `rowversion` still require a full check of that table.
- The numbers below come from a single test machine; yours will differ.

## How long it takes

Test setup: SQL Server 2019 in a container on a laptop (i9-13900H, 20 threads). Tables of
0.2–1 million rows, up to 200 columns of various types, all kinds of keys and a table without one.

| Scenario | Time |
|---|---|
| 500 tables, none changed | ~1.3 s |
| 25 changed tables out of 500 | 24–34 s |
| 6 changed tables of 0.5–1M rows (4.5M in total), ~500 changes each | 15–17 s |
| the same for one table with `rowversion` | 1–7 s |
| full reload of a 1M-row, 34-column table | ~25 s |

Correctness of every change was verified independently of the service, by comparing the source
and target tables with `EXCEPT` in both directions.

## Code layout

A single Go package, split into files by purpose:

| File | Contents |
|---|---|
| `main.go`, `model.go` | startup, settings, sync and run model |
| `engine.go` | scheduler, queue, workers, the course of a run |
| `store.go` | MongoDB: syncs, runs, logs, cell cache |
| `api.go`, `discover.go`, `sizes.go`, `sizing.go`, `resources.go` | HTTP API for the UI, metrics, resource advice |
| `schema.go`, `gitops.go`, `schemachanges.go` | schema extract and publish, git |
| `credentials.go`, `secrets.go`, `sqlconn.go`, `target.go` | logins, encryption, connections, creating the target database |
| `datastep.go` | the data step: table list, change prepass, worker pool |
| `reconcile.go` | checking one table: cheapest checks first, then the IBLT |
| `iblt.go` | IBLT cells and decoding |
| `rowhash.go`, `tablemeta.go`, `sqlbuild.go` | row hashing, table metadata, SQL building |
| `apply.go`, `reload.go` | copying differences, full reload |
| `cellcache.go`, `rowversion.go` | target cell cache, incremental check by `rowversion` |
| `side.go`, `tuning.go`, `throttle.go` | one side's connection, tuning knobs, bandwidth limit |
| `ui/static/` | the web UI (plain HTML/CSS/JS, no build step) |
| `k8s/` | Kubernetes manifests |

```bash
go vet ./...
go test ./...
```
