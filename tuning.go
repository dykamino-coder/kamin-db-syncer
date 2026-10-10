package main

// Tuning knobs (environment variables) and the adaptive batch sizer.

import (
	"os"
	"strconv"
	"time"
)

const (
	smallTableRows = 1000
	maxBlobBytes   = 16 << 20 // keep each compressed blob (and the worker's memory) bounded
)

// scanMaxDop limits parallelism of hashing scans so the sync never grabs all cores of the source.
var scanMaxDop = envInt("SCAN_MAXDOP", 1)

// scanMaxDopMax: adaptive cap for hashing scans of big tables (>= bigScanRows) when the server is
// quiet; hashing scales with cores (1M rows: MAXDOP 1 9.6s, 2 5.5s, 4 3.8s) at the same total CPU.
// 1 disables it.
var scanMaxDopMax = envInt("SCAN_MAXDOP_MAX", 8)

// scanCoreShare: all concurrent scans of a run on one server get at most 1/SCAN_CORE_SHARE of its
// cores (default 2 = half: measured, nothing gets faster beyond it; only used on a quiet server).
var scanCoreShare = max(1, envInt("SCAN_CORE_SHARE", 2))

const (
	bigScanRows = 200_000 // parallel pays off once a scan takes over ~1s; 1M left a 999,840-row table serial
	quietCPUPct = 30      // SQL Server CPU over the last 5 minutes below this = quiet
)

// batchTarget: each data statement aims at this duration -> short transactions and lock times.
var batchTarget = func() time.Duration {
	if f, err := strconv.ParseFloat(os.Getenv("BATCH_TARGET_SECONDS"), 64); err == nil && f > 0 {
		return time.Duration(f * float64(time.Second))
	}
	return 2 * time.Second
}()

// batcher adapts batch size (rows or keys) to hit batchTarget per statement and stay under maxBlobBytes.
type batcher struct {
	size, min, max int
	fixed          bool
	maxBytes       int
}

func newBatcher(start, minSize, maxSize, fixed int) *batcher {
	if fixed > 0 {
		return &batcher{size: fixed, min: fixed, max: fixed, fixed: true, maxBytes: maxBlobBytes}
	}
	return &batcher{size: max(minSize, min(start, maxSize)), min: minSize, max: maxSize, maxBytes: maxBlobBytes}
}

// batcher for this job: blobs also bounded by the bandwidth limit
func (ds *dataSync) batcher(start, minSize, maxSize, fixed int) *batcher {
	b := newBatcher(start, minSize, maxSize, fixed)
	b.maxBytes = ds.maxBlob()
	return b
}

func (b *batcher) observe(n int, elapsed time.Duration, bytes int) {
	if b.fixed || n < b.size || elapsed <= 0 { // a partial (last) batch says nothing about capacity
		return
	}
	ratio := min(2.0, max(0.5, float64(batchTarget)/float64(elapsed)))
	if bytes > 0 {
		ratio = min(ratio, float64(b.maxBytes)/float64(bytes))
	}
	b.size = max(b.min, min(b.max, int(float64(b.size)*ratio)))
}

const defaultParallelTables = 2

// maxCells caps one IBLT round (72 bytes per cell per side); beyond it a reload is cheaper.
const maxCells = 3_000_000

const (
	defaultFullCheck = "0 2 * * *"
)

// reloadParallel: concurrent inserts into the target during a reload. Source reads stay
// sequential (one keyset-paged reader); parsing JSON on the target is the CPU-heavy part.
var reloadParallel = max(1, envInt("RELOAD_PARALLEL", 2))

// reloadReaders: max parallel keyset readers of the source during a reload (see reloadRanges).
var reloadReaders = max(1, envInt("RELOAD_READERS", 8))

// blobGzipLevel: compression of target blobs re-made by the worker (BLOB_GZIP_LEVEL, default 7: about the size SQL Server's COMPRESS gives,
// so the target link carries no more than before).
var blobGzipLevel = envInt("BLOB_GZIP_LEVEL", 7)
