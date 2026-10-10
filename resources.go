package main

// Pod resource sampling (every 2s): memory and CPU of the whole container from cgroup v2 (includes
// SqlPackage and git child processes), network from /proc/net/dev. Feeds the live UI graphs,
// per-run accounting (peak memory, CPU seconds, bytes in/out) and the sizing advice.

import (
	"bufio"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

type ResSample struct {
	At       time.Time `json:"at"`
	MemBytes int64     `json:"mem"`
	CPUCores float64   `json:"cpu"`    // cores used over the last interval
	InBps    float64   `json:"in_bps"` // bytes/s received
	OutBps   float64   `json:"out_bps"`
}

type rawCounters struct {
	at       time.Time
	cpuUsec  int64
	rxBytes  int64
	txBytes  int64
	memBytes int64
}

type Resources struct {
	mu       sync.Mutex
	last     rawCounters
	history  []ResSample // ring, newest last
	MemLimit int64       // cgroup memory.max (0 = unlimited/unknown)
	CPULimit float64     // cgroup cpu.max quota in cores (0 = unlimited/unknown)
	runs     map[int64]*runRes
}

// runRes tracks one run's share. With several runs in parallel the pod-level counters overlap,
// so per-run numbers are an upper bound — good enough for sizing.
type runRes struct {
	start   rawCounters
	peakMem int64
}

const historyLen = 300 // 10 minutes at 2s

func newResources() *Resources {
	r := &Resources{runs: map[int64]*runRes{}}
	r.MemLimit = readInt("/sys/fs/cgroup/memory.max")
	if b, err := os.ReadFile("/sys/fs/cgroup/cpu.max"); err == nil {
		f := strings.Fields(string(b))
		if len(f) == 2 && f[0] != "max" {
			q, _ := strconv.ParseFloat(f[0], 64)
			p, _ := strconv.ParseFloat(f[1], 64)
			if p > 0 {
				r.CPULimit = q / p
			}
		}
	}
	r.last = readCounters()
	return r
}

func readInt(path string) int64 {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	n, _ := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
	return n
}

func readCounters() rawCounters {
	c := rawCounters{at: time.Now()}
	if f, err := os.Open("/sys/fs/cgroup/cpu.stat"); err == nil {
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			if k, v, ok := strings.Cut(sc.Text(), " "); ok && k == "usage_usec" {
				c.cpuUsec, _ = strconv.ParseInt(v, 10, 64)
			}
		}
		f.Close()
	}
	// working set as Kubernetes counts it: memory.current minus reclaimable inactive page cache
	c.memBytes = readInt("/sys/fs/cgroup/memory.current")
	if f, err := os.Open("/sys/fs/cgroup/memory.stat"); err == nil {
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			if k, v, ok := strings.Cut(sc.Text(), " "); ok && k == "inactive_file" {
				n, _ := strconv.ParseInt(v, 10, 64)
				c.memBytes = max(0, c.memBytes-n)
			}
		}
		f.Close()
	}
	if c.memBytes == 0 { // no cgroup v2 (e.g. local run): this process only
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		c.memBytes = int64(ms.Sys)
	}
	if f, err := os.Open("/proc/net/dev"); err == nil {
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			name, rest, ok := strings.Cut(sc.Text(), ":")
			n := strings.TrimSpace(name)
			if !ok || n == "lo" || strings.HasPrefix(n, "veth") || strings.HasPrefix(n, "podman") || strings.HasPrefix(n, "cni") ||
				strings.HasPrefix(n, "docker") || strings.HasPrefix(n, "br-") || strings.HasPrefix(n, "virbr") || strings.HasPrefix(n, "flannel") ||
				strings.HasPrefix(n, "cali") || strings.HasPrefix(n, "tunl") || strings.HasPrefix(n, "kube-") {
				continue
			}
			fs := strings.Fields(rest)
			if len(fs) >= 9 {
				rx, _ := strconv.ParseInt(fs[0], 10, 64)
				tx, _ := strconv.ParseInt(fs[8], 10, 64)
				c.rxBytes += rx
				c.txBytes += tx
			}
		}
		f.Close()
	}
	return c
}

func (r *Resources) Run(stop <-chan struct{}) {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			r.sample()
		}
	}
}

func (r *Resources) sample() {
	c := readCounters()
	r.mu.Lock()
	defer r.mu.Unlock()
	dt := c.at.Sub(r.last.at).Seconds()
	if dt <= 0 {
		return
	}
	s := ResSample{At: c.at.UTC(), MemBytes: c.memBytes,
		CPUCores: float64(c.cpuUsec-r.last.cpuUsec) / 1e6 / dt,
		InBps:    float64(c.rxBytes-r.last.rxBytes) / dt,
		OutBps:   float64(c.txBytes-r.last.txBytes) / dt}
	if s.CPUCores < 0 || s.InBps < 0 || s.OutBps < 0 { // counter reset
		s.CPUCores, s.InBps, s.OutBps = 0, 0, 0
	}
	r.last = c
	r.history = append(r.history, s)
	if len(r.history) > historyLen {
		r.history = r.history[len(r.history)-historyLen:]
	}
	for _, rr := range r.runs {
		rr.peakMem = max(rr.peakMem, c.memBytes)
	}
}

func (r *Resources) Snapshot() (cur ResSample, hist []ResSample) {
	r.mu.Lock()
	defer r.mu.Unlock()
	hist = append([]ResSample(nil), r.history...)
	if len(hist) > 0 {
		cur = hist[len(hist)-1]
	}
	return
}

func (r *Resources) StartRun(id int64) {
	c := readCounters()
	r.mu.Lock()
	r.runs[id] = &runRes{start: c, peakMem: c.memBytes}
	r.mu.Unlock()
}

// RunUsage fills the resource fields of a run's stats (called on heartbeat and at the end).
func (r *Resources) RunUsage(id int64, st *RunStats) {
	r.mu.Lock()
	rr := r.runs[id]
	r.mu.Unlock()
	if rr == nil {
		return
	}
	c := readCounters()
	st.CPUSeconds = float64(c.cpuUsec-rr.start.cpuUsec) / 1e6
	st.NetIn = c.rxBytes - rr.start.rxBytes
	st.NetOut = c.txBytes - rr.start.txBytes
	st.PeakMem = max(rr.peakMem, c.memBytes)
}

func (r *Resources) EndRun(id int64) {
	r.mu.Lock()
	delete(r.runs, id)
	r.mu.Unlock()
}
