package main

// Sizing advice from measured runs (including the one in progress): how many worker slots and
// replicas are needed so every database finishes before its next occurrence, and what memory / CPU
// requests and limits a pod needs.

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"sort"
	"time"
)

type JobSizing struct {
	JobID       int64   `json:"job_id"`
	Name        string  `json:"name"`
	Runs        int     `json:"runs"`
	InProgress  bool    `json:"in_progress"`
	DurAvg      float64 `json:"dur_avg_s"`
	DurP95      float64 `json:"dur_p95_s"`
	Interval    float64 `json:"interval_s"` // 0 = manual
	Duty        float64 `json:"duty"`       // share of one worker the job keeps busy
	MemPeak     int64   `json:"mem_peak"`   // p95 of pod memory above idle during its runs
	CPUAvg      float64 `json:"cpu_avg"`    // cores while running (average)
	CPUP95      float64 `json:"cpu_p95"`
	NetPerRun   int64   `json:"net_per_run"`  // bytes in+out, average
	NetAvgBps   float64 `json:"net_avg_bps"`  // averaged over the schedule interval
	NetPeakBps  float64 `json:"net_peak_bps"` // average rate while a run is going (max over runs)
	InPerRun    int64   `json:"in_per_run"`
	OutPerRun   int64   `json:"out_per_run"`
	Warning     string  `json:"warning,omitempty"`
	InitialOnly bool    `json:"initial_only"` // only initial-load runs measured so far
}

type Sizing struct {
	Jobs        []JobSizing `json:"jobs"`
	BaselineMem int64       `json:"baseline_mem"`
	MaxParallel int         `json:"max_parallel"`
	Workers     int         `json:"workers"`
	Replicas    int         `json:"replicas"`
	MemRequest  int64       `json:"mem_request"`
	MemLimit    int64       `json:"mem_limit"`
	CPURequest  float64     `json:"cpu_request"`
	CPULimit    float64     `json:"cpu_limit"`
	NetAvgBps   float64     `json:"net_avg_bps"`
	NetPeakBps  float64     `json:"net_peak_bps"`
	Notes       []string    `json:"notes"`
	YAML        string      `json:"yaml"`
}

func p95(v []float64) float64 {
	if len(v) == 0 {
		return 0
	}
	s := append([]float64(nil), v...)
	sort.Float64s(s)
	return s[int(math.Ceil(0.95*float64(len(s))))-1]
}

func avg(v []float64) float64 {
	if len(v) == 0 {
		return 0
	}
	t := 0.0
	for _, x := range v {
		t += x
	}
	return t / float64(len(v))
}

func roundUpMi(b int64, step int64) int64 {
	mi := (b + (1 << 20) - 1) >> 20
	return ((mi + step - 1) / step) * step << 20
}

func (a *API) computeSizing(ctx context.Context) (*Sizing, error) {
	jobs, err := a.store.ListJobs(ctx)
	if err != nil {
		return nil, err
	}
	_, hist := a.res.Snapshot()
	var baseline int64
	for _, h := range hist {
		if baseline == 0 || h.MemBytes < baseline {
			baseline = h.MemBytes
		}
	}
	sz := &Sizing{BaselineMem: baseline, MaxParallel: a.cfg.MaxParallel}
	var sumDuty, memPeak, cpuP95, netAvg, netPeak float64
	for _, j := range jobs {
		runs, err := a.store.ListRuns(ctx, j.ID, 20)
		if err != nil {
			return nil, err
		}
		js := JobSizing{JobID: j.ID, Name: j.Name}
		if eff := a.eng.effectiveSchedule(j.Schedule); eff != "" && j.Enabled {
			if nx := a.eng.nextRuns(eff, 2, time.Now()); len(nx) == 2 {
				js.Interval = nx[1].Sub(nx[0]).Seconds()
			}
		}
		var durs, mems, cpus, nets, ins, outs, rates []float64
		// regular runs only: initial loads / reloads are not what the schedule has to fit, and runs
		// recorded before resource accounting have no memory/CPU data
		regular := 0
		for _, r := range runs {
			if r.Stats != nil && r.Stats.TablesReloaded == 0 && r.Stats.PeakMem > 0 {
				regular++
			}
		}
		for _, r := range runs {
			if r.StartedAt == nil || r.Stats == nil || (r.Status != "succeeded" && r.Status != "running") || r.Stats.PeakMem == 0 {
				continue
			}
			if regular > 0 && r.Stats.TablesReloaded > 0 {
				continue
			}
			if r.Stats.TablesReloaded > 0 {
				js.InitialOnly = true
			}
			end := time.Now()
			if r.FinishedAt != nil {
				end = *r.FinishedAt
			}
			d := end.Sub(*r.StartedAt).Seconds()
			if d <= 0 {
				continue
			}
			if r.Status == "running" {
				js.InProgress = true
			}
			js.Runs++
			durs = append(durs, d)
			mems = append(mems, float64(max(0, r.Stats.PeakMem-baseline)))
			cpus = append(cpus, r.Stats.CPUSeconds/d)
			n := float64(r.Stats.NetIn + r.Stats.NetOut)
			nets = append(nets, n)
			ins = append(ins, float64(r.Stats.NetIn))
			outs = append(outs, float64(r.Stats.NetOut))
			rates = append(rates, n/d)
		}
		if js.Runs > 0 {
			js.DurAvg, js.DurP95 = avg(durs), p95(durs)
			js.MemPeak = int64(p95(mems))
			js.CPUAvg, js.CPUP95 = avg(cpus), p95(cpus)
			js.NetPerRun, js.InPerRun, js.OutPerRun = int64(avg(nets)), int64(avg(ins)), int64(avg(outs))
			js.NetPeakBps = p95(rates)
			if js.Interval > 0 {
				js.Duty = js.DurP95 / js.Interval
				js.NetAvgBps = avg(nets) / js.Interval
			}
			switch {
			case js.InitialOnly:
				js.Duty = 0
				js.Warning = "only the initial load has been measured so far — the estimate appears after a regular run"
			case js.Duty >= 1:
				js.Warning = "a run takes longer than the interval — the next runs are skipped; increase the interval or enable the tail window"
			case js.Duty > 0.7:
				js.Warning = "a run takes over 70% of the interval — close to skipping runs"
			}
		}
		sumDuty += js.Duty
		memPeak = math.Max(memPeak, float64(js.MemPeak))
		cpuP95 = math.Max(cpuP95, js.CPUP95)
		netAvg += js.NetAvgBps
		netPeak = math.Max(netPeak, js.NetPeakBps)
		sz.Jobs = append(sz.Jobs, js)
	}

	// worker slots: expected busy workers with 50% headroom; replicas: at least 2 for availability
	sz.Workers = max(1, int(math.Ceil(sumDuty*1.5)))
	sz.Replicas = max(2, (sz.Workers+sz.MaxParallel-1)/sz.MaxParallel)
	// memory: idle baseline + every slot at the measured p95 peak; limit with 50% headroom
	if baseline == 0 {
		baseline = 32 << 20
	}
	perSlot := int64(math.Max(memPeak, 64<<20))
	sz.MemRequest = roundUpMi(baseline+int64(sz.MaxParallel)*perSlot, 64)
	sz.MemLimit = roundUpMi(int64(float64(sz.MemRequest)*1.5), 128)
	sz.CPURequest = math.Max(0.1, math.Ceil(float64(sz.MaxParallel)*cpuP95*10)/10)
	sz.CPULimit = math.Max(1, math.Ceil(sz.CPURequest*2))
	sz.NetAvgBps, sz.NetPeakBps = netAvg, netPeak*float64(sz.MaxParallel)

	measured := 0
	for _, js := range sz.Jobs {
		if js.Runs > 0 {
			measured++
		}
	}
	if measured < len(sz.Jobs) {
		sz.Notes = append(sz.Notes, fmt.Sprintf("%d of %d databases have no runs yet — the advice will be refined after them", len(sz.Jobs)-measured, len(sz.Jobs)))
	}
	sz.Notes = append(sz.Notes,
		"SQL Server does the heavy work (hashing), so the pod needs little CPU; memory peaks come from schema extract and publish (SqlPackage)",
		"the first load of big tables takes much longer than regular runs — do not size the interval by it")
	sz.YAML = fmt.Sprintf(`spec:
  replicas: %d
  template:
    spec:
      containers:
        - name: kamin-db-syncer
          env:
            - { name: MAX_PARALLEL, value: "%d" }
          resources:
            requests: { cpu: "%g", memory: %dMi }
            limits:   { cpu: "%g", memory: %dMi }`,
		sz.Replicas, sz.MaxParallel, sz.CPURequest, sz.MemRequest>>20, sz.CPULimit, sz.MemLimit>>20)
	return sz, nil
}

func (a *API) sizing(w http.ResponseWriter, r *http.Request) {
	sz, err := a.computeSizing(r.Context())
	if a.ok(w, err) {
		writeJSON(w, 200, sz)
	}
}

func (a *API) resources(w http.ResponseWriter, r *http.Request) {
	cur, hist := a.res.Snapshot()
	writeJSON(w, 200, map[string]any{"pod": a.cfg.Pod, "current": cur, "history": hist,
		"mem_limit": a.res.MemLimit, "cpu_limit": a.res.CPULimit})
}
