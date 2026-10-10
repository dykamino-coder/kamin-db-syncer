package main

// Bandwidth limits in Mbit/s: per job (data.maxMbit) and per pod across all jobs (POD_MAX_MBIT).
// Incoming (from SQL Server source) and outgoing (to the target) are limited separately. The data
// path accounts every blob / cell set / statement it moves and waits on token buckets, and the
// adaptive batcher keeps one blob within ~2 s of the allowed rate so there are no long bursts.
// Schema extract/publish (SqlPackage) and git transfer only metadata and are not throttled.

import (
	"context"
	"os"
	"strconv"

	"golang.org/x/time/rate"
)

type limiterPair struct{ in, out *rate.Limiter }

func newLimiterPair(mbit float64) *limiterPair {
	if mbit <= 0 {
		return nil
	}
	bps := mbit * 1e6 / 8
	burst := int(max(bps/2, 64<<10))
	return &limiterPair{in: rate.NewLimiter(rate.Limit(bps), burst), out: rate.NewLimiter(rate.Limit(bps), burst)}
}

var podMaxMbit = func() float64 {
	f, _ := strconv.ParseFloat(os.Getenv("POD_MAX_MBIT"), 64)
	return f
}()

var podLimits = newLimiterPair(podMaxMbit)

func waitN(ctx context.Context, l *rate.Limiter, n int64) {
	for l != nil && n > 0 {
		c := min(n, int64(l.Burst()))
		if l.WaitN(ctx, int(c)) != nil {
			return
		}
		n -= c
	}
}

// in/out account bytes moved by the data sync and block until the limits allow more.
func (ds *dataSync) in(n int64) {
	ds.rc.stats.read.Add(n)
	ds.throttle(n, true)
}

func (ds *dataSync) throttle(n int64, incoming bool) {
	for _, p := range []*limiterPair{ds.limits, podLimits} {
		if p == nil {
			continue
		}
		if incoming {
			waitN(ds.rc.ctx, p.in, n)
		} else {
			waitN(ds.rc.ctx, p.out, n)
		}
	}
}

// effectiveMbit is the tightest limit that applies to this job (0 = unlimited).
func (ds *dataSync) effectiveMbit() float64 {
	m := ds.rc.job.Config.Data.MaxMbit
	if podMaxMbit > 0 && (m <= 0 || podMaxMbit < m) {
		m = podMaxMbit
	}
	return m
}

// maxBlob keeps one transfer within ~2 s of the allowed rate (and never above maxBlobBytes).
func (ds *dataSync) maxBlob() int {
	if m := ds.effectiveMbit(); m > 0 {
		return int(max(256<<10, min(float64(maxBlobBytes), m*1e6/8*2)))
	}
	return maxBlobBytes
}
