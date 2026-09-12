//go:build integration

// Per-test observation windows: beginObs prints an opening banner with the
// test's properties; the deferred report() prints a closing summary with the
// request window (first request sent -> last response received), a status
// histogram, a slow-request count, burst throughput, and CDC convergence
// timings. The test's own logs appear between the two. Field meanings are
// documented in acid_test_docs.md ("Observation Windows").
package tests

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// A normal request is ~10-50ms; above this it almost certainly burned through
// multiple server-side OCC retry cycles (each retry sleeps a random 5-100ms
// after losing a WriteConflict race on the wallet's sequence number).
const slowReqThreshold = 250 * time.Millisecond

const lineWidth = 80

type convergeInfo struct {
	phone     string
	want      int64
	took      time.Duration
	lastSeen  int64
	converged bool
}

type observation struct {
	mu      sync.Mutex
	name    string
	props   string
	started time.Time

	windowSet bool
	firstSent time.Time
	lastDone  time.Time

	total     int
	okCount   int
	failCount int
	slow      int
	statuses  map[int]int

	converges []convergeInfo
}

// The suite is sequential (no t.Parallel), so a single active observation is
// safe. Set by beginObs, read by the recording helpers.
var currentObs *observation

// beginObs prints the opening banner for a test with the given properties.
func beginObs(t *testing.T, props string) *observation {
	o := &observation{
		name:     t.Name(),
		props:    props,
		started:  time.Now(),
		statuses: make(map[int]int),
	}
	currentObs = o
	t.Logf("%s", bannerLine(o.name))
	t.Logf(" %s | started: %s", o.props, o.started.Format("15:04:05.000"))
	t.Logf("%s", strings.Repeat("-", lineWidth))
	return o
}

// report prints the closing summary. Defer it from the main test goroutine.
func (o *observation) report(t *testing.T) {
	t.Helper()
	o.mu.Lock()
	defer o.mu.Unlock()

	finished := time.Now()

	if o.windowSet {
		window := o.lastDone.Sub(o.firstSent)
		t.Logf(" window     : %s -> %s (%.2fs)",
			o.firstSent.Format("15:04:05.000"), o.lastDone.Format("15:04:05.000"), window.Seconds())
		t.Logf(" requests   : %d total | %d ok | %d failed %s",
			o.total, o.okCount, o.failCount, statusHistogram(o.statuses))
		t.Logf(" slow       : %d requests >%dms", o.slow, slowReqThreshold.Milliseconds())
		if o.okCount > 0 && window > 0 {
			t.Logf(" throughput : %.1f ok/s", float64(o.okCount)/window.Seconds())
		}
	} else {
		t.Logf(" requests   : none recorded")
	}

	for _, c := range o.converges {
		if c.converged {
			t.Logf(" cdc        : %s -> %d in %.1fs", c.phone, c.want, c.took.Seconds())
		} else {
			t.Logf(" cdc        : %s -> TIMEOUT (want %d, last seen %d)", c.phone, c.want, c.lastSeen)
		}
	}

	t.Logf("%s (finished: %s, total %.1fs)",
		strings.Repeat("=", lineWidth), finished.Format("15:04:05.000"), finished.Sub(o.started).Seconds())
	t.Logf("%s", strings.Repeat("=", lineWidth))

	// Close the observation: requests made before the next beginObs (e.g. a
	// funding deposit ahead of the window) must not be recorded anywhere.
	currentObs = nil
}

// recordReq records one deposit/withdraw/transfer round-trip. Goroutine-safe;
// no-ops when no observation is active.
func recordReq(status int, sent, done time.Time) {
	o := currentObs
	if o == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()

	o.total++
	if status >= 200 && status < 300 {
		o.okCount++
	} else {
		o.failCount++
	}
	o.statuses[status]++
	if done.Sub(sent) > slowReqThreshold {
		o.slow++
	}
	if !o.windowSet || sent.Before(o.firstSent) {
		o.firstSent = sent
	}
	if !o.windowSet || done.After(o.lastDone) {
		o.lastDone = done
	}
	o.windowSet = true
}

// recordConverge records one waitForBalance outcome. Goroutine-safe.
func recordConverge(phone string, want int64, took time.Duration, converged bool, lastSeen int64) {
	o := currentObs
	if o == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.converges = append(o.converges, convergeInfo{
		phone:     phone,
		want:      want,
		took:      took,
		lastSeen:  lastSeen,
		converged: converged,
	})
}

func statusHistogram(statuses map[int]int) string {
	if len(statuses) == 0 {
		return ""
	}
	codes := make([]int, 0, len(statuses))
	for c := range statuses {
		codes = append(codes, c)
	}
	sort.Ints(codes)
	parts := make([]string, 0, len(codes))
	for _, c := range codes {
		parts = append(parts, fmt.Sprintf("%d:%d", c, statuses[c]))
	}
	return "[" + strings.Join(parts, " ") + "]"
}

func bannerLine(name string) string {
	label := "[ " + name + " ]"
	pad := lineWidth - len(label)
	if pad < 2 {
		return label
	}
	left := pad / 2
	return strings.Repeat("=", left) + label + strings.Repeat("=", pad-left)
}
