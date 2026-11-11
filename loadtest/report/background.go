package report

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"
)

// QueueDepth is one queue's messages in one background sample.
type QueueDepth struct {
	Ready   int64 `json:"ready"`
	Unacked int64 `json:"unacked"`
}

// BackgroundSample is one loadtest/background.py line (raw/background.jsonl):
// the broker's confirm counters and queue depths and the API's WAITING count.
// Phase is start (before k6), run, end (k6 done), settle (after k6, before
// the audit) or pause (admission stopped for the audit). A sample with Error
// is a recorded gap: it has no figures.
type BackgroundSample struct {
	TMs                   int64                 `json:"tMs"`
	Phase                 string                `json:"phase"`
	Error                 string                `json:"error,omitempty"`
	BrokerReceivedConfirm int64                 `json:"brokerReceivedConfirm,omitempty"`
	BrokerConfirmed       int64                 `json:"brokerConfirmed,omitempty"`
	Queues                map[string]QueueDepth `json:"queues,omitempty"`
	Waiting               int                   `json:"waiting"`
	WaitingValid          bool                  `json:"waitingValid"`
	WaitingObservedAtMs   int64                 `json:"waitingObservedAtMs,omitempty"`
}

// Background is the load around the measured requests in one window: what
// keeps running when k6 stops (every WAITING request goes round the retry
// tiers) and broker backpressure.
type Background struct {
	Samples int `json:"samples"`
	Gaps    int `json:"gaps"` // samples that could not be read
	// PublishesNotConfirmed: publishes that asked for a confirm minus those
	// confirmed positively, from the last good sample before the window to
	// the last good one in it. Nacks (a full queue under reject-publish), plus
	// or minus at most one in-flight publish per publisher at either end, so
	// a quiet window can read slightly negative.
	PublishesNotConfirmed int64 `json:"publishesNotConfirmed"`
	// QueuePeak: the highest ready+unacked per queue in the window.
	QueuePeak map[string]int64 `json:"queuePeak"`
	// WaitingFirst/Last: WAITING requests in the window's first and last
	// samples whose workflow walk was valid; null if none was.
	WaitingFirst *int `json:"waitingFirst"`
	WaitingLast  *int `json:"waitingLast"`
}

// ReadBackground reads the samples file (JSON lines), oldest first.
func ReadBackground(path string) ([]BackgroundSample, error) {
	f, err := os.Open(path) //nolint:gosec // the run directory's own file
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	var out []BackgroundSample
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if strings.TrimSpace(sc.Text()) == "" {
			continue
		}
		var s BackgroundSample
		if err := json.Unmarshal(sc.Bytes(), &s); err != nil {
			return nil, fmt.Errorf("report: %s: %w", path, err)
		}
		out = append(out, s)
	}
	slices.SortStableFunc(out, func(a, b BackgroundSample) int { return int(a.TMs - b.TMs) })
	return out, sc.Err()
}

// summariseBackground: window's samples, measured from base (the last good
// sample before the window) or, with none, from the window's first good one.
func summariseBackground(window []BackgroundSample, base *BackgroundSample) Background {
	out := Background{Samples: len(window), QueuePeak: map[string]int64{}}
	var first, last *BackgroundSample
	if base != nil {
		first = base
	}
	for i := range window {
		s := &window[i]
		if s.Error != "" {
			out.Gaps++
			continue
		}
		if first == nil {
			first = s
		}
		last = s
		for q, d := range s.Queues {
			out.QueuePeak[q] = max(out.QueuePeak[q], d.Ready+d.Unacked)
		}
		if s.WaitingValid {
			n := s.Waiting
			if out.WaitingFirst == nil {
				out.WaitingFirst = &n
			}
			out.WaitingLast = &n
		}
	}
	if first != nil && last != nil && last != first {
		out.PublishesNotConfirmed = (last.BrokerReceivedConfirm - first.BrokerReceivedConfirm) - (last.BrokerConfirmed - first.BrokerConfirmed)
	}
	return out
}

// lastGoodBefore is the last good sample before t, or nil.
func lastGoodBefore(samples []BackgroundSample, t time.Time) *BackgroundSample {
	var base *BackgroundSample
	for i := range samples {
		if samples[i].Error == "" && time.UnixMilli(samples[i].TMs).Before(t) {
			base = &samples[i]
		}
	}
	return base
}

func phases(samples []BackgroundSample, names ...string) []BackgroundSample {
	var out []BackgroundSample
	for _, s := range samples {
		if slices.Contains(names, s.Phase) {
			out = append(out, s)
		}
	}
	return out
}

// backgroundFigures: the run (start, run and end samples), after it (settle
// and pause, from the end sample) and the pause sample itself.
func backgroundFigures(samples []BackgroundSample) map[string]any {
	var end, pause *BackgroundSample
	for i := range samples {
		switch {
		case samples[i].Error != "":
		case samples[i].Phase == "end":
			end = &samples[i]
		case samples[i].Phase == "pause":
			pause = &samples[i]
		}
	}
	return map[string]any{
		"run":      summariseBackground(phases(samples, "start", "run", "end"), nil),
		"afterRun": summariseBackground(phases(samples, "settle", "pause"), end),
		"atPause":  pause,
	}
}

// stepBackground: the run samples in w, from the last good sample before it.
func stepBackground(samples []BackgroundSample, w Window) Background {
	var in []BackgroundSample
	for _, s := range samples {
		if at := time.UnixMilli(s.TMs); s.Phase == "run" && !at.Before(w.From) && !at.After(w.To) {
			in = append(in, s)
		}
	}
	return summariseBackground(in, lastGoodBefore(samples, w.From))
}
