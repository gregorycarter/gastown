package cmd

import (
	"errors"
	"strings"
)

// dispatchFailureClass separates "this bead is broken" from "the factory is
// busy or degraded". Only the first kind may consume the circuit breaker.
//
// Before this split, three Dolt hiccups nine minutes apart, or a bead whose
// respawn counter had run out, closed the sling context as "circuit-broken"
// and the bead vanished from the queue with no operator-visible trace — the
// mechanism behind 491 scheduler_dispatch_failed events with no dispatch.
type dispatchFailureClass int

const (
	// dispatchFailureWork is a genuine per-bead dispatch failure: the bead,
	// its formula, or its worktree is broken. Counts toward the breaker.
	dispatchFailureWork dispatchFailureClass = iota
	// dispatchFailureRespawnLimit is the per-bead respawn circuit breaker
	// firing. The bead is fine; its counter is exhausted. Needs
	// `gt sling respawn-reset`, not a closed context.
	dispatchFailureRespawnLimit
	// dispatchFailureCapacity is admission control or the per-rig polecat
	// directory cap. Pure back-pressure — retry next tick.
	dispatchFailureCapacity
	// dispatchFailureTransport is a bd/Dolt transport failure: connection
	// refused, timeout, database not found. Infrastructure, not the bead.
	dispatchFailureTransport
)

func (c dispatchFailureClass) String() string {
	switch c {
	case dispatchFailureRespawnLimit:
		return "respawn-limit"
	case dispatchFailureCapacity:
		return "capacity"
	case dispatchFailureTransport:
		return "bd-transport"
	default:
		return "work"
	}
}

// countsTowardCircuitBreaker reports whether a failure of this class should
// increment DispatchFailures.
func (c dispatchFailureClass) countsTowardCircuitBreaker() bool {
	return c == dispatchFailureWork
}

// transportFailureMarkers are substrings that identify a bd/Dolt transport
// failure. Matching on message text is unavoidable: bd is a subprocess and
// reports these only through stderr.
var transportFailureMarkers = []string{
	"connection refused",
	"connection reset",
	"broken pipe",
	"database not found",
	"unknown database",
	"i/o timeout",
	"context deadline exceeded",
	"deadline exceeded",
	"dial tcp",
	"timed out",
	"timeout",
	"bd not installed",
	"server is not responding",
	"too many connections",
}

// capacityFailureMarkers identify back-pressure rather than a broken bead.
var capacityFailureMarkers = []string{
	"polecat admission denied",
	"polecat directories (max",
	"capacity is full",
	"no capacity",
}

// classifyDispatchFailure buckets a dispatch error. Unknown errors are
// classified as work failures so a genuinely broken bead still trips the
// breaker.
func classifyDispatchFailure(err error) dispatchFailureClass {
	if err == nil {
		return dispatchFailureWork
	}

	var admissionErr *polecatCapacityAdmissionError
	if errors.As(err, &admissionErr) {
		return dispatchFailureCapacity
	}

	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "respawn limit reached") {
		return dispatchFailureRespawnLimit
	}
	for _, marker := range capacityFailureMarkers {
		if strings.Contains(msg, marker) {
			return dispatchFailureCapacity
		}
	}
	for _, marker := range transportFailureMarkers {
		if strings.Contains(msg, marker) {
			return dispatchFailureTransport
		}
	}
	return dispatchFailureWork
}
