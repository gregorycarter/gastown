package daemon

import (
	"testing"
	"time"
)

func newBackoffTestManager() (*ConvoyManager, *[]string) {
	var logs []string
	m := &ConvoyManager{
		logger: func(format string, args ...interface{}) {
			logs = append(logs, format)
		},
	}
	return m, &logs
}

func TestConvoyFeedBackoff_GrowsAndCaps(t *testing.T) {
	m, logs := newBackoffTestManager()

	m.recordFeedFailure("hq-cv-1", "no dispatchable issues")
	if !m.feedBackoffActive("hq-cv-1") {
		t.Fatal("convoy should be in backoff after a failed feed")
	}

	v, _ := m.feedBackoff.Load("hq-cv-1")
	if got := v.(*convoyFeedBackoff).delay; got != convoyFeedBackoffBase {
		t.Fatalf("first backoff = %v, want %v", got, convoyFeedBackoffBase)
	}

	// Each failure doubles, capped.
	want := convoyFeedBackoffBase
	for i := 0; i < 10; i++ {
		m.recordFeedFailure("hq-cv-1", "no dispatchable issues")
		want *= 2
		if want > convoyFeedBackoffMax {
			want = convoyFeedBackoffMax
		}
		v, _ := m.feedBackoff.Load("hq-cv-1")
		if got := v.(*convoyFeedBackoff).delay; got != want {
			t.Fatalf("backoff after %d failures = %v, want %v", i+2, got, want)
		}
	}

	if len(*logs) != 11 {
		t.Errorf("logged %d lines, want one per state change (11)", len(*logs))
	}
}

func TestConvoyFeedBackoff_ClearedBySuccess(t *testing.T) {
	m, _ := newBackoffTestManager()

	m.recordFeedFailure("hq-cv-1", "no dispatchable issues")
	m.clearFeedBackoff("hq-cv-1")

	if m.feedBackoffActive("hq-cv-1") {
		t.Fatal("a successful feed must clear the backoff")
	}
	m.recordFeedFailure("hq-cv-1", "no dispatchable issues")
	v, _ := m.feedBackoff.Load("hq-cv-1")
	if got := v.(*convoyFeedBackoff).delay; got != convoyFeedBackoffBase {
		t.Fatalf("backoff after a clear = %v, want the base %v", got, convoyFeedBackoffBase)
	}
}

func TestConvoyFeedBackoff_ExpiresAndIsPerConvoy(t *testing.T) {
	m, _ := newBackoffTestManager()

	m.feedBackoff.Store("hq-cv-expired", &convoyFeedBackoff{
		until: time.Now().Add(-time.Second),
		delay: convoyFeedBackoffBase,
	})
	if m.feedBackoffActive("hq-cv-expired") {
		t.Error("an expired backoff must not block a retry")
	}

	m.recordFeedFailure("hq-cv-1", "no dispatchable issues")
	if m.feedBackoffActive("hq-cv-2") {
		t.Error("backoff must be per convoy")
	}
}
