package capacity

import (
	"reflect"
	"testing"
)

func TestDispatchHoldLabels(t *testing.T) {
	var defaults *SchedulerConfig
	if got := defaults.DispatchHoldLabels([]string{"demo", "dispatch:hold", "live-validation"}); !reflect.DeepEqual(got, []string{"dispatch:hold", "live-validation"}) {
		t.Fatalf("defaults: got %v", got)
	}
	if got := defaults.DispatchHoldLabels([]string{"demo", "ci"}); len(got) != 0 {
		t.Fatalf("unlabelled bead held: %v", got)
	}

	// dispatch:hold always holds, even when the configured list omits it
	// or is explicitly empty.
	for _, list := range [][]string{{"watchdog"}, {}} {
		cfg := &SchedulerConfig{AutoFeedExcludeLabels: list}
		if got := cfg.DispatchHoldLabel([]string{"dispatch:hold"}, nil); got != "dispatch:hold" {
			t.Errorf("list %v: dispatch:hold not held (got %q)", list, got)
		}
	}
	cfg := &SchedulerConfig{AutoFeedExcludeLabels: []string{"watchdog"}}
	if got := cfg.DispatchHoldLabel([]string{"watchdog"}, nil); got != "watchdog" {
		t.Errorf("configured label not held: %q", got)
	}
	if got := cfg.DispatchHoldLabel([]string{"live-validation"}, nil); got != "" {
		t.Errorf("label outside configured list held: %q", got)
	}
}

func TestDispatchHoldLabel_Override(t *testing.T) {
	var cfg *SchedulerConfig
	labels := []string{"live-validation", "dispatch:hold"}
	if got := cfg.DispatchHoldLabel(labels, []string{"live-validation"}); got != "dispatch:hold" {
		t.Fatalf("got %q, want the label that was not overridden", got)
	}
	if got := cfg.DispatchHoldLabel(labels, []string{"live-validation", "dispatch:hold"}); got != "" {
		t.Fatalf("fully overridden bead still held by %q", got)
	}
	f := &SlingContextFields{HoldOverride: " live-validation, dispatch:hold ,"}
	if got := f.HoldOverrideLabels(); !reflect.DeepEqual(got, []string{"live-validation", "dispatch:hold"}) {
		t.Fatalf("HoldOverrideLabels = %v", got)
	}
}
