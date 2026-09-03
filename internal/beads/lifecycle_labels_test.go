package beads

import "testing"

func TestAwaitingMergeLabelRoundTrip(t *testing.T) {
	issue := &Issue{Labels: []string{"gastown", AwaitingMergeLabel("bt-wisp-tgbaa")}}
	if !IsAwaitingMerge(issue) {
		t.Fatal("IsAwaitingMerge = false")
	}
	if got := AwaitingMergeMR(issue); got != "bt-wisp-tgbaa" {
		t.Fatalf("AwaitingMergeMR = %q", got)
	}
	if got := AwaitingMergeLabels(issue); len(got) != 1 {
		t.Fatalf("AwaitingMergeLabels = %v", got)
	}
	if !LabelsContainAwaitingMerge(issue.Labels) {
		t.Fatal("LabelsContainAwaitingMerge = false")
	}
}

func TestAwaitingMergeLabelEmptyMR(t *testing.T) {
	if got := AwaitingMergeLabel("  "); got != "awaiting-merge:unknown" {
		t.Fatalf("AwaitingMergeLabel(\"\") = %q", got)
	}
	issue := &Issue{Labels: []string{"awaiting-merge:unknown"}}
	if !IsAwaitingMerge(issue) {
		t.Fatal("an unknown-MR hold is still a hold")
	}
	if got := AwaitingMergeMR(issue); got != "" {
		t.Fatalf("AwaitingMergeMR = %q, want empty", got)
	}
}

func TestAwaitingMergeAbsent(t *testing.T) {
	if IsAwaitingMerge(nil) || IsAwaitingMerge(&Issue{Labels: []string{"gastown"}}) {
		t.Fatal("false positive")
	}
	if AwaitingMergeMR(&Issue{}) != "" {
		t.Fatal("expected empty MR id")
	}
}

func TestAwaitingMergeMultipleHolds(t *testing.T) {
	// Two holds means an earlier one was never released — both are reported so
	// the caller can remove the stale one.
	issue := &Issue{Labels: []string{AwaitingMergeLabel("bt-wisp-mhr53"), AwaitingMergeLabel("bt-wisp-tgbaa")}}
	if got := AwaitingMergeLabels(issue); len(got) != 2 {
		t.Fatalf("AwaitingMergeLabels = %v, want both holds", got)
	}
}
