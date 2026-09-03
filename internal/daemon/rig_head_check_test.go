package daemon

import (
	"errors"
	"strings"
	"testing"
)

func TestRigHeadDriftReason(t *testing.T) {
	tests := []struct {
		name     string
		status   rigHeadStatus
		wantWarn bool
		contains string
	}{
		{"up to date", rigHeadStatus{Branch: "main", Behind: 0}, false, ""},
		{"within tolerance", rigHeadStatus{Branch: "main", Behind: 5}, false, ""},
		{"behind", rigHeadStatus{Branch: "main", Behind: 18}, true, "18 commits behind origin/main"},
		{"detached", rigHeadStatus{Branch: "HEAD", Detached: true}, true, "detached HEAD"},
		{"inspection failed", rigHeadStatus{Err: errors.New("not a git repo")}, false, ""},
		{"no origin/main ref", rigHeadStatus{Branch: "main", Err: errors.New("unknown revision")}, false, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := rigHeadDriftReason("bridge_town_core", tc.status, rigHeadMaxBehind)
			if tc.wantWarn && got == "" {
				t.Fatal("expected a warning, got none")
			}
			if !tc.wantWarn && got != "" {
				t.Fatalf("expected no warning, got %q", got)
			}
			if tc.contains != "" && !strings.Contains(got, tc.contains) {
				t.Errorf("warning %q missing %q", got, tc.contains)
			}
			if tc.wantWarn && !strings.Contains(got, "bridge_town_core") {
				t.Errorf("warning %q should name the rig", got)
			}
		})
	}
}

func TestInspectRigHeadMissingCheckout(t *testing.T) {
	st := inspectRigHead(t.TempDir() + "/absent")
	if st.Err == nil {
		t.Error("a missing checkout should report an error, not a clean status")
	}
	if rigHeadDriftReason("rig", st, rigHeadMaxBehind) != "" {
		t.Error("a missing checkout must not warn")
	}
}
