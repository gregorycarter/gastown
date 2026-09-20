package polecat

import (
	"github.com/steveyegge/gastown/internal/beads"
	"testing"
)

func TestHisnStartPreservesBlockedAndOperatorOwnedWork(t *testing.T) {
	for _, tc := range []struct {
		status, assignee, label string
		allowed                 bool
	}{
		{"blocked", "hisn/polecats/flint", "", false},
		{"in_progress", "Greg Carter", "", false},
		{"closed", "hisn/polecats/flint", "", false},
		{"in_progress", "", "awaiting-merge:hisn-wisp-existing", false},
		{"open", "hisn/polecats/flint", "needs-operator", false},
		{"hooked", "hisn/polecats/flint", "", true},
		{"in_progress", "hisn/polecats/flint", "", true},
		{"open", "", "", true},
	} {
		s := &beads.Issue{ID: "hisn-original", Status: tc.status, Assignee: tc.assignee, Labels: []string{tc.label}}
		err := validateHisnStartSource(s, "hisn/polecats/flint")
		if (err == nil) != tc.allowed {
			t.Fatalf("%+v: %v", tc, err)
		}
	}
	if validateHisnStartSource(nil, "hisn/polecats/flint") == nil {
		t.Fatal("unknown source accepted")
	}
}
