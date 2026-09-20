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
		err := validateHisnStartSource(s, "hisn/polecats/flint", SessionStartOptions{})
		if (err == nil) != tc.allowed {
			t.Fatalf("%+v: %v", tc, err)
		}
	}
	if validateHisnStartSource(nil, "hisn/polecats/flint", SessionStartOptions{}) == nil {
		t.Fatal("unknown source accepted")
	}
}

func TestHisnStartAllowsOnlyAdmittedMatchingMR(t *testing.T) {
	for _, problem := range []string{"", "other MR", "review", "operator", "rollout", "blocked", "closed", "reassigned", "unassigned", "no admission", "no head", "other branch", "other source", "ordinary restart"} {
		t.Run(problem, func(t *testing.T) {
			source := &beads.Issue{ID: "hisn-test", Status: "hooked", Assignee: "hisn/polecats/flint", Labels: []string{"awaiting-merge:hisn-wisp-mr"}}
			opts := SessionStartOptions{Issue: source.ID, PreserveBranch: "polecat/flint/hisn-test", PreserveHead: "verified-head", RecoveryMR: "hisn-wisp-mr", BeforeLaunch: func() error { return nil }}
			switch problem {
			case "other MR":
				source.Labels = append(source.Labels, "awaiting-merge:hisn-wisp-other")
			case "review":
				source.Labels = append(source.Labels, "needs-review")
			case "operator":
				source.Labels = append(source.Labels, "needs-operator")
			case "rollout":
				source.Labels = append(source.Labels, "needs-operator-rollout")
			case "blocked", "closed":
				source.Status = problem
			case "reassigned":
				source.Assignee = "Greg Carter"
			case "unassigned":
				source.Assignee = ""
			case "no admission":
				opts.BeforeLaunch = nil
			case "no head":
				opts.PreserveHead = ""
			case "other branch":
				opts.PreserveBranch = "polecat/opal/hisn-test"
			case "other source":
				opts.Issue = "hisn-other"
			case "ordinary restart":
				opts.RecoveryMR = ""
			}
			err := validateHisnStartSource(source, "hisn/polecats/flint", opts)
			if (err == nil) != (problem == "") {
				t.Fatalf("problem=%s: %v", problem, err)
			}
		})
	}
}
