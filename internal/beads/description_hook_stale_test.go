package beads

import "testing"

func TestDescriptionHookStale(t *testing.T) {
	const me = "kasb_core/polecats/deathclaw"
	working := &AgentFields{AgentState: "working", HookBead: "kasb-x"}
	cases := []struct {
		name   string
		fields *AgentFields
		issue  *Issue
		want   bool
	}{
		{"hooked to another polecat", working, &Issue{Status: "hooked", Assignee: "kasb_core/polecats/scavenger"}, true},
		{"parked, unassigned", working, &Issue{Status: "open"}, true},
		{"mine", working, &Issue{Status: "hooked", Assignee: me}, false},
		{"mine, short address", working, &Issue{Status: "in_progress", Assignee: "kasb_core/deathclaw"}, false},
		{"landed", working, &Issue{Status: "closed", Assignee: "kasb_core/polecats/scavenger"}, false},
		{"spawning", &AgentFields{AgentState: "spawning", HookBead: "kasb-x"}, &Issue{Status: "open"}, false},
		{"unreadable", working, nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := DescriptionHookStale(tc.fields, tc.issue, me); got != tc.want {
				t.Fatalf("DescriptionHookStale = %v, want %v", got, tc.want)
			}
		})
	}
}
