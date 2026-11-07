package cmd

import (
	"github.com/steveyegge/gastown/internal/beads"
	"testing"
)

func TestReconcileTerminalSourceHook(t *testing.T) {
	for _, tc := range []struct {
		name                       string
		enabled                    bool
		hook, source, status, want string
	}{
		{"explicit exact terminal source", true, "kasb-old", "kasb-old", "closed", "kasb-old"},
		{"default retains legacy hook", false, "kasb-old", "kasb-old", "closed", ""},
		{"open source retained", true, "kasb-old", "kasb-old", "open", ""},
		{"hooked source retained", true, "kasb-old", "kasb-old", "hooked", ""},
		{"unknown source retained", true, "kasb-old", "kasb-old", "", ""},
		{"unrelated replacement retained", true, "kasb-new", "kasb-old", "closed", ""},
		{"empty hook unchanged", true, "", "kasb-old", "closed", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := reconcileTerminalSourceHook(tc.enabled, &beads.AgentFields{HookBead: tc.hook}, &polecatReconcileReport{SourceIssue: tc.source, SourceIssueStatus: tc.status})
			if got != tc.want {
				t.Fatalf("hook exemption %q want%q", got, tc.want)
			}
		})
	}
}
