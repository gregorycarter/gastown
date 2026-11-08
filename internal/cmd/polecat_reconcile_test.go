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

func TestReconcileMissingSourceFromExactBranchHook(t *testing.T) {
	for _, tc := range []struct {
		name                               string
		enabled                            bool
		source, worker, branch, hook, want string
	}{
		{"landed infrastructure source missing lifecycle history", true, "", "radrat", "polecat/radrat/kasb-23wa.4+mw7mm5m1", "kasb-23wa.4", "kasb-23wa.4"},
		{"default does not infer source", false, "", "radrat", "polecat/radrat/kasb-23wa.4+mw7mm5m1", "kasb-23wa.4", ""},
		{"existing custody remains authoritative", true, "kasb-original", "radrat", "polecat/radrat/kasb-23wa.4+mw7mm5m1", "kasb-23wa.4", "kasb-original"},
		{"replacement hook cannot supply source", true, "", "radrat", "polecat/radrat/kasb-23wa.4+mw7mm5m1", "kasb-other", ""},
		{"another worker branch cannot supply source", true, "", "atom", "polecat/radrat/kasb-23wa.4+mw7mm5m1", "kasb-23wa.4", ""},
		{"unknown branch fails closed", true, "", "radrat", "main", "kasb-23wa.4", ""},
		{"missing hook fails closed", true, "", "radrat", "polecat/radrat/kasb-23wa.4+mw7mm5m1", "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := reconcileSourceFromTerminalHook(tc.enabled, tc.source, tc.worker, tc.branch, tc.hook)
			if got != tc.want {
				t.Fatalf("source binding = %q, want %q", got, tc.want)
			}
		})
	}
}
