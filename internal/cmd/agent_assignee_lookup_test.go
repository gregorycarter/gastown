package cmd

import "testing"

func TestCanonicalAgentAssignee_TownRoleUsesTrailingSlash(t *testing.T) {
	if got := canonicalAgentAssignee("deacon"); got != "deacon/" {
		t.Fatalf("canonicalAgentAssignee(deacon) = %q, want %q", got, "deacon/")
	}
}

func TestHookAssigneeCandidates_TownLevelAliases(t *testing.T) {
	got := hookAssigneeCandidates("deacon/")
	want := map[string]bool{
		"deacon":  true,
		"deacon/": true,
	}
	for _, candidate := range got {
		delete(want, candidate)
	}
	if len(want) != 0 {
		t.Fatalf("hookAssigneeCandidates(deacon/) missing aliases: %v (got %v)", want, got)
	}
}

func TestHookAssigneeCandidates_PolecatShorthand(t *testing.T) {
	got := hookAssigneeCandidates("gastown/polecats/rust")
	want := map[string]bool{
		"gastown/polecats/rust": true,
		"gastown/rust":          true,
	}
	for _, candidate := range got {
		delete(want, candidate)
	}
	if len(want) != 0 {
		t.Fatalf("hookAssigneeCandidates(gastown/polecats/rust) missing aliases: %v (got %v)", want, got)
	}
}
