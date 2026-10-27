package cmd

import "testing"

// kasb 27 Oct 2026: the Refinery landed kasb-5g6f.9.53 and deleted its branch
// before the polecat's gt done push; the source bead carried this reason.
const refineryCloseReason = "Merged in kasb-wisp-aashv3\nmerged e72ac925a39430d841f83f1c2af6ec1cb985fd70 via kasb-wisp-aashv3\ntarget_branch: main"

func TestRefineryLandingProofAcceptsRecordedMergeOnTarget(t *testing.T) {
	var gotTarget, gotSHA string
	reason, ok := refineryLandingProof(refineryCloseReason, "main", func(target, sha string) bool {
		gotTarget, gotSHA = target, sha
		return true
	})
	if !ok {
		t.Fatal("expected landing proof for a Refinery merge reason whose commit is on main")
	}
	if gotTarget != "main" || gotSHA != "e72ac925a39430d841f83f1c2af6ec1cb985fd70" {
		t.Fatalf("checked %s@%s, want main@e72ac925...", gotSHA, gotTarget)
	}
	if reason != "merged e72ac925a394 on main" {
		t.Fatalf("reason = %q", reason)
	}
}

func TestRefineryLandingProofRejectsMergeNotOnTarget(t *testing.T) {
	if _, ok := refineryLandingProof(refineryCloseReason, "main", func(string, string) bool { return false }); ok {
		t.Fatal("a recorded merge commit that is not on the target must not count as landed")
	}
}

func TestRefineryLandingProofRejectsNonMergeClosures(t *testing.T) {
	always := func(string, string) bool { return true }
	for _, reason := range []string{
		"",
		"Completed with no code changes (already fixed or already merged)",
		"superseded: landed as 9f338d6 via MR kasb-wisp-e417pw (Mayor)",
		"Merged in kasb-wisp-x",
	} {
		if _, ok := refineryLandingProof(reason, "main", always); ok {
			t.Errorf("reason %q must not count as a Refinery landing", reason)
		}
	}
}

func TestRefineryLandingProofUsesRecordedTargetBranch(t *testing.T) {
	var gotTarget string
	reason := "Merged in gt-wisp-1\nmerged abcdef1234567 via gt-wisp-1\ntarget_branch: integration/gt-epic"
	refineryLandingProof(reason, "main", func(target, _ string) bool { gotTarget = target; return true })
	if gotTarget != "integration/gt-epic" {
		t.Fatalf("target = %q, want integration/gt-epic", gotTarget)
	}
}
