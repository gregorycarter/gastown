package git

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type submittedSeriesFixture struct {
	dir, remote, branch, base, submitted, preLanding string
}

func newSubmittedSeriesFixture(t *testing.T) submittedSeriesFixture {
	t.Helper()
	dir, remote, branch := initTestRepoWithRemote(t)
	base := vswlCommitFile(t, dir, "feature.txt", "original\n", "shared starting point")
	vswlGit(t, dir, "checkout", "-q", "-b", "submitted")
	vswlCommitFile(t, dir, "feature.txt", "draft\n", "intermediate work")
	vswlCommitFile(t, dir, "feature.txt", "final\n", "amend intermediate work")
	submitted := vswlCommitFile(t, dir, "required.txt", "required source work\n", "second required change")
	vswlGit(t, dir, "checkout", "-q", branch)
	preLanding := vswlCommitFile(t, dir, "unrelated.txt", "independent main work\n", "main advances before landing")
	vswlGit(t, dir, "push", "-q", "origin", branch)
	return submittedSeriesFixture{dir, remote, branch, base, submitted, preLanding}
}

func (f submittedSeriesFixture) squash(t *testing.T) string {
	t.Helper()
	vswlGit(t, f.dir, "merge", "--squash", f.submitted)
	vswlGit(t, f.dir, "commit", "-q", "-m", "land final submitted series")
	return vswlGit(t, f.dir, "rev-parse", "HEAD")
}

func (f submittedSeriesFixture) push(t *testing.T) string {
	t.Helper()
	vswlGit(t, f.dir, "push", "-q", "origin", f.branch)
	return vswlGit(t, f.dir, "rev-parse", "HEAD")
}

func requireSubmittedSeriesRejected(t *testing.T, g *Git, branch, submitted, preLanding string) {
	t.Helper()
	receipt, err := g.SubmittedSeriesLandingReceipt("origin", branch, submitted, preLanding)
	if err == nil || receipt != "" {
		t.Fatalf("receipt=%q err=%v; unproven series must return an error and no receipt", receipt, err)
	}
}

func TestSubmittedSeriesLandingReceipt_AcceptsSquashedAmendmentsAndTargetAdvance(t *testing.T) {
	f := newSubmittedSeriesFixture(t)
	f.squash(t)
	vswlCommitFile(t, f.dir, "later.txt", "later independent change\n", "target advances after landing")
	target := f.push(t)
	g := NewGit(f.dir)
	// This is the causal production shape: intermediate draft patches are not
	// individually present, but all final submitted changes are on the target.
	if _, err := g.SubmittedWorkLandingReceipt("origin", f.branch, f.submitted); err == nil {
		t.Fatal("legacy per-commit proof unexpectedly accepted this squash fixture")
	}
	vswlGit(t, f.dir, "update-ref", "refs/remotes/origin/"+f.branch, f.submitted)
	receipt, err := g.SubmittedSeriesLandingReceipt("origin", f.branch, f.submitted, f.preLanding)
	if err != nil || receipt != target {
		t.Fatalf("receipt=%q err=%v, want exact fresh target %s", receipt, err, target)
	}
}

func TestSubmittedSeriesLandingReceipt_RejectsMissingRevertedAndConflictingChanges(t *testing.T) {
	for _, state := range []string{"missing", "partial", "reverted", "conflicting"} {
		t.Run(state, func(t *testing.T) {
			f := newSubmittedSeriesFixture(t)
			switch state {
			case "missing":
				vswlCommitFile(t, f.dir, "later.txt", "no source work\n", "only unrelated work lands")
			case "partial":
				vswlCommitFile(t, f.dir, "feature.txt", "final\n", "omit required.txt")
			case "reverted":
				landed := f.squash(t)
				vswlGit(t, f.dir, "revert", "--no-edit", landed)
			case "conflicting":
				vswlCommitFile(t, f.dir, "required.txt", "required source work\n", "land one required path")
				vswlCommitFile(t, f.dir, "feature.txt", "conflicting target choice\n", "conflict with final source")
			}
			f.push(t)
			requireSubmittedSeriesRejected(t, NewGit(f.dir), f.branch, f.submitted, f.preLanding)
		})
	}
}

func TestSubmittedSeriesLandingReceipt_RejectsNetZeroSeries(t *testing.T) {
	f := newSubmittedSeriesFixture(t)
	vswlGit(t, f.dir, "checkout", "-q", "-b", "net-zero", f.base)
	vswlCommitFile(t, f.dir, "feature.txt", "temporary\n", "temporary source work")
	submitted := vswlCommitFile(t, f.dir, "feature.txt", "original\n", "undo all source work")
	vswlGit(t, f.dir, "checkout", "-q", f.branch)
	vswlCommitFile(t, f.dir, "later.txt", "unrelated\n", "target advances")
	f.push(t)
	requireSubmittedSeriesRejected(t, NewGit(f.dir), f.branch, submitted, f.preLanding)
}

func TestSubmittedSeriesLandingReceipt_RejectsSourceMerge(t *testing.T) {
	f := newSubmittedSeriesFixture(t)
	vswlGit(t, f.dir, "checkout", "-q", "-b", "source-side", f.base)
	vswlCommitFile(t, f.dir, "side.txt", "merged source work\n", "side branch")
	vswlGit(t, f.dir, "checkout", "-q", "submitted")
	vswlGit(t, f.dir, "merge", "--no-ff", "-m", "merge source-side history", "source-side")
	f.submitted = vswlGit(t, f.dir, "rev-parse", "HEAD")
	vswlGit(t, f.dir, "checkout", "-q", f.branch)
	f.squash(t)
	f.push(t)
	requireSubmittedSeriesRejected(t, NewGit(f.dir), f.branch, f.submitted, f.preLanding)
}

func TestSubmittedSeriesLandingReceipt_RejectsAmbiguousSourceBase(t *testing.T) {
	dir, _, branch := initTestRepoWithRemote(t)
	base := vswlGit(t, dir, "rev-parse", "HEAD")
	vswlGit(t, dir, "checkout", "-q", "-b", "left")
	left := vswlCommitFile(t, dir, "left.txt", "left\n", "left ancestor")
	vswlGit(t, dir, "checkout", "-q", "-b", "right", base)
	right := vswlCommitFile(t, dir, "right.txt", "right\n", "right ancestor")
	vswlGit(t, dir, "checkout", "-q", "left")
	vswlGit(t, dir, "merge", "--no-ff", "-m", "first cross merge", "right")
	combinedTree := vswlGit(t, dir, "rev-parse", "HEAD^{tree}")
	// Reversing the parents creates a genuine criss-cross graph whose two
	// common bases cannot be replaced by an arbitrarily selected single base.
	otherMerge := vswlGit(t, dir, "commit-tree", combinedTree, "-p", right, "-p", left, "-m", "second cross merge")
	submitted := vswlCommitFile(t, dir, "feature.txt", "source work\n", "submitted work")
	vswlGit(t, dir, "checkout", "-q", "-B", branch, otherMerge)
	preLanding := vswlCommitFile(t, dir, "target.txt", "target work\n", "prelanding target")
	if bases := strings.Fields(vswlGit(t, dir, "merge-base", "--all", submitted, preLanding)); len(bases) != 2 {
		t.Fatalf("fixture needs two merge bases; got %v", bases)
	}
	vswlCommitFile(t, dir, "feature.txt", "source work\n", "land complete source content")
	vswlGit(t, dir, "push", "-q", "origin", branch)
	requireSubmittedSeriesRejected(t, NewGit(dir), branch, submitted, preLanding)
}

func TestSubmittedSeriesLandingReceipt_RejectsInvalidPreLandingIdentity(t *testing.T) {
	f := newSubmittedSeriesFixture(t)
	f.squash(t)
	target := f.push(t)
	for name, preLanding := range map[string]string{"target": target, "source": f.submitted,
		"missing": strings.Repeat("0", 40), "empty": ""} {
		t.Run(name, func(t *testing.T) {
			requireSubmittedSeriesRejected(t, NewGit(f.dir), f.branch, f.submitted, preLanding)
		})
	}
	// A valid commit from another history is not a prelanding snapshot of this target.
	vswlGit(t, f.dir, "checkout", "-q", "-b", "wrong-base", f.preLanding)
	wrong := vswlCommitFile(t, f.dir, "wrong.txt", "not on target\n", "wrong prelanding branch")
	requireSubmittedSeriesRejected(t, NewGit(f.dir), f.branch, f.submitted, wrong)
}

func TestSubmittedSeriesLandingReceipt_RejectsPreLandingAlreadyContainingSubmitted(t *testing.T) {
	f := newSubmittedSeriesFixture(t)
	vswlGit(t, f.dir, "merge", "--no-ff", "-m", "source already on prelanding target", f.submitted)
	preLanding := vswlGit(t, f.dir, "rev-parse", "HEAD")
	vswlCommitFile(t, f.dir, "later.txt", "later\n", "advance after already-landed source")
	f.push(t)
	requireSubmittedSeriesRejected(t, NewGit(f.dir), f.branch, f.submitted, preLanding)
}

func TestSubmittedSeriesLandingReceipt_RejectsStaleLocalAndWrongTargetProof(t *testing.T) {
	f := newSubmittedSeriesFixture(t)
	f.squash(t)
	good := f.push(t)
	vswlGit(t, f.dir, "checkout", "-q", "-b", "wrong-target", f.preLanding)
	vswlCommitFile(t, f.dir, "wrong.txt", "no source changes\n", "wrong target advances")
	vswlGit(t, f.dir, "push", "-q", "origin", "wrong-target")
	requireSubmittedSeriesRejected(t, NewGit(f.dir), "wrong-target", f.submitted, f.preLanding)
	// The actual remote is incomplete while a local tracking ref claims the
	// complete squash landed. A receipt must derive from the fresh remote.
	vswlGit(t, f.dir, "push", "-q", "--force", "origin", "HEAD:refs/heads/"+f.branch)
	vswlGit(t, f.dir, "update-ref", "refs/remotes/origin/"+f.branch, good)
	requireSubmittedSeriesRejected(t, NewGit(f.dir), f.branch, f.submitted, f.preLanding)
}

func TestSubmittedSeriesLandingReceipt_UsesPushURL(t *testing.T) {
	f := newSubmittedSeriesFixture(t)
	f.squash(t)
	pushRemote := filepath.Join(t.TempDir(), "push.git")
	vswlGit(t, f.dir, "init", "--bare", pushRemote)
	vswlGit(t, f.dir, "remote", "set-url", "--push", "origin", pushRemote)
	target := f.push(t)
	// The fetch URL still has only the prelanding target.
	if fetched := vswlGit(t, f.remote, "rev-parse", "refs/heads/"+f.branch); fetched != f.preLanding {
		t.Fatalf("fixture fetch target=%s, want prelanding=%s", fetched, f.preLanding)
	}
	receipt, err := NewGit(f.dir).SubmittedSeriesLandingReceipt("origin", f.branch, f.submitted, f.preLanding)
	if err != nil || receipt != target {
		t.Fatalf("receipt=%q err=%v, want actual push target %s", receipt, err, target)
	}
}

func TestSubmittedSeriesLandingReceipt_RejectsCustomMergeDriverFalseNoop(t *testing.T) {
	f := newSubmittedSeriesFixture(t)
	vswlCommitFile(t, f.dir, "required.txt", "required source work\n", "land second path")
	vswlCommitFile(t, f.dir, "feature.txt", "conflicting target choice\n", "source intent missing")
	target := f.push(t)
	vswlGit(t, f.dir, "config", "merge.discard.driver", "true")
	if err := os.WriteFile(filepath.Join(f.dir, ".git", "info", "attributes"), []byte("feature.txt merge=discard\n"), 0644); err != nil {
		t.Fatal(err)
	}
	// A naive tree comparison would pass: the custom driver ignores the
	// conflicting source content and reports success with the target unchanged.
	merged := vswlGit(t, f.dir, "merge-tree", "--write-tree", "--merge-base="+f.base, target, f.submitted)
	if tree := vswlGit(t, f.dir, "rev-parse", target+"^{tree}"); merged != tree {
		t.Fatalf("fixture needs false no-op under custom driver; got %q, want %s", merged, tree)
	}
	requireSubmittedSeriesRejected(t, NewGit(f.dir), f.branch, f.submitted, f.preLanding)
}

func TestSubmittedSeriesLandingReceipt_RejectsMergeAttributes(t *testing.T) {
	for _, location := range []string{"tracked", "local-info"} {
		t.Run(location, func(t *testing.T) {
			f := newSubmittedSeriesFixture(t)
			if location == "tracked" {
				vswlGit(t, f.dir, "checkout", "-q", "submitted")
				f.submitted = vswlCommitFile(t, f.dir, ".gitattributes", "feature.txt merge=union\n", "custom source merge semantics")
				vswlGit(t, f.dir, "checkout", "-q", f.branch)
			}
			f.squash(t)
			f.push(t)
			if location == "local-info" {
				if err := os.WriteFile(filepath.Join(f.dir, ".git", "info", "attributes"), []byte("feature.txt merge=union\n"), 0644); err != nil {
					t.Fatal(err)
				}
			}
			requireSubmittedSeriesRejected(t, NewGit(f.dir), f.branch, f.submitted, f.preLanding)
		})
	}
}

// kasb-6fa8o: an atomic batch where member B edits within diff context of
// member A's change. Re-merging either member's delta onto the final batch tip
// fails (A duplicates/conflicts, B conflicts), although each landed exactly.
// Pinning each member to its own landing boundary proves both.
func TestSubmittedSeriesLandedReceipt_ProvesAdjacentBatchMembers(t *testing.T) {
	dir, _, branch := initTestRepoWithRemote(t)
	baseContent := "class R:\n    a = 1\n    b = 2\n\n    def run(self):\n        pass\n\n    def stop(self):\n        pass\n"
	base := vswlCommitFile(t, dir, "r.py", baseContent, "batch base")
	vswlGit(t, dir, "push", "-q", "origin", branch)
	vswlGit(t, dir, "checkout", "-q", "-b", "member-a")
	a := vswlCommitFile(t, dir, "r.py", strings.Replace(baseContent, "    def stop(self):", "    def on_err(self):\n        pass\n\n    def stop(self):", 1), "member A adds a method")
	vswlGit(t, dir, "checkout", "-q", branch)
	vswlGit(t, dir, "checkout", "-q", "-b", "member-b")
	b := vswlCommitFile(t, dir, "r.py", strings.Replace(baseContent, "        pass\n\n    def stop", "        pass\n    c = 3\n    d = 4\n\n    def stop", 1), "member B adds fields above it")
	vswlGit(t, dir, "checkout", "-q", branch)
	vswlGit(t, dir, "cherry-pick", a)
	landedA := vswlGit(t, dir, "rev-parse", "HEAD")
	vswlGit(t, dir, "cherry-pick", b)
	landedB := vswlGit(t, dir, "rev-parse", "HEAD")
	vswlGit(t, dir, "push", "-q", "origin", branch)
	g := NewGit(dir)

	// The tip-based proof falsely fails both members (the 20 Nov epp6d/f24a8 shape).
	requireSubmittedSeriesRejected(t, g, branch, a, base)
	requireSubmittedSeriesRejected(t, g, branch, b, base)

	if got, err := g.SubmittedSeriesLandedReceipt("origin", branch, a, base, landedA); err != nil || got != landedA {
		t.Fatalf("member A: receipt=%q err=%v, want %s", got, err, landedA)
	}
	if got, err := g.SubmittedSeriesLandedReceipt("origin", branch, b, landedA, landedB); err != nil || got != landedB {
		t.Fatalf("member B: receipt=%q err=%v, want %s", got, err, landedB)
	}

	// Still strict: wrong landed tree, a boundary that does not strictly precede
	// the landed commit, a landed commit not on the target, or no landed SHA.
	for name, args := range map[string][3]string{
		"landed tree is not the member's delta": {a, base, landedB},
		"pre-landing equals landed":             {b, landedB, landedB},
		"landed not on target":                  {b, base, b},
		"member already in pre-landing":         {a, landedA, landedB},
		"empty landed":                          {a, base, ""},
	} {
		if got, err := g.SubmittedSeriesLandedReceipt("origin", branch, args[0], args[1], args[2]); err == nil || got != "" {
			t.Fatalf("%s: receipt=%q err=%v, want rejection", name, got, err)
		}
	}
}

// kasb-6fa8o follow-up: the lander composes the batch in its own clone and
// pushes it; the proving repository (the rig's .repo.git) has the pre-landing
// and landed commits only after fetching the target. 20 Nov 09:58-10:13Z every
// pinned proof failed with "pre-landing commit unavailable".
func TestSubmittedSeriesLandedReceipt_FetchesLanderCommitsFirst(t *testing.T) {
	dir, remote, branch := initTestRepoWithRemote(t)
	baseContent := "one\ntwo\nthree\n"
	base := vswlCommitFile(t, dir, "f.txt", baseContent, "base")
	vswlGit(t, dir, "push", "-q", "origin", branch)
	vswlGit(t, dir, "checkout", "-q", "-b", "member-a")
	a := vswlCommitFile(t, dir, "a.txt", "a\n", "member A")
	vswlGit(t, dir, "push", "-q", "origin", "member-a")
	vswlGit(t, dir, "checkout", "-q", "-b", "member-b", base)
	b := vswlCommitFile(t, dir, "b.txt", "b\n", "member B")
	vswlGit(t, dir, "push", "-q", "origin", "member-b")

	// The lander's own clone composes and pushes the batch.
	lander := t.TempDir()
	vswlGit(t, lander, "clone", "-q", remote, ".")
	vswlGit(t, lander, "config", "user.email", "lander@example.test")
	vswlGit(t, lander, "config", "user.name", "lander")
	vswlGit(t, lander, "checkout", "-q", branch)
	vswlGit(t, lander, "cherry-pick", a)
	landedA := vswlGit(t, lander, "rev-parse", "HEAD")
	vswlGit(t, lander, "cherry-pick", b)
	landedB := vswlGit(t, lander, "rev-parse", "HEAD")
	vswlGit(t, lander, "push", "-q", "origin", branch)

	g := NewGit(dir)
	if _, err := g.run("cat-file", "-e", landedA+"^{commit}"); err == nil {
		t.Fatal("fixture: the proving repository must not have the lander's commits yet")
	}
	if got, err := g.SubmittedSeriesLandedReceipt("origin", branch, a, base, landedA); err != nil || got != landedA {
		t.Fatalf("member A: receipt=%q err=%v", got, err)
	}
	if got, err := g.SubmittedSeriesLandedReceipt("origin", branch, b, landedA, landedB); err != nil || got != landedB {
		t.Fatalf("member B: receipt=%q err=%v", got, err)
	}
}
