package asyncpreflight

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// carryRepo is a rig whose .repo.git holds:
//
//	base -- main (origin/main: an unrelated change)
//	  \       \-- rebased   (same change as old, on main)
//	   \       \-- changed  (a different change to the same file)
//	    \       \-- indented (old's change with different indentation)
//	     \       \-- extra   (old's change plus one more file)
//	      \-- old (the preflighted head)
type carryRepo struct {
	rig, repo                              string
	old, rebased, changed, indented, extra string
}

func gitT(t *testing.T, dir string, args ...string) string {
	t.Helper()
	c := exec.Command("git", append([]string{"-C", dir, "-c", "user.name=t", "-c", "user.email=t@example.com", "-c", "commit.gpgsign=false"}, args...)...)
	c.Env = append(gitEnv(), "GIT_CONFIG_GLOBAL="+os.DevNull, "GIT_CONFIG_NOSYSTEM=1")
	out, err := c.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

func commitFiles(t *testing.T, repo, parent string, files map[string]string, msg string) string {
	t.Helper()
	if parent != "" {
		gitT(t, repo, "checkout", "-q", "--detach", parent)
	}
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(repo, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		gitT(t, repo, "add", name)
	}
	gitT(t, repo, "commit", "-q", "-m", msg)
	return gitT(t, repo, "rev-parse", "HEAD")
}

const (
	appBase = "import os\n\n\ndef keep():\n    return 0\n"
	appOld  = "import os\n\n\ndef keep():\n    return 0\n\n\ndef f(x):\n    if x:\n        return 1\n    return 2\n"
	appNew  = "import os\n\n\ndef keep():\n    return 0\n\n\ndef f(x):\n    if x:\n        return 1\n    return 3\n"
	appInd  = "import os\n\n\ndef keep():\n    return 0\n\n\ndef f(x):\n    if x:\n        return 1\n        return 2\n"
)

func newCarryRepo(t *testing.T) carryRepo {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	rig := t.TempDir()
	repo := filepath.Join(rig, ".repo.git")
	gitT(t, rig, "init", "-q", repo)
	base := commitFiles(t, repo, "", map[string]string{"app.py": appBase, "other.txt": "one\n"}, "base")
	main := commitFiles(t, repo, base, map[string]string{"other.txt": "one\ntwo\n"}, "main moves on")
	gitT(t, repo, "update-ref", "refs/remotes/origin/main", main)
	r := carryRepo{rig: rig, repo: repo}
	r.old = commitFiles(t, repo, base, map[string]string{"app.py": appOld}, "add f")
	r.rebased = commitFiles(t, repo, main, map[string]string{"app.py": appOld}, "add f")
	r.changed = commitFiles(t, repo, main, map[string]string{"app.py": appNew}, "add f")
	r.indented = commitFiles(t, repo, main, map[string]string{"app.py": appInd}, "add f")
	r.extra = commitFiles(t, repo, main, map[string]string{"app.py": appOld, "more.txt": "x\n"}, "add f")
	return r
}

// marker writes a marker for (src, head) with the given status and receipt.
func (r carryRepo) marker(t *testing.T, head, status, receiptStatus string) string {
	t.Helper()
	req := request(r.rig, "/w", time.Now())
	req.Head = head
	m, path, _, err := Prepare(req)
	if err != nil {
		t.Fatal(err)
	}
	m.Status = status
	if status == StatusFailed {
		m.ReceiptStatus, m.Reason = receiptStatus, "api-tests: 1 failed"
	}
	if err := Save(path, m); err != nil {
		t.Fatal(err)
	}
	if receiptStatus != "" {
		writeReceipt(t, m.ReceiptCopy, head, src, receiptStatus)
	}
	return path
}

func TestCarryOverIdenticalRebaseIsAccepted(t *testing.T) {
	withDeadPIDs(t)
	r := newCarryRepo(t)
	path := r.marker(t, r.old, StatusPassed, "passed")

	// The MR was resubmitted with the rebased head but kept the old binding.
	f := MRFields{Source: src, CommitSHA: r.rebased, RunPath: path, Target: "main"}
	g := Evaluate(r.rig, f, r.rebased, time.Now())
	if g.State != GatePassed || !g.Landable() || g.Carried == nil {
		t.Fatalf("identical rebase refused: %+v", g)
	}
	if g.Carried.From != r.old || g.Carried.To != r.rebased || len(g.Carried.PatchID) != 40 {
		t.Fatalf("carry=%+v", g.Carried)
	}
	want := "receipt carried over: " + src + " " + r.old[:12] + " -> " + r.rebased[:12] + " (patch-id " + g.Carried.PatchID[:12] + ")"
	if !strings.HasPrefix(g.Reason, want) {
		t.Fatalf("reason %q, want prefix %q", g.Reason, want)
	}

	// The MR still advertises the old head; the lander passes the rebased
	// branch head ("moved" today).
	f = MRFields{Source: src, CommitSHA: r.old, RunPath: path}
	if g := Evaluate(r.rig, f, r.rebased, time.Now()); g.State != GatePassed || g.Carried == nil || g.Carried.To != r.rebased {
		t.Fatalf("moved identical rebase refused: %+v", g)
	}
	// The exact head is unchanged behaviour (no carry-over).
	if g := Evaluate(r.rig, f, r.old, time.Now()); g.State != GatePassed || g.Carried != nil {
		t.Fatalf("exact head: %+v", g)
	}
}

func TestCarryOverChangedPatchIsRefused(t *testing.T) {
	withDeadPIDs(t)
	r := newCarryRepo(t)
	path := r.marker(t, r.old, StatusPassed, "passed")
	for name, head := range map[string]string{"changed line": r.changed, "indentation only": r.indented, "extra file": r.extra} {
		t.Run(name, func(t *testing.T) {
			g := Evaluate(r.rig, MRFields{Source: src, CommitSHA: head, RunPath: path}, head, time.Now())
			if g.State != GateInvalid || g.Carried != nil || !strings.Contains(g.Reason, "receipt not carried over: change differs") {
				t.Fatalf("%+v", g)
			}
			g = Evaluate(r.rig, MRFields{Source: src, CommitSHA: r.old, RunPath: path}, head, time.Now())
			if g.State != GateMoved || g.Carried != nil {
				t.Fatalf("moved: %+v", g)
			}
		})
	}
	t.Run("another source", func(t *testing.T) {
		g := Evaluate(r.rig, MRFields{Source: "kasb-other.1", CommitSHA: r.rebased, RunPath: path}, r.rebased, time.Now())
		if g.State != GateInvalid || g.Carried != nil {
			t.Fatalf("%+v", g)
		}
	})
	t.Run("branch head is neither", func(t *testing.T) {
		g := Evaluate(r.rig, MRFields{Source: src, CommitSHA: r.rebased, RunPath: path}, r.changed, time.Now())
		if g.State != GateInvalid || g.Carried != nil {
			t.Fatalf("%+v", g)
		}
	})
	t.Run("a preflight recorded for the new head wins", func(t *testing.T) {
		r.marker(t, r.rebased, StatusFailed, "failed")
		g := Evaluate(r.rig, MRFields{Source: src, CommitSHA: r.rebased, RunPath: path}, r.rebased, time.Now())
		if g.State != GateInvalid || g.Carried != nil || !strings.Contains(g.Reason, "a preflight exists for the new head") {
			t.Fatalf("%+v", g)
		}
	})
}

func TestCarryOverNeverCarriesAFailedReceipt(t *testing.T) {
	withDeadPIDs(t)
	r := newCarryRepo(t)
	cases := map[string]struct{ status, receipt string }{
		"failed marker":                {StatusFailed, "failed"},
		"error marker":                 {StatusError, ""},
		"passed marker, failed copy":   {StatusPassed, "failed"},
		"passed marker, light receipt": {StatusPassed, "passed_light"},
		"passed marker, no copy":       {StatusPassed, ""},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			path := r.marker(t, r.old, tc.status, tc.receipt)
			t.Cleanup(func() { _ = os.Remove(path); _ = os.Remove(ReceiptCopyPath(path)) })
			for _, f := range []MRFields{
				{Source: src, CommitSHA: r.rebased, RunPath: path},
				{Source: src, CommitSHA: r.old, RunPath: path},
			} {
				g := Evaluate(r.rig, f, r.rebased, time.Now())
				if g.Landable() || g.Carried != nil || !g.NeedsFix() {
					t.Fatalf("%+v", g)
				}
			}
		})
	}
}

func TestCarryOverPatchIDErrorFailsClosed(t *testing.T) {
	withDeadPIDs(t)
	r := newCarryRepo(t)
	path := r.marker(t, r.old, StatusPassed, "passed")
	f := MRFields{Source: src, CommitSHA: r.rebased, RunPath: path}

	t.Run("injected error", func(t *testing.T) {
		old := ChangeIdentity
		t.Cleanup(func() { ChangeIdentity = old })
		ChangeIdentity = func(repo, base, head string) (ChangeID, error) {
			if head == r.rebased {
				return ChangeID{}, errors.New("boom")
			}
			return old(repo, base, head)
		}
		g := Evaluate(r.rig, f, r.rebased, time.Now())
		if g.State != GateInvalid || g.Carried != nil || !strings.Contains(g.Reason, "boom") {
			t.Fatalf("%+v", g)
		}
	})
	t.Run("old head missing from the repo", func(t *testing.T) {
		ghost := strings.Repeat("e", 40)
		gpath := r.marker(t, ghost, StatusPassed, "passed")
		g := Evaluate(r.rig, MRFields{Source: src, CommitSHA: r.rebased, RunPath: gpath}, r.rebased, time.Now())
		if g.State != GateInvalid || g.Carried != nil || !strings.Contains(g.Reason, "patch-id of eeeeeeeee") {
			t.Fatalf("%+v", g)
		}
	})
	t.Run("no base ref", func(t *testing.T) {
		g := Evaluate(r.rig, MRFields{Source: src, CommitSHA: r.rebased, RunPath: path, Target: "release"}, r.rebased, time.Now())
		if g.State != GateInvalid || g.Carried != nil {
			t.Fatalf("%+v", g)
		}
	})
	t.Run("no repository", func(t *testing.T) {
		if err := os.Rename(r.repo, r.repo+".gone"); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Rename(r.repo+".gone", r.repo) })
		g := Evaluate(r.rig, f, r.rebased, time.Now())
		if g.State != GateInvalid || g.Carried != nil {
			t.Fatalf("%+v", g)
		}
	})
	t.Run("head already on main", func(t *testing.T) {
		gitT(t, r.repo, "update-ref", "refs/remotes/origin/main", r.rebased)
		t.Cleanup(func() { gitT(t, r.repo, "update-ref", "refs/remotes/origin/main", r.rebased+"~1") })
		g := Evaluate(r.rig, f, r.rebased, time.Now())
		if g.State != GateInvalid || g.Carried != nil || !strings.Contains(g.Reason, "empty change") {
			t.Fatalf("%+v", g)
		}
	})
}

func TestCarryFieldsRoundTrip(t *testing.T) {
	co := &CarryOver{From: headA, To: headB, PatchID: strings.Repeat("c", 40)}
	desc := "branch: x\ncommit_sha: " + headB
	got := WithCarryFields(desc, co)
	if !HasCarryFields(got, co) || HasCarryFields(desc, co) {
		t.Fatal(got)
	}
	if again := WithCarryFields(got, co); again != got {
		t.Fatalf("not idempotent:\n%s\n---\n%s", got, again)
	}
	if strings.Count(WithCarryFields(got, &CarryOver{From: headB, To: headA, PatchID: "d"}), FieldCarriedFrom) != 1 {
		t.Fatal("duplicate fields")
	}
}
