package git

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

type isolatedSubmittedObject struct {
	writer, proof, remote, main, submitted, landed string
}

func isolatedSubmittedObjectFixture(t *testing.T) isolatedSubmittedObject {
	t.Helper()
	writer, remote, main := initTestRepoWithRemote(t)
	proof := filepath.Join(t.TempDir(), "native-proof")
	// Clone before the worker creates the submitted commits: the native store
	// cannot accidentally inherit source objects through a local clone hardlink.
	vswlGit(t, writer, "clone", "--no-local", "--single-branch", "--branch", main, remote, proof)
	vswlGit(t, writer, "checkout", "-q", "-b", "operator/submitted")
	first := vswlCommitFile(t, writer, "first.txt", "first change\n", "first submitted change")
	submitted := vswlCommitFile(t, writer, "second.txt", "second change\n", "second submitted change")
	vswlGit(t, writer, "push", "-q", "origin", "operator/submitted")
	vswlGit(t, writer, "checkout", "-q", main)
	vswlCommitFile(t, writer, "unrelated.txt", "main advance\n", "independent main advance")
	vswlGit(t, writer, "cherry-pick", first, submitted)
	landed := vswlGit(t, writer, "rev-parse", "HEAD")
	vswlGit(t, writer, "push", "-q", "origin", main)
	return isolatedSubmittedObject{writer, proof, remote, main, submitted, landed}
}

func requireSubmittedObjectMissing(t *testing.T, repo, sha string) {
	t.Helper()
	if err := exec.Command("git", "-C", repo, "cat-file", "-e", sha+"^{commit}").Run(); err == nil {
		t.Fatalf("fixture unexpectedly contains original submitted object %s", sha)
	}
}

func submittedObjectCustody(t *testing.T, repo string) string {
	t.Helper()
	return vswlGit(t, repo, "show-ref") + "\nHEAD=" + vswlGit(t, repo, "rev-parse", "HEAD") +
		"\nSTATUS=" + vswlGit(t, repo, "status", "--porcelain") +
		"\nINDEX=" + vswlGit(t, repo, "diff", "--cached") +
		"\nWORKTREE=" + vswlGit(t, repo, "diff")
}

func TestEnsureSubmittedCommitRepairsIsolatedStoreWithoutChangingCustody(t *testing.T) {
	f := isolatedSubmittedObjectFixture(t)
	g := NewGit(f.proof)
	requireSubmittedObjectMissing(t, f.proof, f.submitted)
	if _, err := g.SubmittedWorkLandingReceipt("origin", f.main, f.submitted); err == nil {
		t.Fatal("legacy proof must fail before original object is available")
	}
	requireSubmittedObjectMissing(t, f.proof, f.submitted)
	if err := os.WriteFile(filepath.Join(f.proof, "README.md"), []byte("local staged work\n"), 0644); err != nil {
		t.Fatal(err)
	}
	vswlGit(t, f.proof, "add", "README.md")
	if err := os.WriteFile(filepath.Join(f.proof, "operator-notes.txt"), []byte("keep me\n"), 0644); err != nil {
		t.Fatal(err)
	}
	before := submittedObjectCustody(t, f.proof)
	fetchHead, err := os.ReadFile(filepath.Join(f.proof, ".git", "FETCH_HEAD"))
	if err != nil {
		t.Fatal(err)
	}
	if err := g.EnsureSubmittedCommit("origin", "operator/submitted", f.submitted); err != nil {
		t.Fatalf("obtain exact original object: %v", err)
	}
	if after := submittedObjectCustody(t, f.proof); after != before {
		t.Fatalf("object transfer changed checkout/index/refs\nbefore: %s\nafter: %s", before, after)
	}
	afterFetchHead, err := os.ReadFile(filepath.Join(f.proof, ".git", "FETCH_HEAD"))
	if err != nil || string(afterFetchHead) != string(fetchHead) {
		t.Fatalf("object transfer changed FETCH_HEAD: err=%v", err)
	}
	if notes, err := os.ReadFile(filepath.Join(f.proof, "operator-notes.txt")); err != nil || string(notes) != "keep me\n" {
		t.Fatalf("untracked work was changed: %q err=%v", notes, err)
	}
	// Acquisition is followed by the same real ancestry/patch proof, which now
	// returns the exact landed revision rather than treating the fetch as proof.
	if receipt, err := g.SubmittedWorkLandingReceipt("origin", f.main, f.submitted); err != nil || receipt != f.landed {
		t.Fatalf("receipt=%q err=%v, want verified landing %s", receipt, err, f.landed)
	}
}

func TestEnsureSubmittedCommitRejectsWrongBranchHeadAndMissingRemote(t *testing.T) {
	for _, shape := range []string{"wrong-branch", "missing-branch", "wrong-head", "missing-remote"} {
		t.Run(shape, func(t *testing.T) {
			f := isolatedSubmittedObjectFixture(t)
			branch := "operator/submitted"
			switch shape {
			case "wrong-branch":
				branch = f.main
			case "missing-branch":
				branch = "operator/missing"
			case "wrong-head":
				vswlGit(t, f.writer, "checkout", "-q", branch)
				vswlCommitFile(t, f.writer, "new-work.txt", "new unsubmitted work\n", "worker advances source")
				vswlGit(t, f.writer, "push", "-q", "origin", branch)
			case "missing-remote":
				vswlGit(t, f.proof, "remote", "set-url", "origin", filepath.Join(t.TempDir(), "missing.git"))
			}
			requireSubmittedObjectMissing(t, f.proof, f.submitted)
			before := submittedObjectCustody(t, f.proof)
			if err := NewGit(f.proof).EnsureSubmittedCommit("origin", branch, f.submitted); err == nil {
				t.Fatal("unavailable or mismatched source must fail closed")
			}
			requireSubmittedObjectMissing(t, f.proof, f.submitted)
			if after := submittedObjectCustody(t, f.proof); after != before {
				t.Fatalf("rejected acquisition changed custody\nbefore: %s\nafter: %s", before, after)
			}
		})
	}
}

func TestEnsureSubmittedCommitDoesNotReplaceLandingProof(t *testing.T) {
	f := isolatedSubmittedObjectFixture(t)
	// Restore the remote integration target to the initial state; the source is
	// available and authentic but its work has not landed on that target.
	initial := vswlGit(t, f.proof, "rev-parse", "HEAD")
	vswlGit(t, f.writer, "push", "-q", "--force", "origin", initial+":refs/heads/"+f.main)
	g := NewGit(f.proof)
	if err := g.EnsureSubmittedCommit("origin", "operator/submitted", f.submitted); err != nil {
		t.Fatalf("source should be obtainable: %v", err)
	}
	if receipt, err := g.SubmittedWorkLandingReceipt("origin", f.main, f.submitted); err == nil || receipt != "" {
		t.Fatalf("source acquisition is not landing: receipt=%q err=%v", receipt, err)
	}
}

func TestEnsureSubmittedCommitExistingObjectNeedsNoNetwork(t *testing.T) {
	f := isolatedSubmittedObjectFixture(t)
	existing := vswlGit(t, f.proof, "rev-parse", "HEAD")
	vswlGit(t, f.proof, "remote", "set-url", "origin", filepath.Join(t.TempDir(), "offline.git"))
	if err := NewGit(f.proof).EnsureSubmittedCommit("origin", "", existing); err != nil {
		t.Fatalf("existing exact object should not require source network access: %v", err)
	}
}

func TestEnsureSubmittedCommitUsesPushURL(t *testing.T) {
	f := isolatedSubmittedObjectFixture(t)
	vswlGit(t, f.proof, "remote", "set-url", "origin", filepath.Join(t.TempDir(), "wrong-fetch.git"))
	vswlGit(t, f.proof, "remote", "set-url", "--push", "origin", f.remote)
	if err := NewGit(f.proof).EnsureSubmittedCommit("origin", "operator/submitted", f.submitted); err != nil {
		t.Fatalf("exact submitted object must come from push URL: %v", err)
	}
	vswlGit(t, f.proof, "cat-file", "-e", f.submitted+"^{commit}")
}
