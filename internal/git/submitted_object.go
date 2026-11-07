package git

import (
	"fmt"
	"strings"
	"time"
)

// EnsureSubmittedCommit obtains the original submitted object when an isolated
// worker used a different object store from native post-merge. It is not a
// landing proof: callers must still verify this exact commit against the target.
// Existing objects need no network lookup. Missing objects are fetched only
// after the recorded source branch advertises the exact submitted SHA; fetching
// that immutable SHA cannot silently substitute a newer source revision.
func (g *Git) EnsureSubmittedCommit(remote, branch, commit string) error {
	if len(commit) != 40 || !isHexString(commit) {
		return fmt.Errorf("submitted_object_unavailable: immutable full submitted commit SHA required")
	}
	if _, err := g.run("cat-file", "-e", commit+"^{commit}"); err == nil {
		return nil
	}
	branch = strings.TrimPrefix(strings.TrimSpace(branch), "refs/heads/")
	if branch == "" {
		return fmt.Errorf("submitted_object_unavailable: recorded source branch is missing")
	}
	if _, err := g.run("check-ref-format", "refs/heads/"+branch); err != nil {
		return fmt.Errorf("submitted_object_unavailable: invalid recorded source branch: %w", err)
	}
	pushTarget := g.pushTarget(remote)
	advertised, err := g.runWithTimeout(30*time.Second, "ls-remote", "--heads", pushTarget, "refs/heads/"+branch)
	if err != nil {
		return fmt.Errorf("submitted_object_unavailable: cannot read source branch %s/%s: %w", remote, branch, err)
	}
	if tip := parseLSRemoteTip(advertised, branch); tip != commit {
		return fmt.Errorf("submitted_object_unavailable: source branch %s/%s does not advertise submitted %s (tip %s)", remote, branch, shortSHA(commit), shortSHA(tip))
	}
	// Object transfer only: no checkout, source/tracking ref, tag or FETCH_HEAD
	// update. Source-branch retirement remains protected by its existing lease.
	if _, err := g.runWithTimeout(60*time.Second, "fetch", "--no-tags", "--no-write-fetch-head", "--no-recurse-submodules", "--refmap=", pushTarget, commit); err != nil {
		return fmt.Errorf("submitted_object_unavailable: cannot fetch submitted %s: %w", shortSHA(commit), err)
	}
	if _, err := g.run("cat-file", "-e", commit+"^{commit}"); err != nil {
		return fmt.Errorf("submitted_object_unavailable: fetched submitted object is not a commit: %w", err)
	}
	return nil
}
