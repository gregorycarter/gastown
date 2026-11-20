package git

import (
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"time"
)

// SubmittedSeriesLandingReceipt is an explicit final-net proof for a consolidated
// linear submission. preLanding must be the target revision immediately before
// the landing, never an intermediate source commit or a post-landing revision.
// The caller owns that boundary; the source base is derived, not caller-selected.
// Unlike the legacy ancestry/cherry proof, this proves the complete nonempty
// source-base -> submitted delta is contained in a fresh target snapshot. Earlier
// shared history is outside that delta. No worktree, index or source ref changes.
func (g *Git) SubmittedSeriesLandingReceipt(remote, branch, submitted, preLanding string) (string, error) {
	return g.submittedSeriesReceipt(remote, branch, submitted, preLanding, "")
}

// SubmittedSeriesLandedReceipt is the same strict final-net proof pinned to
// the exact landed commit (kasb-6fa8o). preLanding must strictly precede
// landed, and landed must be on the fetched target. The submitted delta
// (merge-base(submitted, preLanding)..submitted) must merge cleanly onto
// preLanding and give exactly landed's tree.
//
// This is the proof for one member of an atomic batch push: the fetched
// target also holds LATER members, and re-merging an earlier member's delta
// onto a tree that already has it plus adjacent later edits is not
// idempotent (duplicated hunks or conflicts). Proving each member against its
// own landing boundary is exact and stronger than comparing to the tip.
func (g *Git) SubmittedSeriesLandedReceipt(remote, branch, submitted, preLanding, landed string) (string, error) {
	if landed == "" {
		return "", fmt.Errorf("submitted_series_unproven: landed must be an immutable full commit SHA")
	}
	return g.submittedSeriesReceipt(remote, branch, submitted, preLanding, landed)
}

func (g *Git) submittedSeriesReceipt(remote, branch, submitted, preLanding, landed string) (string, error) {
	fail := func(format string, args ...any) (string, error) {
		return "", fmt.Errorf("submitted_series_unproven: "+format, args...)
	}
	commits := map[string]string{"submitted": submitted, "pre-landing": preLanding}
	if landed != "" {
		commits["landed"] = landed
	}
	for name, value := range commits {
		if len(value) != 40 || !isHexString(value) {
			return fail("%s must be an immutable full commit SHA", name)
		}
		if _, err := g.run("rev-parse", "--verify", value+"^{commit}"); err != nil {
			return fail("%s commit unavailable: %w", name, err)
		}
	}
	if _, err := g.runWithTimeout(60*time.Second, "fetch", "--no-tags", g.pushTarget(remote), "refs/heads/"+branch); err != nil {
		return fail("cannot fetch push target: %w", err)
	}
	target, err := g.run("rev-parse", "--verify", "FETCH_HEAD^{commit}")
	if err != nil || len(target) != 40 || !isHexString(target) {
		return fail("cannot resolve fetched target: %v", err)
	}
	// proved is the commit whose tree must equal the re-merged delta: the
	// fetched tip, or the exact landed commit when one is pinned.
	proved := target
	if landed != "" {
		onTarget, err := g.IsAncestor(landed, target)
		if err != nil || !onTarget {
			return fail("landed commit is not on the fetched target")
		}
		proved = landed
	}
	before, err := g.IsAncestor(preLanding, proved)
	if err != nil || !before || preLanding == proved {
		if landed != "" {
			return fail("pre-landing revision must strictly precede the landed commit")
		}
		return fail("pre-landing revision must strictly precede fetched target")
	}
	alreadyPresent, err := g.IsAncestor(submitted, preLanding)
	if err != nil || alreadyPresent {
		return fail("pre-landing revision already contains submitted work or ancestry is unavailable")
	}
	base, err := g.run("merge-base", "--all", submitted, preLanding)
	if err != nil || len(base) != 40 || !isHexString(base) {
		return fail("submission and pre-landing revision require one unambiguous common base")
	}
	merges, err := g.run("rev-list", "--min-parents=2", base+".."+submitted)
	if err != nil || merges != "" {
		return fail("only a linear submitted series can use final-net proof")
	}
	// Raw -z begins with metadata, so run's whitespace trim cannot damage a
	// filename beginning with whitespace. Disable rename folding to retain both
	// endpoints and check every changed path's effective merge attributes.
	raw, err := g.run("diff", "--raw", "-z", "--no-renames", base, submitted)
	if err != nil || raw == "" {
		return fail("submitted series has no provable nonempty final delta")
	}
	parts := strings.Split(strings.TrimSuffix(raw, "\x00"), "\x00")
	if len(parts)%2 != 0 {
		return fail("unexpected changed-path inventory")
	}
	paths := make([]string, 0, len(parts)/2)
	for i := 0; i < len(parts); i += 2 {
		if !strings.HasPrefix(parts[i], ":") || parts[i+1] == "" {
			return fail("unexpected changed-path inventory")
		}
		paths = append(paths, parts[i+1])
	}
	// A custom driver (including a union/ours strategy) can report success while
	// discarding source changes. Such configurations are outside this proof.
	configured, err := g.run("config", "--get-regexp", `^merge\..*\.(driver|recursive)$`)
	var exitErr *exec.ExitError
	if configured != "" || (err != nil && (!errors.As(err, &exitErr) || exitErr.ExitCode() != 1)) {
		return fail("custom merge-driver configuration is not accepted")
	}
	strategy, err := g.run("config", "--get", "merge.default")
	if (err == nil && strategy != "text") || (err != nil && (!errors.As(err, &exitErr) || exitErr.ExitCode() != 1)) {
		return fail("non-default merge strategy is not accepted")
	}
	options := []string{"--literal-pathspecs", "-c", "merge.default=text", "-c", "merge.renormalize=false", "-c", "merge.renames=false", "-c", "core.attributesFile=/dev/null"}
	trees := []string{base, submitted, target}
	if landed != "" {
		trees = []string{base, submitted, preLanding, landed}
	}
	for _, tree := range trees {
		for offset := 0; offset < len(paths); offset += 100 {
			end := min(offset+100, len(paths))
			args := append(append([]string{}, options...), "check-attr", "--source="+tree, "-z", "merge", "--")
			out, attrErr := g.runWithTimeout(30*time.Second, append(args, paths[offset:end]...)...)
			if attrErr != nil {
				return fail("merge attributes unavailable: %w", attrErr)
			}
			attributes := strings.Split(strings.TrimSuffix(out, "\x00"), "\x00")
			if len(attributes) != (end-offset)*3 {
				return fail("incomplete merge-attribute coverage")
			}
			for i := 0; i < len(attributes); i += 3 {
				if attributes[i] != paths[offset+i/3] || attributes[i+1] != "merge" || attributes[i+2] != "unspecified" {
					return fail("explicit merge attributes are not accepted for %q", paths[offset+i/3])
				}
			}
		}
	}
	onto := target
	if landed != "" {
		onto = preLanding
	}
	args := append(append([]string{}, options...), "merge-tree", "--write-tree", "--no-messages", "--merge-base="+base, onto, submitted)
	mergedTree, err := g.runWithTimeout(60*time.Second, args...)
	if err != nil || len(mergedTree) != 40 || !isHexString(mergedTree) {
		return fail("final submitted delta does not merge cleanly: %v", err)
	}
	provedTree, err := g.run("rev-parse", "--verify", proved+"^{tree}")
	if err != nil || mergedTree != provedTree {
		if landed != "" {
			return fail("landed commit is not exactly the submitted delta on the pre-landing revision")
		}
		return fail("fetched target is missing final submitted changes")
	}
	if err := g.VerifyPushedCommitReachableFromPushTarget(remote, branch, proved); err != nil {
		return fail("proved snapshot is no longer on the push target: %w", err)
	}
	return proved, nil
}
