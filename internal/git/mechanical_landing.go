package git

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"sort"
	"strings"
	"time"

	"github.com/steveyegge/gastown/internal/util"
)

// MechanicalRewrite describes what a lander rewrote mechanically when it landed
// a submission that did not merge cleanly (kasb-1dxro). Paths are files the
// lander regenerated wholesale (for example a migration-number index or prompt
// pins). Renames are whole-word literal substitutions it applied to file names
// and contents (for example a renumbered migration name).
type MechanicalRewrite struct {
	Paths   []string    `json:"paths"`
	Renames [][2]string `json:"renames"`
}

// MechanicalLandingReceipt proves that landed, a single-parent commit on the
// push target, is the submitted work merged onto landed's parent with only the
// declared mechanical rewrite applied. It returns landed on success.
//
// The proof merges submitted onto the parent with the ordinary three-way
// merge. Any conflict outside rw.Paths rejects. Then every path where that
// naive merge and landed differ must either be in rw.Paths, or be the naive
// file with rw.Renames substituted (in its name and its bytes). Anything else,
// such as a dropped or edited hunk, rejects. Missing evidence fails closed.
func (g *Git) MechanicalLandingReceipt(remote, branch, submitted, landed string, rw MechanicalRewrite) (string, error) {
	fail := func(format string, args ...any) (string, error) {
		return "", fmt.Errorf("mechanical_landing_unproven: "+format, args...)
	}
	for name, value := range map[string]string{"submitted": submitted, "landed": landed} {
		if len(value) != 40 || !isHexString(value) {
			return fail("%s must be an immutable full commit SHA", name)
		}
		if _, err := g.run("rev-parse", "--verify", value+"^{commit}"); err != nil {
			return fail("%s commit unavailable: %w", name, err)
		}
	}
	if len(rw.Paths) == 0 && len(rw.Renames) == 0 {
		return fail("empty mechanical rewrite record")
	}
	for _, pair := range rw.Renames {
		if pair[0] == "" || pair[1] == "" || pair[0] == pair[1] {
			return fail("invalid rename %q -> %q", pair[0], pair[1])
		}
	}
	if _, err := g.runWithTimeout(60*time.Second, "fetch", "--no-tags", g.pushTarget(remote), "refs/heads/"+branch); err != nil {
		return fail("cannot fetch push target: %w", err)
	}
	target, err := g.run("rev-parse", "--verify", "FETCH_HEAD^{commit}")
	if err != nil || len(target) != 40 || !isHexString(target) {
		return fail("cannot resolve fetched target: %v", err)
	}
	if onTarget, err := g.IsAncestor(landed, target); err != nil || !onTarget {
		return fail("landed commit is not on the fetched target")
	}
	parents, err := g.run("rev-list", "--parents", "-n", "1", landed)
	fields := strings.Fields(parents)
	if err != nil || len(fields) != 2 {
		return fail("landed commit must have exactly one parent")
	}
	parent := fields[1]
	if already, err := g.IsAncestor(submitted, parent); err != nil || already {
		return fail("landed commit's parent already contains the submitted work or ancestry is unavailable")
	}
	base, err := g.run("merge-base", "--all", submitted, parent)
	if err != nil || len(base) != 40 || !isHexString(base) {
		return fail("submission and landed parent require one unambiguous common base")
	}
	if merges, err := g.run("rev-list", "--min-parents=2", base+".."+submitted); err != nil || merges != "" {
		return fail("only a linear submitted series can be proved")
	}
	options := []string{"-c", "merge.default=text", "-c", "merge.renormalize=false", "-c", "merge.renames=false", "-c", "core.attributesFile=/dev/null"}
	args := append(append([]string{}, options...), "merge-tree", "--write-tree", "--name-only", "--no-messages", "--merge-base="+base, parent, submitted)
	outBytes, code, mergeErr := g.rawGit(60*time.Second, args...)
	if mergeErr != nil || (code != 0 && code != 1) {
		return fail("naive merge failed (exit %d): %v", code, mergeErr)
	}
	lines := strings.Split(strings.TrimSpace(string(outBytes)), "\n")
	if len(lines) == 0 || len(lines[0]) != 40 || !isHexString(lines[0]) {
		return fail("naive merge produced no tree")
	}
	naive := lines[0]
	mechanical := map[string]bool{}
	for _, p := range rw.Paths {
		mechanical[p] = true
	}
	for _, conflicted := range lines[1:] {
		if conflicted = strings.TrimSpace(conflicted); conflicted != "" && !mechanical[conflicted] {
			return fail("conflict outside the mechanical paths: %s", conflicted)
		}
	}
	landedTree, err := g.run("rev-parse", "--verify", landed+"^{tree}")
	if err != nil {
		return fail("landed tree unavailable: %w", err)
	}
	raw, err := g.run("diff", "--raw", "-z", "--no-renames", naive, landedTree)
	if err != nil {
		return fail("cannot compare naive merge with landed tree: %w", err)
	}
	type change struct{ status, path string }
	var changes []change
	if raw != "" {
		parts := strings.Split(strings.TrimSuffix(raw, "\x00"), "\x00")
		if len(parts)%2 != 0 {
			return fail("unexpected changed-path inventory")
		}
		for i := 0; i < len(parts); i += 2 {
			meta := strings.Fields(parts[i])
			if len(meta) < 5 || !strings.HasPrefix(meta[0], ":") {
				return fail("unexpected changed-path inventory")
			}
			changes = append(changes, change{status: meta[4][:1], path: parts[i+1]})
		}
	}
	added := map[string]bool{}
	deleted := map[string]bool{}
	for _, c := range changes {
		switch c.status {
		case "A":
			added[c.path] = true
		case "D":
			deleted[c.path] = true
		}
	}
	renamedFrom := map[string]string{}
	for p := range deleted {
		if n := SubstituteWords(p, rw.Renames); n != p {
			renamedFrom[n] = p
		}
	}
	for _, c := range changes {
		if mechanical[c.path] {
			continue
		}
		switch c.status {
		case "M", "T":
			if err := g.sameAfterRenames(naive, c.path, landedTree, c.path, rw.Renames); err != nil {
				return fail("%s: %v", c.path, err)
			}
		case "A":
			src, ok := renamedFrom[c.path]
			if !ok {
				return fail("%s was added by the landing but is not a declared rename", c.path)
			}
			if err := g.sameAfterRenames(naive, src, landedTree, c.path, rw.Renames); err != nil {
				return fail("%s (renamed from %s): %v", c.path, src, err)
			}
		case "D":
			n := SubstituteWords(c.path, rw.Renames)
			if n == c.path || !added[n] {
				return fail("%s was removed by the landing without a declared rename", c.path)
			}
		default:
			return fail("unsupported change %s on %s", c.status, c.path)
		}
	}
	if err := g.VerifyPushedCommitReachableFromPushTarget(remote, branch, target); err != nil {
		return fail("proved snapshot is no longer on the push target: %w", err)
	}
	return landed, nil
}

func (g *Git) sameAfterRenames(naiveTree, naivePath, landedTree, landedPath string, renames [][2]string) error {
	want, code, err := g.rawGit(30*time.Second, "cat-file", "blob", naiveTree+":"+naivePath)
	if err != nil || code != 0 {
		return fmt.Errorf("naive blob unavailable")
	}
	got, code, err := g.rawGit(30*time.Second, "cat-file", "blob", landedTree+":"+landedPath)
	if err != nil || code != 0 {
		return fmt.Errorf("landed blob unavailable")
	}
	if !bytes.Equal([]byte(SubstituteWords(string(want), renames)), got) {
		return fmt.Errorf("content differs beyond the declared renames")
	}
	return nil
}

// SubstituteWords replaces whole-word occurrences of each rename's old text in
// one left-to-right pass (longest match first), so a rename never chains into
// another. A word boundary is any byte outside [A-Za-z0-9_].
func SubstituteWords(text string, renames [][2]string) string {
	if len(renames) == 0 || text == "" {
		return text
	}
	sorted := append([][2]string{}, renames...)
	sort.SliceStable(sorted, func(i, j int) bool { return len(sorted[i][0]) > len(sorted[j][0]) })
	isWord := func(b byte) bool {
		return b == '_' || (b >= '0' && b <= '9') || (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z')
	}
	var out strings.Builder
	for i := 0; i < len(text); {
		matched := false
		if i == 0 || !isWord(text[i-1]) {
			for _, pair := range sorted {
				old := pair[0]
				end := i + len(old)
				if end <= len(text) && text[i:end] == old && (end == len(text) || !isWord(text[end])) {
					out.WriteString(pair[1])
					i = end
					matched = true
					break
				}
			}
		}
		if !matched {
			out.WriteByte(text[i])
			i++
		}
	}
	return out.String()
}

// rawGit runs git and returns untrimmed stdout and the exit code. A non-zero
// exit is not an error here; callers decide which codes are acceptable.
func (g *Git) rawGit(timeout time.Duration, args ...string) ([]byte, int, error) {
	if err := g.guardUnsafeTownRootMutation(args); err != nil {
		return nil, -1, err
	}
	if g.gitDir != "" {
		args = append([]string{"--git-dir=" + g.gitDir}, args...)
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "git", args...)
	util.SetDetachedProcessGroup(cmd)
	if g.workDir != "" {
		cmd.Dir = g.workDir
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if ctx.Err() == context.DeadlineExceeded {
		return nil, -1, fmt.Errorf("git %s timed out after %v", args[0], timeout)
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return stdout.Bytes(), exitErr.ExitCode(), nil
	}
	if err != nil {
		return nil, -1, err
	}
	return stdout.Bytes(), 0, nil
}
