package asyncpreflight

// kasb-7huzb.41: a rebase that leaves the change identical changes the head
// SHA but not what was proven. The gate carries a PASSED receipt over to the
// new head when the change (merge-base(base, head)..head) of both heads has
// the same patch-id and the same changed-path set. Everything else (another
// source, a non-passing marker or receipt, a preflight already recorded for
// the new head, any git error, any difference) keeps the exact-head rule.

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

// CarryOver records a passed receipt accepted for an identical rebased change.
type CarryOver struct {
	From    string `json:"from_head"`
	To      string `json:"to_head"`
	PatchID string `json:"patch_id"`
}

// ChangeID identifies the change a head makes on top of its merge-base with
// the target branch.
type ChangeID struct {
	PatchID string // git patch-id --verbatim of the whole change
	Shape   string // sorted "oldmode newmode status path" lines
}

// ChangeIdentity computes the ChangeID of head against baseRef in repo.
// Replaced in tests.
var ChangeIdentity = changeIdentity

// changeIdentityTimeout bounds each git call.
const changeIdentityTimeout = 2 * time.Minute

var refRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._/-]*$`)

// RepoDir is the git repository the gate reads: the rig's shared .repo.git
// (polecat worktrees and the refinery clone share its objects), else
// mayor/rig.
func RepoDir(rigPath string) string {
	if st, err := os.Stat(filepath.Join(rigPath, ".repo.git")); err == nil && st.IsDir() {
		return filepath.Join(rigPath, ".repo.git")
	}
	return filepath.Join(rigPath, "mayor", "rig")
}

// BaseRef is the remote-tracking ref of the MR target ("origin/main" when
// unset).
func BaseRef(target string) (string, error) {
	target = strings.TrimSpace(target)
	if target == "" {
		target = "main"
	}
	if !refRe.MatchString(target) || strings.Contains(target, "..") {
		return "", fmt.Errorf("invalid target %q", target)
	}
	return "origin/" + target, nil
}

func gitEnv() []string {
	var env []string
	for _, kv := range os.Environ() {
		switch strings.SplitN(kv, "=", 2)[0] {
		case "GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE", "GIT_OBJECT_DIRECTORY", "GIT_ALTERNATE_OBJECT_DIRECTORIES", "GIT_EXTERNAL_DIFF":
			continue
		}
		env = append(env, kv)
	}
	return env
}

func runGit(repo string, stdin []byte, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), changeIdentityTimeout)
	defer cancel()
	c := exec.CommandContext(ctx, "git", append([]string{"-C", repo}, args...)...)
	c.Env = gitEnv()
	if stdin != nil {
		c.Stdin = bytes.NewReader(stdin)
	}
	var stderr bytes.Buffer
	c.Stderr = &stderr
	out, err := c.Output()
	if err != nil {
		return nil, fmt.Errorf("git %s: %v: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return out, nil
}

func changeIdentity(repo, baseRef, head string) (ChangeID, error) {
	if !shaRe.MatchString(head) {
		return ChangeID{}, fmt.Errorf("invalid head %q", head)
	}
	out, err := runGit(repo, nil, "rev-parse", "--verify", "--quiet", "--end-of-options", head+"^{commit}")
	if err != nil || strings.TrimSpace(string(out)) != head {
		return ChangeID{}, fmt.Errorf("commit %s not in %s: %v", short(head), repo, err)
	}
	out, err = runGit(repo, nil, "rev-parse", "--verify", "--quiet", "--end-of-options", baseRef+"^{commit}")
	base := strings.TrimSpace(string(out))
	if err != nil || !shaRe.MatchString(base) {
		return ChangeID{}, fmt.Errorf("base %s not in %s: %v", baseRef, repo, err)
	}
	out, err = runGit(repo, nil, "merge-base", base, head)
	mb := strings.TrimSpace(string(out))
	if err != nil || !shaRe.MatchString(mb) {
		return ChangeID{}, fmt.Errorf("no merge-base of %s and %s: %v", baseRef, short(head), err)
	}
	if mb == head {
		return ChangeID{}, fmt.Errorf("%s is already contained in %s (empty change)", short(head), baseRef)
	}
	raw, err := runGit(repo, nil, "diff-tree", "-r", "-z", "--raw", "--no-renames", "--no-abbrev", mb, head)
	if err != nil {
		return ChangeID{}, err
	}
	shape, err := parseRawShape(raw)
	if err != nil {
		return ChangeID{}, err
	}
	patch, err := runGit(repo, nil, "diff-tree", "-p", "--binary", "--full-index", "--no-renames", "--no-color",
		"--no-ext-diff", "--no-textconv", "--diff-algorithm=myers", mb, head)
	if err != nil {
		return ChangeID{}, err
	}
	if len(bytes.TrimSpace(patch)) == 0 {
		return ChangeID{}, fmt.Errorf("empty diff for %s", short(head))
	}
	// --verbatim: whitespace is significant (Python indentation), unlike the
	// default whitespace-stripping patch-id. It implies --stable.
	out, err = runGit(repo, patch, "patch-id", "--verbatim")
	if err != nil {
		return ChangeID{}, err
	}
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(lines) != 1 {
		return ChangeID{}, fmt.Errorf("git patch-id returned %d ids for %s", len(lines), short(head))
	}
	fields := strings.Fields(lines[0])
	if len(fields) == 0 || !shaRe.MatchString(fields[0]) {
		return ChangeID{}, fmt.Errorf("git patch-id output %q", lines[0])
	}
	return ChangeID{PatchID: fields[0], Shape: shape}, nil
}

// parseRawShape turns `git diff-tree -r -z --raw --no-renames` output into
// sorted "oldmode newmode status path" lines (blob ids dropped: they differ
// legitimately when main touched other parts of a changed file).
func parseRawShape(raw []byte) (string, error) {
	parts := strings.Split(strings.TrimSuffix(string(raw), "\x00"), "\x00")
	var entries []string
	for i := 0; i < len(parts); i++ {
		meta := parts[i]
		if meta == "" {
			continue
		}
		f := strings.Fields(strings.TrimPrefix(meta, ":"))
		if !strings.HasPrefix(meta, ":") || len(f) != 5 || i+1 >= len(parts) {
			return "", fmt.Errorf("unexpected diff-tree raw entry %q", meta)
		}
		i++
		entries = append(entries, f[0]+" "+f[1]+" "+f[4]+" "+parts[i])
	}
	if len(entries) == 0 {
		return "", fmt.Errorf("no changed paths")
	}
	sort.Strings(entries)
	return strings.Join(entries, "\n"), nil
}

// passedReceipt returns the receipt of a passed marker, or why it is not one.
func passedReceipt(m *Marker) (*Receipt, string) {
	if m.Status != StatusPassed {
		return nil, fmt.Sprintf("marker is %s, not passed", m.Status)
	}
	r, err := ReadReceipt(m.ReceiptCopy)
	if err != nil {
		return nil, fmt.Sprintf("marker says passed but its receipt copy is unreadable: %v", err)
	}
	if r.Head != m.Head || r.Source != m.Source || !Passing(r.Status) {
		return nil, fmt.Sprintf("receipt copy is %s for %s@%s, not a pass for %s@%s", r.Status, r.Source, short(r.Head), m.Source, short(m.Head))
	}
	return r, ""
}

// carryOver decides whether m's passed receipt covers newHead. It returns the
// carry-over and the receipt, or a non-empty refusal reason.
func carryOver(rigPath string, f MRFields, m *Marker, newHead, branchHead string) (*CarryOver, *Receipt, string) {
	if m.Source != f.Source || !shaRe.MatchString(m.Head) || !shaRe.MatchString(newHead) || m.Head == newHead {
		return nil, nil, "not a rebase of the same source"
	}
	if branchHead != "" && branchHead != newHead {
		return nil, nil, fmt.Sprintf("branch head %s is not the MR head %s", short(branchHead), short(newHead))
	}
	r, why := passedReceipt(m)
	if r == nil {
		return nil, nil, why
	}
	// A preflight recorded for the new head (running, failed or passed) is
	// authoritative; never let an older pass override it.
	if p, err := MarkerPath(rigPath, f.Source, newHead); err != nil {
		return nil, nil, err.Error()
	} else if _, err := os.Stat(p); err == nil {
		return nil, nil, fmt.Sprintf("a preflight exists for the new head (%s)", p)
	}
	base, err := BaseRef(f.Target)
	if err != nil {
		return nil, nil, err.Error()
	}
	repo := RepoDir(rigPath)
	oldID, err := ChangeIdentity(repo, base, m.Head)
	if err != nil {
		return nil, nil, fmt.Sprintf("patch-id of %s: %v", short(m.Head), err)
	}
	newID, err := ChangeIdentity(repo, base, newHead)
	if err != nil {
		return nil, nil, fmt.Sprintf("patch-id of %s: %v", short(newHead), err)
	}
	if oldID.PatchID == "" || oldID.PatchID != newID.PatchID {
		return nil, nil, fmt.Sprintf("change differs (patch-id %s vs %s)", short12(oldID.PatchID), short12(newID.PatchID))
	}
	if oldID.Shape != newID.Shape {
		return nil, nil, "changed-path set or file modes differ"
	}
	return &CarryOver{From: m.Head, To: newHead, PatchID: oldID.PatchID}, r, ""
}

func carriedResult(res GateResult, co *CarryOver, r *Receipt, m *Marker) GateResult {
	res.State, res.Carried = GatePassed, co
	res.Reason = fmt.Sprintf("receipt carried over: %s %s -> %s (patch-id %s); preflight %s (receipt %s)",
		m.Source, short12(co.From), short12(co.To), short12(co.PatchID), r.Status, m.ReceiptCopy)
	return res
}

func short12(s string) string {
	if len(s) > 12 {
		return s[:12]
	}
	return s
}

// Description fields recording a carry-over on the MR.
const (
	FieldCarriedFrom    = "preflight_receipt_carried_from"
	FieldCarriedTo      = "preflight_receipt_carried_to"
	FieldCarriedPatchID = "preflight_receipt_patch_id"
)

// HasCarryFields reports whether description already records co.
func HasCarryFields(description string, co *CarryOver) bool {
	got := map[string]string{}
	for _, line := range strings.Split(description, "\n") {
		if k, v, ok := strings.Cut(line, ":"); ok {
			got[strings.TrimSpace(k)] = strings.TrimSpace(v)
		}
	}
	return got[FieldCarriedFrom] == co.From && got[FieldCarriedTo] == co.To && got[FieldCarriedPatchID] == co.PatchID
}

// WithCarryFields returns description with exactly one set of carry-over
// fields for co (replacing any earlier set).
func WithCarryFields(description string, co *CarryOver) string {
	var kept []string
	for _, line := range strings.Split(description, "\n") {
		k, _, ok := strings.Cut(line, ":")
		switch strings.TrimSpace(k) {
		case FieldCarriedFrom, FieldCarriedTo, FieldCarriedPatchID:
			if ok {
				continue
			}
		}
		kept = append(kept, line)
	}
	return strings.TrimRight(strings.Join(kept, "\n"), "\n") +
		"\n" + FieldCarriedFrom + ": " + co.From +
		"\n" + FieldCarriedTo + ": " + co.To +
		"\n" + FieldCarriedPatchID + ": " + co.PatchID
}
