// Package artifact implements policy-driven cleanup of generated lifecycle artifacts.
// It is deliberately filesystem-only: callers discover lifecycle state and pass it
// in as SafetyState, while the engine enforces path and deletion invariants.
package artifact

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	ModeDryRun = "dry-run"
	ModeApply  = "apply"
	// CIHookProtocolVersion is written by participating runner start hooks and
	// checked exactly before runner-mediated maintenance is authorized.
	CIHookProtocolVersion = "mkdir-v1"
)

// DefaultPaths are generated, reproducible directories. They are matched only
// at the cleanup root; nested CI paths must be explicitly configured.
var DefaultPaths = []string{
	"target", ".next", "coverage", "dist", ".pytest_cache", ".ruff_cache",
	".mypy_cache", ".pnpm-store",
}

// DefaultProtectedPaths cannot be cleaned merely by putting them in Paths.
// A rig-level AllowProtectedPaths entry is also required. The two-key model
// prevents a broad town policy from opting project data into deletion.
var PermanentProtectedPaths = []string{
	".git", ".repo.git", ".beads", ".dolt-*", "secrets", ".env*",
	".ci-maintenance.lock", ".ci-protocol-mutex", ".maintenance-active", ".ci-active-jobs",
	".job-starting", ".job-active", ".handoff-active", ".ci-job-hooks-installed",
	"_actions", "_temp", "_work", "_tool",
}

var OverridableProtectedPaths = []string{
	"data", "ml/models", "seif_ingestion/checkpoints*",
}

var DefaultProtectedPaths = append(append([]string(nil), PermanentProtectedPaths...), OverridableProtectedPaths...)

// PolicyConfig is the JSON-facing, mergeable policy. Pointer scalar fields
// distinguish an omitted value from an explicit false/zero override.
type PolicyConfig struct {
	Enabled             *bool    `json:"enabled,omitempty"`
	Mode                string   `json:"mode,omitempty"`
	Paths               []string `json:"paths,omitempty"`
	ProtectedPaths      []string `json:"protected_paths,omitempty"`
	AllowProtectedPaths []string `json:"allow_protected_paths,omitempty"`
	MaxAge              string   `json:"max_age,omitempty"`
	MaxBytes            *int64   `json:"max_bytes,omitempty"`
	OnPolecatReuse      *bool    `json:"on_polecat_reuse,omitempty"`
	OnPostMerge         *bool    `json:"on_post_merge,omitempty"`
	OnCIMaintenance     *bool    `json:"on_ci_maintenance,omitempty"`
}

// Policy is a resolved policy ready for execution.
type Policy struct {
	Enabled             bool
	Mode                string
	Paths               []string
	ProtectedPaths      []string
	AllowProtectedPaths []string
	MaxAge              time.Duration
	MaxBytes            int64
	OnPolecatReuse      bool
	OnPostMerge         bool
	OnCIMaintenance     bool
}

// DefaultPolicy is conservative: useful build paths are known, but automatic
// cleanup is disabled and manual execution remains a dry run.
func DefaultPolicy() Policy {
	return Policy{
		Mode:           ModeDryRun,
		Paths:          append([]string(nil), DefaultPaths...),
		ProtectedPaths: append([]string(nil), DefaultProtectedPaths...),
	}
}

// ResolvePolicy merges defaults, town settings, then rig settings. Protected
// defaults are additive and cannot be removed; protected overrides are accepted
// only from the rig policy, never from a town-wide policy.
func ResolvePolicy(town, rig *PolicyConfig) (Policy, error) {
	p := DefaultPolicy()
	applyConfig := func(c *PolicyConfig, allowProtectedOverride bool) error {
		if c == nil {
			return nil
		}
		if c.Enabled != nil {
			p.Enabled = *c.Enabled
		}
		if c.Mode != "" {
			p.Mode = c.Mode
		}
		if c.Paths != nil {
			p.Paths = append([]string(nil), c.Paths...)
		}
		if c.ProtectedPaths != nil {
			p.ProtectedPaths = appendUnique(p.ProtectedPaths, c.ProtectedPaths...)
		}
		if allowProtectedOverride && c.AllowProtectedPaths != nil {
			p.AllowProtectedPaths = append([]string(nil), c.AllowProtectedPaths...)
		}
		if c.MaxAge != "" {
			d, err := time.ParseDuration(c.MaxAge)
			if err != nil || d < 0 {
				return fmt.Errorf("invalid cleanup max_age %q", c.MaxAge)
			}
			p.MaxAge = d
		}
		if c.MaxBytes != nil {
			p.MaxBytes = *c.MaxBytes
		}
		if c.OnPolecatReuse != nil {
			p.OnPolecatReuse = *c.OnPolecatReuse
		}
		if c.OnPostMerge != nil {
			p.OnPostMerge = *c.OnPostMerge
		}
		if c.OnCIMaintenance != nil {
			p.OnCIMaintenance = *c.OnCIMaintenance
		}
		return nil
	}
	if err := applyConfig(town, false); err != nil {
		return Policy{}, err
	}
	if err := applyConfig(rig, true); err != nil {
		return Policy{}, err
	}
	if err := p.Validate(); err != nil {
		return Policy{}, err
	}
	return p, nil
}

// Validate checks policy values before any filesystem traversal.
func (p Policy) Validate() error {
	if p.Mode != "" && p.Mode != ModeDryRun && p.Mode != ModeApply {
		return fmt.Errorf("invalid cleanup mode %q (want dry-run or apply)", p.Mode)
	}
	if p.MaxAge < 0 {
		return errors.New("cleanup max_age must not be negative")
	}
	if p.MaxBytes < 0 {
		return errors.New("cleanup max_bytes must not be negative")
	}
	if len(p.Paths) == 0 {
		return errors.New("cleanup paths allowlist must not be empty")
	}
	for _, path := range p.Paths {
		if err := ValidateRelativePattern(path); err != nil {
			return err
		}
	}
	for _, path := range p.ProtectedPaths {
		if err := validateProtectedPattern(path); err != nil {
			return err
		}
	}
	for _, path := range p.AllowProtectedPaths {
		if err := ValidateRelativePattern(path); err != nil {
			return fmt.Errorf("invalid protected-path override: %w", err)
		}
		if !eligibleProtectedOverride(path) {
			return fmt.Errorf("protected-path override %q is not an overridable business-data path", path)
		}
	}
	return nil
}

func validateProtectedPattern(pattern string) error {
	normalized := filepath.ToSlash(pattern)
	if pattern == "" || filepath.IsAbs(pattern) || filepath.VolumeName(pattern) != "" || path.Clean(normalized) != normalized || normalized == "." {
		return fmt.Errorf("unsafe protected cleanup path %q", pattern)
	}
	for _, part := range strings.Split(normalized, "/") {
		if part == "" || part == "." || part == ".." {
			return fmt.Errorf("unsafe protected cleanup path %q", pattern)
		}
		if i := strings.IndexAny(part, "*?["); i == 0 {
			return fmt.Errorf("protected cleanup path %q has an unanchored component", pattern)
		}
	}
	if strings.Contains(pattern, "**") {
		return fmt.Errorf("recursive protected cleanup glob is not allowed: %q", pattern)
	}
	if _, err := path.Match(normalized, normalized); err != nil {
		return fmt.Errorf("invalid protected cleanup pattern %q: %w", pattern, err)
	}
	return nil
}

func eligibleProtectedOverride(path string) bool {
	for _, permanent := range PermanentProtectedPaths {
		if patternsIntersect(path, permanent) {
			return false
		}
	}
	for _, overridable := range OverridableProtectedPaths {
		if protectedPatternCoversOverride(overridable, path) {
			return true
		}
	}
	return false
}

func protectedPatternCoversOverride(protected, override string) bool {
	protected = strings.ToLower(filepath.ToSlash(protected))
	override = strings.ToLower(filepath.ToSlash(override))
	// The override must equal or descend from a path selected by the protected
	// pattern. Walking only override ancestors deliberately rejects broader
	// parents such as "ml" for the protected "ml/models" subtree.
	for current := override; current != ""; {
		if matched, _ := path.Match(protected, current); matched {
			return true
		}
		cut := strings.LastIndex(current, "/")
		if cut < 0 {
			break
		}
		current = current[:cut]
	}
	return false
}

// ValidateRelativePattern rejects paths that could select the root or escape it.
// Globs are supported, but must be anchored by a literal first component.
func ValidateRelativePattern(pattern string) error {
	if pattern == "" {
		return errors.New("cleanup path must not be empty")
	}
	if filepath.IsAbs(pattern) || filepath.VolumeName(pattern) != "" {
		return fmt.Errorf("cleanup path %q must be relative", pattern)
	}
	normalized := filepath.ToSlash(pattern)
	clean := path.Clean(normalized)
	if clean != normalized || clean == "." || clean == "/" {
		return fmt.Errorf("unsafe cleanup path %q", pattern)
	}
	parts := strings.Split(normalized, "/")
	for _, part := range parts {
		if part == "" || part == "." || part == ".." {
			return fmt.Errorf("unsafe cleanup path %q", pattern)
		}
	}
	if strings.ContainsAny(parts[0], "*?[") {
		return fmt.Errorf("cleanup path %q must have a literal first component", pattern)
	}
	if strings.Contains(pattern, "**") {
		return fmt.Errorf("recursive cleanup glob is not allowed: %q", pattern)
	}
	if _, err := path.Match(normalized, normalized); err != nil {
		return fmt.Errorf("invalid cleanup pattern %q: %w", pattern, err)
	}
	return nil
}

// SafetyState contains lifecycle facts discovered by the caller.
type SafetyState struct {
	Dirty               bool     `json:"dirty"`
	ActiveMR            bool     `json:"active_mr"`
	MRVerified          bool     `json:"mr_verified"`
	ActiveRunner        bool     `json:"active_runner"`
	RunnerHooksVerified bool     `json:"runner_hooks_verified"`
	RunnerVerified      bool     `json:"runner_verified"`
	Reasons             []string `json:"reasons,omitempty"`
}

func (s SafetyState) refusalReasons() []string {
	reasons := append([]string(nil), s.Reasons...)
	if s.Dirty {
		reasons = append(reasons, "dirty-worktree")
	}
	if s.ActiveMR {
		reasons = append(reasons, "active-merge-request")
	}
	if s.ActiveRunner {
		reasons = append(reasons, "active-runner-job")
	}
	return appendUnique(nil, reasons...)
}

type Options struct {
	Root string
	// ProtectionRoot optionally anchors protected paths above Root. Commands
	// that permit a narrowed --root should set this to their trusted workspace
	// boundary so selecting .dolt-data, data, or another protected subtree cannot
	// relabel its descendants and bypass protection.
	ProtectionRoot string
	Policy         Policy
	Apply          bool
	Scope          string
	HookPoint      string
	Safety         SafetyState
	RequireGit     bool
	// RequireIgnored is appropriate for worktree lifecycle hooks: even an
	// allowlisted directory must also be classified as ignored by Git.
	RequireIgnored            bool
	RequireMRVerification     bool
	RequireRunnerVerification bool
	// SafetyCheck is called immediately before every mutation. Lifecycle hooks
	// should use it to re-read canonical MR/runner/worktree state after scanning.
	SafetyCheck func() SafetyState
	Now         time.Time
}

type PathResult struct {
	Path   string `json:"path"`
	Bytes  int64  `json:"bytes"`
	Action string `json:"action"`
	Reason string `json:"reason,omitempty"`
}

type Result struct {
	Root            string       `json:"root"`
	Scope           string       `json:"scope"`
	HookPoint       string       `json:"hook_point"`
	DryRun          bool         `json:"dry_run"`
	Refused         bool         `json:"refused"`
	ApplyIncomplete bool         `json:"apply_incomplete"`
	RefusalReasons  []string     `json:"refusal_reasons,omitempty"`
	BytesConsidered int64        `json:"bytes_considered"`
	BytesEligible   int64        `json:"bytes_eligible"`
	BytesFreed      int64        `json:"bytes_freed"`
	PathsCleaned    []PathResult `json:"paths_cleaned"`
	PathsSkipped    []PathResult `json:"paths_skipped"`
	Recommendations []string     `json:"recommendations,omitempty"`
}

type candidate struct {
	rel    string
	abs    string
	bytes  int64
	newest time.Time
	info   fs.FileInfo
}

var errFilesystemBoundary = errors.New("filesystem boundary")
var errProtectedDescendant = errors.New("protected descendant")
var errSymlinkDescendant = errors.New("symlink descendant")

// Clean evaluates and optionally applies a cleanup policy. Apply must be set
// explicitly by the caller; policy mode alone never makes a manual dry run write.
func Clean(opts Options) (Result, error) {
	result := Result{
		Root:         opts.Root,
		Scope:        opts.Scope,
		HookPoint:    opts.HookPoint,
		DryRun:       !opts.Apply,
		PathsCleaned: []PathResult{},
		PathsSkipped: []PathResult{},
	}
	if err := opts.Policy.Validate(); err != nil {
		return result, err
	}
	root, err := canonicalRoot(opts.Root)
	if err != nil {
		return result, err
	}
	result.Root = root
	rootInfo, err := os.Lstat(root)
	if err != nil {
		return result, fmt.Errorf("stating cleanup root: %w", err)
	}
	rootFilesystem, verifyFilesystem := filesystemID(rootInfo)
	var deletionRoot *os.Root
	if opts.Apply {
		deletionRoot, err = os.OpenRoot(root)
		if err != nil {
			return result, fmt.Errorf("opening confined cleanup root: %w", err)
		}
		defer func() { _ = deletionRoot.Close() }()
	}
	// A caller may intentionally select a subdirectory as the cleanup root, but
	// protected paths remain anchored at the containing worktree. Otherwise a
	// root such as <worktree>/data combined with path "raw" would turn the
	// protected data/raw directory into an apparently unprotected raw directory.
	protectionRoot := root
	if opts.ProtectionRoot != "" {
		protectionRoot, err = canonicalRoot(opts.ProtectionRoot)
		if err != nil {
			return result, fmt.Errorf("resolving protection root: %w", err)
		}
		if !pathWithinRoot(protectionRoot, root) {
			return result, fmt.Errorf("cleanup root %q is outside protection root %q", root, protectionRoot)
		}
	} else if gitRoot, ok := containingGitRoot(root); ok {
		protectionRoot = gitRoot
	}
	if opts.Now.IsZero() {
		opts.Now = time.Now()
	}
	if !opts.Policy.Enabled && opts.Apply {
		result.Refused = true
		result.RefusalReasons = []string{"policy-disabled"}
	}

	var candidates []candidate
	var candidateBytes int64
	seen := make(map[string]bool)
	for _, pattern := range opts.Policy.Paths {
		matches, globErr := filepath.Glob(filepath.Join(root, pattern))
		if globErr != nil {
			return result, fmt.Errorf("expanding cleanup path %q: %w", pattern, globErr)
		}
		if len(matches) == 0 {
			result.PathsSkipped = append(result.PathsSkipped, PathResult{Path: pattern, Action: "skipped", Reason: "missing"})
			continue
		}
		for _, match := range matches {
			rel, relErr := filepath.Rel(root, match)
			if relErr != nil || !isContainedRelative(rel) {
				result.ApplyIncomplete = result.ApplyIncomplete || opts.Apply
				result.PathsSkipped = append(result.PathsSkipped, PathResult{Path: pattern, Action: "skipped", Reason: "outside-root"})
				continue
			}
			rel = filepath.ToSlash(rel)
			if seen[rel] {
				continue
			}
			seen[rel] = true
			if reason := safeDirectory(root, match); reason != "" {
				result.ApplyIncomplete = result.ApplyIncomplete || opts.Apply
				result.PathsSkipped = append(result.PathsSkipped, PathResult{Path: rel, Action: "skipped", Reason: reason})
				continue
			}
			candidateInfo, infoErr := os.Lstat(match)
			if infoErr != nil || !candidateInfo.IsDir() {
				result.ApplyIncomplete = result.ApplyIncomplete || opts.Apply
				result.PathsSkipped = append(result.PathsSkipped, PathResult{Path: rel, Action: "skipped", Reason: "stat-failed"})
				continue
			}
			bytes, newest, scanErr := directoryFactsOnFilesystem(match, rootFilesystem, verifyFilesystem)
			if scanErr != nil {
				result.ApplyIncomplete = result.ApplyIncomplete || opts.Apply
				reason := "scan-failed: " + scanErr.Error()
				if errors.Is(scanErr, errFilesystemBoundary) {
					reason = "filesystem-boundary"
				}
				result.PathsSkipped = append(result.PathsSkipped, PathResult{Path: rel, Action: "skipped", Reason: reason})
				continue
			}
			result.BytesConsidered += bytes
			protectedRel := rel
			if protectionRoot != root {
				anchoredRel, anchorErr := filepath.Rel(protectionRoot, match)
				if anchorErr != nil || !isContainedRelative(anchoredRel) {
					result.ApplyIncomplete = result.ApplyIncomplete || opts.Apply
					result.PathsSkipped = append(result.PathsSkipped, PathResult{Path: rel, Bytes: bytes, Action: "skipped", Reason: "outside-protection-root"})
					continue
				}
				protectedRel = filepath.ToSlash(anchoredRel)
			}
			if protectedByPolicy(rel, opts.Policy) || protectedByPolicy(protectedRel, opts.Policy) {
				result.ApplyIncomplete = result.ApplyIncomplete || opts.Apply
				result.PathsSkipped = append(result.PathsSkipped, PathResult{Path: rel, Bytes: bytes, Action: "skipped", Reason: "protected"})
				continue
			}
			if protectedPath, descendantErr := findProtectedDescendant(match, root, protectionRoot, opts.Policy, rootFilesystem, verifyFilesystem); descendantErr != nil {
				result.ApplyIncomplete = result.ApplyIncomplete || opts.Apply
				reason := descendantScanReason(protectedPath, descendantErr)
				result.PathsSkipped = append(result.PathsSkipped, PathResult{Path: rel, Bytes: bytes, Action: "skipped", Reason: reason})
				continue
			}
			tracked, gitVerified := containsTrackedPath(root, rel)
			if opts.RequireGit && !gitVerified {
				result.ApplyIncomplete = result.ApplyIncomplete || opts.Apply
				result.PathsSkipped = append(result.PathsSkipped, PathResult{Path: rel, Action: "skipped", Reason: "git-unverified"})
				continue
			}
			if tracked {
				result.ApplyIncomplete = result.ApplyIncomplete || opts.Apply
				result.PathsSkipped = append(result.PathsSkipped, PathResult{Path: rel, Action: "skipped", Reason: "tracked-path"})
				continue
			}
			if opts.RequireIgnored && !isGitIgnored(root, rel) {
				result.ApplyIncomplete = result.ApplyIncomplete || opts.Apply
				result.PathsSkipped = append(result.PathsSkipped, PathResult{Path: rel, Action: "skipped", Reason: "not-gitignored"})
				continue
			}
			candidateBytes += bytes
			candidates = append(candidates, candidate{rel: rel, abs: match, bytes: bytes, newest: newest, info: candidateInfo})
		}
	}

	refusals := append([]string(nil), result.RefusalReasons...)
	refusals = appendUnique(refusals, opts.Safety.refusalReasons()...)
	if opts.Apply && opts.RequireMRVerification && !opts.Safety.MRVerified {
		refusals = appendUnique(refusals, "active-mr-unverified")
	}
	if opts.Apply && opts.RequireRunnerVerification && !opts.Safety.RunnerVerified {
		refusals = appendUnique(refusals, "runner-state-unverified")
	}
	if len(refusals) > 0 {
		result.Refused = true
		result.ApplyIncomplete = opts.Apply
		result.RefusalReasons = refusals
		for _, c := range candidates {
			result.PathsSkipped = append(result.PathsSkipped, PathResult{Path: c.rel, Bytes: c.bytes, Action: "skipped", Reason: strings.Join(refusals, ",")})
		}
		return result, nil
	}

	// Oldest artifacts are selected first. MaxBytes is a high-water threshold:
	// cleanup stops once considered bytes fall to or below it.
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].newest.Equal(candidates[j].newest) {
			return candidates[i].rel < candidates[j].rel
		}
		return candidates[i].newest.Before(candidates[j].newest)
	})
	remaining := candidateBytes
	lateRefused := false
	for _, c := range candidates {
		if lateRefused {
			result.PathsSkipped = append(result.PathsSkipped, PathResult{Path: c.rel, Bytes: c.bytes, Action: "skipped", Reason: "safety-state-changed"})
			continue
		}
		if opts.Policy.MaxBytes > 0 && remaining <= opts.Policy.MaxBytes {
			result.PathsSkipped = append(result.PathsSkipped, PathResult{Path: c.rel, Bytes: c.bytes, Action: "skipped", Reason: "within-size-limit"})
			continue
		}
		if opts.Policy.MaxAge > 0 && opts.Now.Sub(c.newest) < opts.Policy.MaxAge {
			result.PathsSkipped = append(result.PathsSkipped, PathResult{Path: c.rel, Bytes: c.bytes, Action: "skipped", Reason: "younger-than-max-age"})
			continue
		}
		result.BytesEligible += c.bytes
		if !opts.Apply {
			result.PathsCleaned = append(result.PathsCleaned, PathResult{Path: c.rel, Bytes: c.bytes, Action: "would-clean"})
			remaining -= c.bytes
			continue
		}
		if opts.SafetyCheck != nil {
			lateState := opts.SafetyCheck()
			lateReasons := lateState.refusalReasons()
			if opts.RequireMRVerification && !lateState.MRVerified {
				lateReasons = appendUnique(lateReasons, "active-mr-unverified")
			}
			if opts.RequireRunnerVerification && !lateState.RunnerVerified {
				lateReasons = appendUnique(lateReasons, "runner-state-unverified")
			}
			if len(lateReasons) > 0 {
				lateRefused = true
				result.Refused = true
				result.ApplyIncomplete = true
				result.RefusalReasons = appendUnique(result.RefusalReasons, lateReasons...)
				result.PathsSkipped = append(result.PathsSkipped, PathResult{Path: c.rel, Bytes: c.bytes, Action: "skipped", Reason: "safety-state-changed: " + strings.Join(lateReasons, ",")})
				continue
			}
		}
		// Revalidate immediately before deletion to narrow TOCTOU exposure.
		if reason := safeDirectory(root, c.abs); reason != "" {
			result.ApplyIncomplete = true
			result.PathsSkipped = append(result.PathsSkipped, PathResult{Path: c.rel, Bytes: c.bytes, Action: "skipped", Reason: reason})
			continue
		}
		// Git classification can change while a directory is being scanned. Repeat
		// the tracked/ignored checks at the final mutation boundary so a newly
		// tracked artifact is never removed by a stale decision.
		tracked, gitVerified := containsTrackedPath(root, c.rel)
		if opts.RequireGit && !gitVerified {
			result.ApplyIncomplete = true
			result.PathsSkipped = append(result.PathsSkipped, PathResult{Path: c.rel, Bytes: c.bytes, Action: "skipped", Reason: "git-unverified"})
			continue
		}
		if tracked {
			result.ApplyIncomplete = true
			result.PathsSkipped = append(result.PathsSkipped, PathResult{Path: c.rel, Bytes: c.bytes, Action: "skipped", Reason: "tracked-path"})
			continue
		}
		if opts.RequireIgnored && !isGitIgnored(root, c.rel) {
			result.ApplyIncomplete = true
			result.PathsSkipped = append(result.PathsSkipped, PathResult{Path: c.rel, Bytes: c.bytes, Action: "skipped", Reason: "not-gitignored"})
			continue
		}
		lateInfo, lateInfoErr := deletionRoot.Lstat(filepath.FromSlash(c.rel))
		if lateInfoErr != nil || !lateInfo.IsDir() || !os.SameFile(c.info, lateInfo) {
			result.ApplyIncomplete = true
			result.PathsSkipped = append(result.PathsSkipped, PathResult{Path: c.rel, Bytes: c.bytes, Action: "skipped", Reason: "candidate-identity-changed"})
			continue
		}
		lateBytes, lateNewest, lateScanErr := directoryFactsOnFilesystem(c.abs, rootFilesystem, verifyFilesystem)
		if lateScanErr != nil || lateBytes != c.bytes || !lateNewest.Equal(c.newest) {
			result.ApplyIncomplete = true
			reason := "candidate-contents-changed"
			if errors.Is(lateScanErr, errFilesystemBoundary) {
				reason = "filesystem-boundary"
			}
			result.PathsSkipped = append(result.PathsSkipped, PathResult{Path: c.rel, Bytes: c.bytes, Action: "skipped", Reason: reason})
			continue
		}
		if protectedPath, descendantErr := findProtectedDescendant(c.abs, root, protectionRoot, opts.Policy, rootFilesystem, verifyFilesystem); descendantErr != nil {
			result.ApplyIncomplete = true
			result.PathsSkipped = append(result.PathsSkipped, PathResult{Path: c.rel, Bytes: c.bytes, Action: "skipped", Reason: descendantScanReason(protectedPath, descendantErr)})
			continue
		}
		// Root.RemoveAll resolves every path component relative to the open root
		// handle and refuses escapes. Unlike a pathname-based RemoveAll, this stays
		// confined if another process swaps an ancestor for a symlink after the
		// validation and Git checks above.
		if err := deletionRoot.RemoveAll(filepath.FromSlash(c.rel)); err != nil {
			result.ApplyIncomplete = true
			freed := estimatedRemovedBytes(c.abs, c.bytes, rootFilesystem, verifyFilesystem)
			result.BytesFreed += freed
			remaining -= freed
			result.PathsSkipped = append(result.PathsSkipped, PathResult{Path: c.rel, Bytes: c.bytes, Action: "skipped", Reason: fmt.Sprintf("remove-failed (estimated %d bytes removed): %v", freed, err)})
			continue
		}
		if _, err := deletionRoot.Lstat(filepath.FromSlash(c.rel)); !errors.Is(err, fs.ErrNotExist) {
			result.ApplyIncomplete = true
			result.PathsSkipped = append(result.PathsSkipped, PathResult{Path: c.rel, Bytes: c.bytes, Action: "skipped", Reason: "remove-incomplete"})
			continue
		}
		result.BytesFreed += c.bytes
		remaining -= c.bytes
		result.PathsCleaned = append(result.PathsCleaned, PathResult{Path: c.rel, Bytes: c.bytes, Action: "cleaned"})
	}
	return result, nil
}

func canonicalRoot(root string) (string, error) {
	if root == "" || !filepath.IsAbs(root) {
		return "", fmt.Errorf("cleanup root must be an absolute path, got %q", root)
	}
	resolved, err := filepath.EvalSymlinks(root)
	if err != nil {
		return "", fmt.Errorf("resolving cleanup root: %w", err)
	}
	info, err := os.Stat(resolved)
	if err != nil || !info.IsDir() {
		return "", fmt.Errorf("cleanup root is not a directory: %s", root)
	}
	return filepath.Clean(resolved), nil
}

func containingGitRoot(root string) (string, bool) {
	cmd := exec.Command("git", "-C", root, "rev-parse", "--show-toplevel") //nolint:gosec // fixed executable
	out, err := cmd.Output()
	if err != nil {
		return "", false
	}
	gitRoot, err := canonicalRoot(strings.TrimSpace(string(out)))
	if err != nil {
		return "", false
	}
	rel, err := filepath.Rel(gitRoot, root)
	if err != nil || filepath.IsAbs(rel) || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", false
	}
	return gitRoot, true
}

func pathWithinRoot(root, candidate string) bool {
	rel, err := filepath.Rel(root, candidate)
	return err == nil && !filepath.IsAbs(rel) && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

func safeDirectory(root, path string) string {
	rel, err := filepath.Rel(root, path)
	if err != nil || !isContainedRelative(rel) {
		return "outside-root"
	}
	current := root
	for _, part := range strings.Split(filepath.Clean(rel), string(filepath.Separator)) {
		current = filepath.Join(current, part)
		info, statErr := os.Lstat(current)
		if statErr != nil {
			if os.IsNotExist(statErr) {
				return "missing"
			}
			return "stat-failed"
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return "symlink"
		}
	}
	info, err := os.Stat(path)
	if err != nil || !info.IsDir() {
		return "not-directory"
	}
	return ""
}

func isContainedRelative(rel string) bool {
	return rel != "." && rel != "" && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) && !filepath.IsAbs(rel)
}

func directoryFacts(root string) (int64, time.Time, error) {
	return directoryFactsOnFilesystem(root, 0, false)
}

func directoryFactsOnFilesystem(root string, expectedFilesystem uint64, verifyFilesystem bool) (int64, time.Time, error) {
	var bytes int64
	var newest time.Time
	err := filepath.WalkDir(root, func(walkPath string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.ModTime().After(newest) {
			newest = info.ModTime()
		}
		if info.Mode().IsRegular() {
			bytes += info.Size()
		}
		if entry.IsDir() {
			mounted, mountErr := isMountPoint(walkPath)
			if mountErr != nil {
				return fmt.Errorf("reading mount table: %w", mountErr)
			}
			if mounted {
				return fmt.Errorf("%w at %s", errFilesystemBoundary, entry.Name())
			}
		}
		if verifyFilesystem {
			if currentFilesystem, ok := filesystemID(info); !ok || currentFilesystem != expectedFilesystem {
				return fmt.Errorf("%w at %s", errFilesystemBoundary, entry.Name())
			}
		}
		return nil
	})
	return bytes, newest, err
}

func findProtectedDescendant(candidate, cleanupRoot, protectionRoot string, policy Policy, expectedFilesystem uint64, verifyFilesystem bool) (string, error) {
	protectedPath := ""
	err := filepath.WalkDir(candidate, func(walkPath string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if walkPath == candidate {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("%w: %s", errSymlinkDescendant, entry.Name())
		}
		if verifyFilesystem {
			if currentFilesystem, ok := filesystemID(info); !ok || currentFilesystem != expectedFilesystem {
				return fmt.Errorf("%w at %s", errFilesystemBoundary, entry.Name())
			}
		}
		cleanupRel, err := filepath.Rel(cleanupRoot, walkPath)
		if err != nil || !isContainedRelative(cleanupRel) {
			return fmt.Errorf("protected descendant containment check failed for %s", walkPath)
		}
		protectionRel, err := filepath.Rel(protectionRoot, walkPath)
		if err != nil || !isContainedRelative(protectionRel) {
			return fmt.Errorf("protected descendant anchor check failed for %s", walkPath)
		}
		cleanupRel = filepath.ToSlash(cleanupRel)
		protectionRel = filepath.ToSlash(protectionRel)
		if protectedByPolicy(cleanupRel, policy) || protectedByPolicy(protectionRel, policy) {
			protectedPath = cleanupRel
			return fmt.Errorf("%w", errProtectedDescendant)
		}
		return nil
	})
	return protectedPath, err
}

func descendantScanReason(protectedPath string, err error) string {
	switch {
	case errors.Is(err, errProtectedDescendant):
		return "protected-descendant: " + protectedPath
	case errors.Is(err, errFilesystemBoundary):
		return "filesystem-boundary"
	case errors.Is(err, errSymlinkDescendant):
		return "symlink-descendant"
	default:
		return "scan-failed: " + err.Error()
	}
}

func estimatedRemovedBytes(candidate string, originalBytes int64, expectedFilesystem uint64, verifyFilesystem bool) int64 {
	remainingBytes, _, err := directoryFactsOnFilesystem(candidate, expectedFilesystem, verifyFilesystem)
	if errors.Is(err, fs.ErrNotExist) || os.IsNotExist(err) {
		return originalBytes
	}
	if err != nil || remainingBytes >= originalBytes {
		return 0
	}
	return originalBytes - remainingBytes
}

func protectedByPolicy(path string, p Policy) bool {
	protectedPaths := appendUnique(append([]string(nil), DefaultProtectedPaths...), p.ProtectedPaths...)
	for _, protected := range protectedPaths {
		matchingPaths := []string{}
		if patternsIntersect(path, protected) {
			matchingPaths = append(matchingPaths, filepath.ToSlash(path))
		}
		// Permanent metadata and secret names stay protected wherever they occur
		// inside a candidate. Business/custom data rules remain anchored to the
		// worktree policy root so ordinary build paths named "data" are cleanable.
		if isPermanentProtectedPattern(protected) {
			matchingPaths = appendUnique(matchingPaths, protectedPathSuffixes(path, protected)...)
		}
		if len(matchingPaths) == 0 {
			continue
		}
		allowed := false
		for _, matchedPath := range matchingPaths {
			for _, override := range p.AllowProtectedPaths {
				if patternCoversPath(override, matchedPath) {
					allowed = true
					break
				}
			}
			if allowed {
				break
			}
		}
		if !allowed {
			return true
		}
	}
	return false
}

func isPermanentProtectedPattern(pattern string) bool {
	pattern = strings.ToLower(filepath.ToSlash(pattern))
	for _, permanent := range PermanentProtectedPaths {
		if pattern == strings.ToLower(filepath.ToSlash(permanent)) {
			return true
		}
	}
	return false
}

// protectedPathSuffixes makes protections independent of how broadly a caller
// anchors cleanup. For example, both data/raw and rig/mayor/rig/data/raw match
// the protected data prefix. This is intentionally conservative.
func protectedPathSuffixes(candidate, protected string) []string {
	parts := strings.Split(filepath.ToSlash(candidate), "/")
	var matches []string
	for i := range parts {
		suffix := strings.Join(parts[i:], "/")
		if patternsIntersect(suffix, protected) {
			matches = append(matches, suffix)
		}
	}
	return matches
}

func patternCoversPath(pattern, candidate string) bool {
	// Be conservative on every platform. Gas Town commonly runs on default
	// case-insensitive APFS, where .BEADS and .beads name the same directory.
	pattern = strings.ToLower(filepath.ToSlash(pattern))
	candidate = strings.ToLower(filepath.ToSlash(candidate))
	if matched, _ := path.Match(pattern, candidate); matched {
		return true
	}
	if strings.ContainsAny(pattern, "*?[") {
		return false
	}
	return strings.HasPrefix(candidate, strings.TrimSuffix(pattern, "/")+"/")
}

// containsTrackedPath prevents cleanup from turning a clean worktree dirty and
// reports whether Git classification was available.
func containsTrackedPath(root, rel string) (tracked, verified bool) {
	probe := exec.Command("git", "-C", root, "rev-parse", "--is-inside-work-tree") //nolint:gosec // fixed executable
	if out, err := probe.Output(); err != nil || strings.TrimSpace(string(out)) != "true" {
		return false, false
	}
	// icase closes the APFS alias gap (.BEADS and .beads can be the same path)
	// while literal prevents cleanup patterns from becoming Git pathspec syntax.
	pathspec := ":(icase,literal)" + filepath.ToSlash(rel)
	cmd := exec.Command("git", "-C", root, "ls-files", "--", pathspec) //nolint:gosec // fixed executable and validated relative path
	out, err := cmd.Output()
	if err != nil {
		return false, false
	}
	return strings.TrimSpace(string(out)) != "", true
}

func isGitIgnored(root, rel string) bool {
	cmd := exec.Command("git", "-C", root, "check-ignore", "-q", "--", filepath.FromSlash(rel)) //nolint:gosec // fixed executable and validated relative path
	return cmd.Run() == nil
}

func patternsIntersect(candidate, pattern string) bool {
	// Protection matching must follow case-insensitive filesystem aliases even
	// when tests or callers run on a case-sensitive host.
	candidate = strings.ToLower(filepath.ToSlash(candidate))
	pattern = strings.ToLower(filepath.ToSlash(pattern))
	// Match the candidate and each of its ancestors. A protected directory glob
	// such as .dolt-* must also cover .dolt-data/hq/chunks.
	for current := candidate; current != ""; {
		if matched, _ := path.Match(pattern, current); matched {
			return true
		}
		cut := strings.LastIndex(current, "/")
		if cut < 0 {
			break
		}
		current = current[:cut]
	}
	// Refuse deletion of either a protected descendant or its ancestor.
	staticPrefix := pattern
	if i := strings.IndexAny(staticPrefix, "*?["); i >= 0 {
		staticPrefix = strings.TrimSuffix(staticPrefix[:i], "/")
	}
	return staticPrefix != "" && (strings.HasPrefix(candidate, staticPrefix+"/") || strings.HasPrefix(staticPrefix, candidate+"/") || candidate == staticPrefix)
}

func appendUnique(dst []string, values ...string) []string {
	seen := make(map[string]bool, len(dst)+len(values))
	for _, item := range dst {
		seen[item] = true
	}
	for _, item := range values {
		if item != "" && !seen[item] {
			dst = append(dst, item)
			seen[item] = true
		}
	}
	return dst
}
