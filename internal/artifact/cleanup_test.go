package artifact

import (
	"encoding/hex"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func boolPtr(v bool) *bool    { return &v }
func int64Ptr(v int64) *int64 { return &v }

func testPolicy(paths ...string) Policy {
	p := DefaultPolicy()
	p.Enabled = true
	p.Paths = paths
	return p
}

func writeArtifact(t *testing.T, root, rel, body string) string {
	t.Helper()
	path := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestResolvePolicyTownAndRigOverrides(t *testing.T) {
	town := &PolicyConfig{
		Enabled:        boolPtr(true),
		Mode:           ModeApply,
		Paths:          []string{"target", "dist"},
		MaxAge:         "24h",
		MaxBytes:       int64Ptr(100),
		OnPolecatReuse: boolPtr(true),
		// A town may not opt protected data into deletion.
		AllowProtectedPaths: []string{"data/raw"},
	}
	rig := &PolicyConfig{
		Paths:               []string{"dist", "data/raw"},
		AllowProtectedPaths: []string{"data/raw"},
		MaxAge:              "48h",
	}
	got, err := ResolvePolicy(town, rig)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Enabled || got.Mode != ModeApply || got.MaxAge != 48*time.Hour || got.MaxBytes != 100 || !got.OnPolecatReuse {
		t.Fatalf("unexpected merged policy: %+v", got)
	}
	if strings.Join(got.Paths, ",") != "dist,data/raw" || strings.Join(got.AllowProtectedPaths, ",") != "data/raw" {
		t.Fatalf("rig overrides not applied: %+v", got)
	}
}

func TestResolvePolicyRejectsPermanentOrUnrelatedProtectedOverrides(t *testing.T) {
	for _, path := range []string{".git", ".dolt-data", ".env", "secrets", "target", "ml", "seif_ingestion"} {
		t.Run(path, func(t *testing.T) {
			_, err := ResolvePolicy(nil, &PolicyConfig{AllowProtectedPaths: []string{path}})
			if err == nil {
				t.Fatalf("expected override %q to fail", path)
			}
		})
	}
}

func TestResolvePolicyAllowsOnlyProtectedSubtreeOverrides(t *testing.T) {
	for _, override := range []string{"data/raw", "ml/models", "ml/models/cache", "seif_ingestion/checkpoints-1", "seif_ingestion/checkpoints-1/cache"} {
		t.Run(override, func(t *testing.T) {
			if _, err := ResolvePolicy(nil, &PolicyConfig{AllowProtectedPaths: []string{override}}); err != nil {
				t.Fatalf("valid narrow override %q was rejected: %v", override, err)
			}
		})
	}
}

func TestValidateRelativePatternRejectsKnownBadPatterns(t *testing.T) {
	bad := []string{"", ".", "..", "../target", "target/../data", "/tmp/target", "*", "*/target", "foo/**/target", "foo/["}
	if runtime.GOOS == "windows" {
		bad = append(bad, `C:\target`)
	}
	for _, pattern := range bad {
		t.Run(pattern, func(t *testing.T) {
			if err := ValidateRelativePattern(pattern); err == nil {
				t.Fatalf("expected %q to be rejected", pattern)
			}
		})
	}
	for _, pattern := range []string{"target", "work/*/.pnpm-store", "shared/trivy-cache"} {
		if err := ValidateRelativePattern(pattern); err != nil {
			t.Fatalf("expected %q to be valid: %v", pattern, err)
		}
	}
}

func TestCleanDryRunThenApply(t *testing.T) {
	root := t.TempDir()
	writeArtifact(t, root, "dist/bundle.js", strings.Repeat("x", 128))
	policy := testPolicy("dist")

	dry, err := Clean(Options{Root: root, Policy: policy, HookPoint: "manual"})
	if err != nil {
		t.Fatal(err)
	}
	if !dry.DryRun || dry.BytesEligible != 128 || dry.BytesFreed != 0 || len(dry.PathsCleaned) != 1 || dry.PathsCleaned[0].Action != "would-clean" {
		t.Fatalf("unexpected dry-run result: %+v", dry)
	}
	if _, err := os.Stat(filepath.Join(root, "dist")); err != nil {
		t.Fatalf("dry run removed artifact: %v", err)
	}

	applied, err := Clean(Options{Root: root, Policy: policy, Apply: true, HookPoint: "manual"})
	if err != nil {
		t.Fatal(err)
	}
	if applied.DryRun || applied.BytesFreed != 128 || len(applied.PathsCleaned) != 1 || applied.PathsCleaned[0].Action != "cleaned" {
		t.Fatalf("unexpected apply result: %+v", applied)
	}
	if _, err := os.Stat(filepath.Join(root, "dist")); !os.IsNotExist(err) {
		t.Fatalf("apply did not remove artifact: %v", err)
	}
}

func TestCleanRefusesDisabledPolicy(t *testing.T) {
	root := t.TempDir()
	writeArtifact(t, root, "dist/x", "x")
	policy := testPolicy("dist")
	policy.Enabled = false
	result, err := Clean(Options{Root: root, Policy: policy, Apply: true})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Refused || strings.Join(result.RefusalReasons, ",") != "policy-disabled" {
		t.Fatalf("expected disabled refusal: %+v", result)
	}
	if _, err := os.Stat(filepath.Join(root, "dist")); err != nil {
		t.Fatal("disabled policy removed artifact")
	}
}

func TestCleanSafetyRefusals(t *testing.T) {
	tests := []struct {
		name   string
		state  SafetyState
		reason string
	}{
		{"dirty worktree", SafetyState{Dirty: true}, "dirty-worktree"},
		{"active MR", SafetyState{ActiveMR: true}, "active-merge-request"},
		{"active runner", SafetyState{ActiveRunner: true}, "active-runner-job"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			writeArtifact(t, root, "dist/x", "x")
			result, err := Clean(Options{Root: root, Policy: testPolicy("dist"), Apply: true, Safety: tc.state})
			if err != nil {
				t.Fatal(err)
			}
			if !result.Refused || !contains(result.RefusalReasons, tc.reason) {
				t.Fatalf("expected %s refusal: %+v", tc.reason, result)
			}
			if _, err := os.Stat(filepath.Join(root, "dist")); err != nil {
				t.Fatal("unsafe cleanup removed artifact")
			}
		})
	}
}

func TestCleanRequiresExplicitStateVerification(t *testing.T) {
	root := t.TempDir()
	writeArtifact(t, root, "dist/x", "x")
	result, err := Clean(Options{
		Root: root, Policy: testPolicy("dist"), Apply: true,
		RequireMRVerification: true, RequireRunnerVerification: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Refused || !contains(result.RefusalReasons, "active-mr-unverified") || !contains(result.RefusalReasons, "runner-state-unverified") {
		t.Fatalf("unverified state did not fail closed: %+v", result)
	}
}

func TestCleanDryRunDoesNotRequireStateAssertion(t *testing.T) {
	root := t.TempDir()
	writeArtifact(t, root, "dist/x", "x")
	result, err := Clean(Options{
		Root: root, Policy: testPolicy("dist"),
		RequireMRVerification: true, RequireRunnerVerification: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Refused || len(result.PathsCleaned) != 1 || result.PathsCleaned[0].Action != "would-clean" {
		t.Fatalf("read-only dry run was blocked by apply-only verification: %+v", result)
	}
}

func TestCleanRechecksSafetyBeforeMutation(t *testing.T) {
	root := t.TempDir()
	writeArtifact(t, root, "dist/x", "x")
	checks := 0
	result, err := Clean(Options{
		Root: root, Policy: testPolicy("dist"), Apply: true,
		SafetyCheck: func() SafetyState {
			checks++
			return SafetyState{ActiveMR: true}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if checks != 1 || !result.Refused || !contains(result.RefusalReasons, "active-merge-request") {
		t.Fatalf("late state was not refused: %+v", result)
	}
	if _, err := os.Stat(filepath.Join(root, "dist")); err != nil {
		t.Fatal("late safety refusal did not preserve artifact")
	}
}

func TestCleanMissingPathIsSkipped(t *testing.T) {
	result, err := Clean(Options{Root: t.TempDir(), Policy: testPolicy("dist")})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.PathsSkipped) != 1 || result.PathsSkipped[0].Reason != "missing" {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestCleanProtectedPathsNeedSecondKey(t *testing.T) {
	root := t.TempDir()
	writeArtifact(t, root, "data/raw/input.csv", "business data")
	policy := testPolicy("data/raw")
	blocked, err := Clean(Options{Root: root, Policy: policy, Apply: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(blocked.PathsSkipped) != 1 || blocked.PathsSkipped[0].Reason != "protected" {
		t.Fatalf("protected data was not blocked: %+v", blocked)
	}

	policy.AllowProtectedPaths = []string{"data/raw"}
	allowed, err := Clean(Options{Root: root, Policy: policy, Apply: true})
	if err != nil {
		t.Fatal(err)
	}
	if allowed.BytesFreed == 0 {
		t.Fatalf("two-key protected override did not apply: %+v", allowed)
	}
}

func TestProtectedPathsAreAccountedWithoutBecomingEligible(t *testing.T) {
	root := t.TempDir()
	writeArtifact(t, root, ".dolt-data/hq/chunk", strings.Repeat("d", 23))
	result, err := Clean(Options{Root: root, Policy: testPolicy(".dolt-data"), Apply: true})
	if err != nil {
		t.Fatal(err)
	}
	if result.BytesConsidered != 23 || result.BytesEligible != 0 || result.BytesFreed != 0 {
		t.Fatalf("protected accounting is wrong: %+v", result)
	}
	if len(result.PathsSkipped) != 1 || result.PathsSkipped[0].Bytes != 23 || result.PathsSkipped[0].Reason != "protected" {
		t.Fatalf("protected path detail missing: %+v", result.PathsSkipped)
	}
}

func TestPermanentProtectionCannotBeRemovedFromDirectPolicy(t *testing.T) {
	root := t.TempDir()
	writeArtifact(t, root, ".beads/state", "keep")
	policy := Policy{Enabled: true, Mode: ModeApply, Paths: []string{".beads"}}
	result, err := Clean(Options{Root: root, Policy: policy, Apply: true})
	if err != nil {
		t.Fatal(err)
	}
	if !result.ApplyIncomplete || len(result.PathsSkipped) != 1 || result.PathsSkipped[0].Reason != "protected" {
		t.Fatalf("direct policy removed built-in protection: %+v", result)
	}
}

func TestPermanentProtectionIsCaseInsensitive(t *testing.T) {
	root := t.TempDir()
	writeArtifact(t, root, ".BEADS/state", "keep")
	result, err := Clean(Options{Root: root, Policy: testPolicy(".BEADS"), Apply: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.PathsSkipped) != 1 || result.PathsSkipped[0].Reason != "protected" {
		t.Fatalf("case alias bypassed built-in protection: %+v", result)
	}
	if _, err := os.Stat(filepath.Join(root, ".BEADS", "state")); err != nil {
		t.Fatal("case-aliased protected directory was removed")
	}
}

func TestEnvironmentProtectionIncludesEnvrc(t *testing.T) {
	root := t.TempDir()
	writeArtifact(t, root, ".envrc/cache", "secret-adjacent")
	result, err := Clean(Options{Root: root, Policy: testPolicy(".envrc"), Apply: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.PathsSkipped) != 1 || result.PathsSkipped[0].Reason != "protected" {
		t.Fatalf(".env* protection did not cover .envrc: %+v", result)
	}
}

func TestProtectedDescendantBlocksParentRemoval(t *testing.T) {
	for _, descendant := range []string{"dist/.git/objects/x", "dist/.beads/state", "dist/secrets/token"} {
		t.Run(descendant, func(t *testing.T) {
			root := t.TempDir()
			writeArtifact(t, root, descendant, "keep")
			result, err := Clean(Options{Root: root, Policy: testPolicy("dist"), Apply: true})
			if err != nil {
				t.Fatal(err)
			}
			if len(result.PathsSkipped) != 1 || !strings.HasPrefix(result.PathsSkipped[0].Reason, "protected-descendant:") {
				t.Fatalf("parent cleanup ignored protected descendant: %+v", result)
			}
			if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(descendant))); err != nil {
				t.Fatal("parent cleanup removed protected descendant")
			}
		})
	}
}

func TestCIMaintenanceProtocolFilesProtectSharedParent(t *testing.T) {
	for _, protocolPath := range []string{
		"shared/.ci-protocol-mutex/owner",
		"shared/.maintenance-active/owner",
		"shared/.ci-active-jobs/runner-1/.job-active",
	} {
		t.Run(protocolPath, func(t *testing.T) {
			root := t.TempDir()
			writeArtifact(t, root, "shared/cache/data", "generated")
			writeArtifact(t, root, protocolPath, CIHookProtocolVersion+"\n")
			result, err := Clean(Options{Root: root, Policy: testPolicy("shared"), Apply: true})
			if err != nil {
				t.Fatal(err)
			}
			if !result.ApplyIncomplete || len(result.PathsSkipped) != 1 || !strings.HasPrefix(result.PathsSkipped[0].Reason, "protected-descendant:") {
				t.Fatalf("shared parent cleanup could remove protocol state: %+v", result)
			}
			if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(protocolPath))); err != nil {
				t.Fatalf("cleanup removed protocol state %s", protocolPath)
			}
		})
	}
}

func TestCIRunnerManagedSetupPathsArePermanentlyProtected(t *testing.T) {
	for _, managedPath := range []string{
		"work/runner-1/_actions/cache/data",
		"work/runner-1/_temp/job/data",
		"work/runner-1/_work/repository/data",
		"work/runner-1/_tool/python/data",
	} {
		t.Run(managedPath, func(t *testing.T) {
			root := t.TempDir()
			writeArtifact(t, root, managedPath, "runner-managed")
			candidate := filepath.ToSlash(filepath.Dir(managedPath))
			result, err := Clean(Options{Root: root, Policy: testPolicy(candidate), Apply: true, Scope: "ci"})
			if err != nil {
				t.Fatal(err)
			}
			if !result.ApplyIncomplete || len(result.PathsCleaned) != 0 || len(result.PathsSkipped) != 1 || result.PathsSkipped[0].Reason != "protected" {
				t.Fatalf("runner setup path was not permanently protected: %+v", result)
			}
			if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(managedPath))); err != nil {
				t.Fatalf("runner setup path was removed: %v", err)
			}
		})
	}
}

func TestNestedBuildDirectoryNamedDataIsNotBusinessData(t *testing.T) {
	root := t.TempDir()
	writeArtifact(t, root, ".next/types/app/(protected)/data/generated", "generated")
	result, err := Clean(Options{Root: root, Policy: testPolicy(".next"), Apply: true})
	if err != nil {
		t.Fatal(err)
	}
	if result.BytesFreed == 0 || len(result.PathsCleaned) != 1 {
		t.Fatalf("nested generated data directory blocked cache cleanup: %+v", result)
	}
}

func TestCleanNarrowedRootKeepsWorktreeProtectedPaths(t *testing.T) {
	worktree := t.TempDir()
	for _, args := range [][]string{{"init", "-q"}} {
		cmd := exec.Command("git", args...)
		cmd.Dir = worktree
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
	dataRoot := filepath.Join(worktree, "data")
	writeArtifact(t, dataRoot, "raw/input.csv", "business data")
	result, err := Clean(Options{Root: dataRoot, Policy: testPolicy("raw"), Apply: true, RequireGit: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.PathsSkipped) != 1 || result.PathsSkipped[0].Reason != "protected" {
		t.Fatalf("narrowed cleanup root bypassed worktree protection: %+v", result)
	}
	if _, err := os.Stat(filepath.Join(dataRoot, "raw", "input.csv")); err != nil {
		t.Fatal("narrowed cleanup root removed protected project data")
	}
}

func TestCleanProtectionRootBlocksNonGitNarrowing(t *testing.T) {
	town := t.TempDir()
	doltRoot := filepath.Join(town, ".dolt-data")
	writeArtifact(t, doltRoot, "hq/chunk", "control-plane data")
	result, err := Clean(Options{
		Root: doltRoot, ProtectionRoot: town, Policy: testPolicy("hq"), Apply: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.PathsSkipped) != 1 || result.PathsSkipped[0].Reason != "protected" {
		t.Fatalf("non-Git narrowed root bypassed protection anchor: %+v", result)
	}
	if _, err := os.Stat(filepath.Join(doltRoot, "hq", "chunk")); err != nil {
		t.Fatal("narrowed CI-style root removed protected Dolt data")
	}
}

func TestCleanProtectionRootBlocksGitMetadataNarrowing(t *testing.T) {
	town := t.TempDir()
	gitRoot := filepath.Join(town, "rig", ".git")
	writeArtifact(t, gitRoot, "objects/valuable", "repository data")
	result, err := Clean(Options{
		Root: gitRoot, ProtectionRoot: town, Policy: testPolicy("objects"), Apply: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.PathsSkipped) != 1 || result.PathsSkipped[0].Reason != "protected" {
		t.Fatalf("narrowed .git root bypassed protection anchor: %+v", result)
	}
	if _, err := os.Stat(filepath.Join(gitRoot, "objects", "valuable")); err != nil {
		t.Fatal("narrowed cleanup root removed Git object storage")
	}
}

func TestCleanNarrowOverrideCannotDeleteProtectedParent(t *testing.T) {
	root := t.TempDir()
	writeArtifact(t, root, "data/raw/input.csv", "x")
	policy := testPolicy("data")
	policy.AllowProtectedPaths = []string{"data/raw"}
	result, err := Clean(Options{Root: root, Policy: policy, Apply: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.PathsSkipped) != 1 || result.PathsSkipped[0].Reason != "protected" {
		t.Fatalf("narrow override authorized parent deletion: %+v", result)
	}
}

func TestCleanRejectsSymlinkComponentsAndEscape(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	writeArtifact(t, outside, "payload/valuable", "keep")
	if err := os.Symlink(filepath.Join(outside, "payload"), filepath.Join(root, "dist")); err != nil {
		t.Fatal(err)
	}
	result, err := Clean(Options{Root: root, Policy: testPolicy("dist"), Apply: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.PathsSkipped) != 1 || result.PathsSkipped[0].Reason != "symlink" {
		t.Fatalf("symlink not rejected: %+v", result)
	}
	if _, err := os.Stat(filepath.Join(outside, "payload", "valuable")); err != nil {
		t.Fatal("symlink escape deleted outside data")
	}
}

func TestCleanRejectsTrackedArtifact(t *testing.T) {
	root := t.TempDir()
	writeArtifact(t, root, "dist/checked-in.txt", "tracked")
	for _, args := range [][]string{{"init", "-q"}, {"add", "dist/checked-in.txt"}} {
		cmd := exec.Command("git", args...)
		cmd.Dir = root
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
	result, err := Clean(Options{Root: root, Policy: testPolicy("dist"), Apply: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.PathsSkipped) != 1 || result.PathsSkipped[0].Reason != "tracked-path" {
		t.Fatalf("tracked artifact not rejected: %+v", result)
	}
}

func TestTrackedClassificationIsCaseInsensitiveAndLiteral(t *testing.T) {
	root := t.TempDir()
	writeArtifact(t, root, ".beads/state", "tracked")
	for _, args := range [][]string{{"init", "-q"}, {"add", ".beads/state"}} {
		cmd := exec.Command("git", args...)
		cmd.Dir = root
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
	tracked, verified := containsTrackedPath(root, ".BEADS")
	if !verified || !tracked {
		t.Fatalf("case-aliased tracked path was not detected: tracked=%t verified=%t", tracked, verified)
	}
}

func TestCleanRequiresGitIgnoredArtifactForWorktreeScope(t *testing.T) {
	root := t.TempDir()
	for _, args := range [][]string{{"init", "-q"}} {
		cmd := exec.Command("git", args...)
		cmd.Dir = root
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
	writeArtifact(t, root, "dist/x", "x")
	policy := testPolicy("dist")
	blocked, err := Clean(Options{Root: root, Policy: policy, Apply: true, RequireGit: true, RequireIgnored: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(blocked.PathsSkipped) != 1 || blocked.PathsSkipped[0].Reason != "not-gitignored" {
		t.Fatalf("nonignored path was not blocked: %+v", blocked)
	}
	if err := os.WriteFile(filepath.Join(root, ".gitignore"), []byte("dist/\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	allowed, err := Clean(Options{Root: root, Policy: policy, Apply: true, RequireGit: true, RequireIgnored: true})
	if err != nil {
		t.Fatal(err)
	}
	if allowed.BytesFreed != 1 {
		t.Fatalf("ignored path was not cleaned: %+v", allowed)
	}
}

func TestCleanRechecksGitClassificationBeforeMutation(t *testing.T) {
	root := t.TempDir()
	cmd := exec.Command("git", "init", "-q")
	cmd.Dir = root
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git init: %v: %s", err, out)
	}
	if err := os.WriteFile(filepath.Join(root, ".gitignore"), []byte("dist/\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	writeArtifact(t, root, "dist/x", "x")
	checks := 0
	result, err := Clean(Options{
		Root: root, Policy: testPolicy("dist"), Apply: true, RequireGit: true, RequireIgnored: true,
		SafetyCheck: func() SafetyState {
			checks++
			add := exec.Command("git", "add", "-f", "dist/x")
			add.Dir = root
			if out, addErr := add.CombinedOutput(); addErr != nil {
				t.Fatalf("git add: %v: %s", addErr, out)
			}
			return SafetyState{}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if checks != 1 || len(result.PathsSkipped) != 1 || result.PathsSkipped[0].Reason != "tracked-path" {
		t.Fatalf("late Git state change was not blocked: %+v", result)
	}
	if _, err := os.Stat(filepath.Join(root, "dist", "x")); err != nil {
		t.Fatal("newly tracked artifact was removed")
	}
}

func TestCleanAgeUsesNewestDescendantAndMaxBytes(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	root := t.TempDir()
	old := writeArtifact(t, root, "work/a/cache/old", strings.Repeat("a", 10))
	newer := writeArtifact(t, root, "work/b/cache/new", strings.Repeat("b", 10))
	oldTime := now.Add(-72 * time.Hour)
	newTime := now.Add(-time.Hour)
	for _, path := range []string{old, filepath.Dir(old), filepath.Dir(filepath.Dir(old))} {
		if err := os.Chtimes(path, oldTime, oldTime); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Chtimes(newer, newTime, newTime); err != nil {
		t.Fatal(err)
	}

	policy := testPolicy("work/*/cache")
	policy.MaxAge = 24 * time.Hour
	policy.MaxBytes = 10 // high-water mark: remove the old 10 bytes, retain new 10.
	result, err := Clean(Options{Root: root, Policy: policy, Now: now})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.PathsCleaned) != 1 || result.PathsCleaned[0].Path != "work/a/cache" {
		t.Fatalf("unexpected age/size selection: %+v", result)
	}
	if len(result.PathsSkipped) != 1 || result.PathsSkipped[0].Reason != "within-size-limit" {
		t.Fatalf("new candidate should be within retained size: %+v", result.PathsSkipped)
	}
}

func TestDetectSafetyDirtyMRRunnerAndHandoff(t *testing.T) {
	root := t.TempDir()
	cmd := exec.Command("git", "init", "-q")
	cmd.Dir = root
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git init: %v: %s", err, out)
	}
	writeArtifact(t, root, "untracked.txt", "dirty")
	writeArtifact(t, root, "work/runner-1/.job-active", "")
	writeArtifact(t, root, ".active-mr", "hq-mr")
	state := DetectSafety(root, "ci")
	if state.Dirty || !state.ActiveMR || !state.ActiveRunner {
		t.Fatalf("incomplete safety discovery: %+v", state)
	}
	if err := os.Remove(filepath.Join(root, "work", "runner-1", ".job-active")); err != nil {
		t.Fatal(err)
	}
	writeArtifact(t, root, "shared/handoff/.handoff-active", "")
	if state := DetectSafety(root, "ci"); !state.ActiveRunner {
		t.Fatal("active cross-job handoff marker did not block CI cleanup")
	}
}

func TestDetectSafetyRequiresInstalledRunnerHooks(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "work", "runner-1", "_actions"), 0o755); err != nil {
		t.Fatal(err)
	}
	if state := DetectSafety(root, "ci"); state.RunnerHooksVerified {
		t.Fatal("runner without installed lifecycle hooks was treated as verified")
	}
	writeArtifact(t, root, "work/runner-1/.ci-job-hooks-installed", CIHookProtocolVersion+"\n")
	if state := DetectSafety(root, "ci"); !state.RunnerHooksVerified {
		t.Fatal("installed runner hook sentinel was not recognized")
	}
	if err := os.MkdirAll(filepath.Join(root, "work", "dd-runner-1", "_temp"), 0o755); err != nil {
		t.Fatal(err)
	}
	if state := DetectSafety(root, "ci"); state.RunnerHooksVerified {
		t.Fatal("uninstrumented alternate runner family was ignored")
	}
	writeArtifact(t, root, "work/dd-runner-1/.ci-job-hooks-installed", CIHookProtocolVersion+"\n")
	if state := DetectSafety(root, "ci"); !state.RunnerHooksVerified {
		t.Fatal("all instrumented runner families were not recognized")
	}
}

func TestDetectSafetyDirtyGateIsWorktreeOnly(t *testing.T) {
	root := t.TempDir()
	cmd := exec.Command("git", "init", "-q")
	cmd.Dir = root
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git init: %v: %s", err, out)
	}
	writeArtifact(t, root, "untracked", "dirty")
	if state := DetectSafety(root, "rig"); !state.Dirty {
		t.Fatal("rig worktree dirtiness was not detected")
	}
	if state := DetectSafety(root, "ci"); state.Dirty {
		t.Fatal("CI root inherited enclosing Git dirtiness")
	}
}

func TestCIMaintenanceLockUsesVerifiedRunnerHelperAndNonce(t *testing.T) {
	root := t.TempDir()
	type invocation struct {
		containerID string
		helperPath  string
		action      string
		nonce       string
	}
	var calls []invocation
	invoke := func(containerID, helperPath, action, nonce string) ([]byte, error) {
		calls = append(calls, invocation{containerID, helperPath, action, nonce})
		attestation := map[string]string{"acquire": "acquired", "release": "released"}[action]
		return []byte(attestation + ":" + nonce + "\n"), nil
	}
	release, err := acquireCIMaintenanceLock(root, "runner-container", invoke)
	if err != nil {
		t.Fatal(err)
	}
	if err := release(); err != nil {
		t.Fatal(err)
	}
	if len(calls) != 2 || calls[0].action != "acquire" || calls[1].action != "release" {
		t.Fatalf("unexpected helper calls: %+v", calls)
	}
	if calls[0].containerID != "runner-container" || calls[0].helperPath != filepath.Join(root, "scripts", ciMaintenanceProtocolScript) {
		t.Fatalf("maintenance did not use the verified runner/helper: %+v", calls[0])
	}
	if calls[0].nonce != calls[1].nonce || len(calls[0].nonce) != 32 {
		t.Fatalf("nonce was not a stable 128-bit value: %+v", calls)
	}
	if _, err := hex.DecodeString(calls[0].nonce); err != nil {
		t.Fatalf("nonce was not hex encoded: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "shared")); !os.IsNotExist(err) {
		t.Fatal("Gas Town mutated host protocol directories instead of invoking the runner helper")
	}
}

func TestCIMaintenanceLockRejectsBadAcquireAttestationAndReleasesNonce(t *testing.T) {
	root := t.TempDir()
	var released bool
	invoke := func(_, _ string, action, nonce string) ([]byte, error) {
		if action == "acquire" {
			return []byte("acquired:wrong-nonce\n"), nil
		}
		released = true
		return []byte("released:" + nonce + "\n"), nil
	}
	if release, err := acquireCIMaintenanceLock(root, "runner-container", invoke); err == nil || release != nil {
		t.Fatal("invalid acquire attestation was accepted")
	}
	if !released {
		t.Fatal("invalid successful acquire was not released with its nonce")
	}
}

func TestCIMaintenanceReleaseErrorIsSurfaced(t *testing.T) {
	root := t.TempDir()
	invoke := func(_, _ string, action, nonce string) ([]byte, error) {
		if action == "release" {
			return nil, errors.New("runner disappeared")
		}
		return []byte("acquired:" + nonce + "\n"), nil
	}
	release, err := acquireCIMaintenanceLock(root, "runner-container", invoke)
	if err != nil {
		t.Fatal(err)
	}
	if err := release(); err == nil || !strings.Contains(err.Error(), "runner disappeared") {
		t.Fatalf("release failure was not surfaced: %v", err)
	}
}

func TestDetectSafetyTreatsOnlyNonemptyActiveJobDirectoryAsActive(t *testing.T) {
	root := t.TempDir()
	activeJobs := filepath.Join(root, "shared", ".ci-active-jobs")
	if err := os.MkdirAll(activeJobs, 0o755); err != nil {
		t.Fatal(err)
	}
	if state := DetectSafety(root, "ci"); state.ActiveRunner {
		t.Fatalf("empty active-job directory was treated as active: %+v", state)
	}
	writeArtifact(t, root, "shared/.ci-active-jobs/runner-1/.job-active", "run:job\n")
	if state := DetectSafety(root, "ci"); !state.ActiveRunner {
		t.Fatal("nonempty active-job directory did not block CI cleanup")
	}
}

func TestDetectSafetyFailsClosedWhenMarkersCannotBeScanned(t *testing.T) {
	root := filepath.Join(t.TempDir(), "missing")
	state := DetectSafety(root, "ci")
	if !state.ActiveRunner || !contains(state.Reasons, "runner-state-unverified") || !contains(state.Reasons, "mr-state-unverified") {
		t.Fatalf("marker scan error did not fail closed: %+v", state)
	}
}

func TestReportDisk(t *testing.T) {
	root := t.TempDir()
	writeArtifact(t, root, "large/x", strings.Repeat("x", 20))
	writeArtifact(t, root, "small/x", strings.Repeat("x", 5))
	report, err := ReportDisk(root, 1)
	if err != nil {
		t.Fatal(err)
	}
	if report.TotalBytes != 25 || len(report.Entries) != 1 || report.Entries[0].Path != "large" {
		t.Fatalf("unexpected disk report: %+v", report)
	}
}

func TestDirectoryFactsDoesNotSuppressScanErrors(t *testing.T) {
	if _, _, err := directoryFacts(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Fatal("expected missing directory scan to fail")
	}
}

func contains(items []string, want string) bool {
	for _, item := range items {
		if item == want {
			return true
		}
	}
	return false
}
