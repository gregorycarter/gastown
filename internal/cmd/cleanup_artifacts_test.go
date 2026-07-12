package cmd

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/steveyegge/gastown/internal/artifact"
)

func TestCleanupArtifactsIsChildWithoutReplacingProcessCleanup(t *testing.T) {
	if cleanupCmd.RunE == nil {
		t.Fatal("existing process cleanup behavior was replaced")
	}
	found, _, err := cleanupCmd.Find([]string{"artifacts"})
	if err != nil || found != cleanupArtifactsCmd {
		t.Fatalf("artifact child command missing: found=%v err=%v", found, err)
	}
	found, _, err = cleanupCmd.Find([]string{"disk"})
	if err != nil || found != cleanupDiskCmd {
		t.Fatalf("disk child command missing: found=%v err=%v", found, err)
	}
}

func TestWriteArtifactResultJSONHasNoProseNoise(t *testing.T) {
	result := artifact.Result{
		Root: "/tmp/rig", Scope: "rig", HookPoint: "manual", DryRun: true,
		PathsCleaned: []artifact.PathResult{}, PathsSkipped: []artifact.PathResult{},
	}
	var out bytes.Buffer
	if err := writeArtifactResult(&out, result, true); err != nil {
		t.Fatal(err)
	}
	var decoded artifact.Result
	if err := json.Unmarshal(out.Bytes(), &decoded); err != nil {
		t.Fatalf("output was not one JSON document: %q: %v", out.String(), err)
	}
	if strings.Contains(out.String(), "Artifact cleanup") || decoded.HookPoint != "manual" {
		t.Fatalf("JSON output contained prose or lost fields: %q", out.String())
	}
}

func TestResolveArtifactRootRejectsOutsideAndBroadRoots(t *testing.T) {
	town := t.TempDir()
	rig := filepath.Join(town, "rig")
	root := filepath.Join(rig, "mayor", "rig")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	got, err := resolveArtifactRoot(town, rig, "rig", root)
	want, canonicalErr := filepath.EvalSymlinks(root)
	if canonicalErr != nil {
		t.Fatal(canonicalErr)
	}
	if err != nil || got != want {
		t.Fatalf("valid root failed: got=%q err=%v", got, err)
	}
	if _, err := resolveArtifactRoot(town, rig, "rig", t.TempDir()); err == nil {
		t.Fatal("outside root should be rejected")
	}
	if _, err := resolveArtifactRoot(town, rig, "rig", town); err == nil {
		t.Fatal("town root should be rejected for mutating scope")
	}
	otherRig := filepath.Join(town, "other", "mayor", "rig")
	if err := os.MkdirAll(otherRig, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := resolveArtifactRoot(town, rig, "rig", otherRig); err == nil {
		t.Fatal("per-rig policy should not authorize a different rig root")
	}
	if _, err := resolveArtifactRoot(town, rig, "unknown", root); err == nil {
		t.Fatal("unknown explicit-root scope should be rejected")
	}
}

func TestValidateArtifactNumericFlags(t *testing.T) {
	if err := validateArtifactNumericFlags(-1, false, 10); err != nil {
		t.Fatalf("internal max-bytes sentinel should be accepted: %v", err)
	}
	if err := validateArtifactNumericFlags(-1, true, 10); err == nil {
		t.Fatal("explicit negative --max-bytes should fail")
	}
	if err := validateArtifactNumericFlags(0, true, 0); err == nil {
		t.Fatal("non-positive --top should fail")
	}
}

func TestValidateCIApplyRequiresCanonicalRootAndPolicy(t *testing.T) {
	town := t.TempDir()
	canonical := filepath.Join(town, "ci-runner")
	fixture := filepath.Join(town, "fixture")
	for _, dir := range []string{canonical, fixture} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	policy := artifact.DefaultPolicy()
	policy.OnCIMaintenance = true
	if err := validateCIApply(town, fixture, "ci", true, policy); err == nil {
		t.Fatal("CI apply accepted a narrowed fixture root")
	}
	policy.OnCIMaintenance = false
	if err := validateCIApply(town, canonical, "ci", true, policy); err == nil {
		t.Fatal("CI apply ignored disabled on_ci_maintenance policy")
	}
	policy.OnCIMaintenance = true
	if err := validateCIApply(town, canonical, "ci", true, policy); err != nil {
		t.Fatalf("canonical configured CI apply was rejected: %v", err)
	}
}

func TestValidateWorktreeApplyRootRejectsNarrowedSubdirectory(t *testing.T) {
	rig := t.TempDir()
	worktree := filepath.Join(rig, "mayor", "rig")
	narrow := filepath.Join(worktree, "data")
	if err := os.MkdirAll(narrow, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := validateWorktreeApplyRoot(rig, narrow, "rig", true); err == nil {
		t.Fatal("rig apply accepted a narrowed protected root")
	}
	if err := validateWorktreeApplyRoot(rig, worktree, "rig", true); err != nil {
		t.Fatalf("canonical rig worktree was rejected: %v", err)
	}
}

func TestValidatePolecatApplyRootRequiresRigRegisteredWorktree(t *testing.T) {
	rig := t.TempDir()
	mainWorktree := filepath.Join(rig, "mayor", "rig")
	if err := os.MkdirAll(mainWorktree, 0o755); err != nil {
		t.Fatal(err)
	}
	runCleanupGit(t, mainWorktree, "init")
	runCleanupGit(t, mainWorktree, "config", "user.email", "test@example.com")
	runCleanupGit(t, mainWorktree, "config", "user.name", "Test User")
	if err := os.WriteFile(filepath.Join(mainWorktree, "README.md"), []byte("fixture\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	runCleanupGit(t, mainWorktree, "add", "README.md")
	runCleanupGit(t, mainWorktree, "commit", "-m", "fixture")

	registered := filepath.Join(rig, "polecats", "amber")
	if err := os.MkdirAll(filepath.Dir(registered), 0o755); err != nil {
		t.Fatal(err)
	}
	runCleanupGit(t, mainWorktree, "worktree", "add", "-b", "polecat/amber", registered)
	if err := validateWorktreeApplyRoot(rig, registered, "polecat", true); err != nil {
		t.Fatalf("registered polecat worktree was rejected: %v", err)
	}

	unregistered := filepath.Join(rig, "polecats", "unregistered")
	if err := os.MkdirAll(unregistered, 0o755); err != nil {
		t.Fatal(err)
	}
	runCleanupGit(t, unregistered, "init")
	if err := validateWorktreeApplyRoot(rig, unregistered, "polecat", true); err == nil {
		t.Fatal("independent repository under polecats was accepted as registered")
	}
}

func runCleanupGit(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
	}
}

func TestApplyIncompleteIsMachineReadableAndFailsAutomation(t *testing.T) {
	result := artifact.Result{
		Root: "/tmp/rig", Scope: "rig", HookPoint: "manual", ApplyIncomplete: true,
		PathsCleaned: []artifact.PathResult{}, PathsSkipped: []artifact.PathResult{{Path: "dist", Action: "skipped", Reason: "remove-failed"}},
	}
	var out bytes.Buffer
	if err := writeArtifactResult(&out, result, true); err != nil {
		t.Fatal(err)
	}
	var decoded artifact.Result
	if err := json.Unmarshal(out.Bytes(), &decoded); err != nil || !decoded.ApplyIncomplete {
		t.Fatalf("incomplete apply was not machine-readable: %q err=%v", out.String(), err)
	}
	if err := artifactApplyError(true, result); err == nil {
		t.Fatal("incomplete apply should return a non-zero command error")
	}
	if err := artifactApplyError(false, result); err != nil {
		t.Fatalf("dry-run reporting should remain successful: %v", err)
	}
}

func TestValidateLiveRunnerProtocol(t *testing.T) {
	root := t.TempDir()
	workdir := filepath.Join(root, "work", "dd-runner-1")
	installTestRunnerProtocol(t, root, workdir)
	values := []string{
		workdir, "dd-runner-1", artifact.CIHookProtocolVersion,
		filepath.Join(root, "scripts", "pre-job-workspace-heal.sh"),
		filepath.Join(root, "scripts", "ci-job-completed.sh"),
	}
	if err := validateLiveRunnerProtocol(root, values); err != nil {
		t.Fatalf("valid live runner protocol was rejected: %v", err)
	}
	values[2] = "stale-v0"
	if err := validateLiveRunnerProtocol(root, values); err == nil {
		t.Fatal("stale live runner protocol was accepted")
	}
	values[2] = artifact.CIHookProtocolVersion
	values[3] = "/bogus/pre-job-workspace-heal.sh"
	if err := validateLiveRunnerProtocol(root, values); err == nil {
		t.Fatal("same-named hook outside the canonical root was accepted")
	}
}

func TestValidateLiveRunnerContainerRequiresExactComposeIdentityAndBinds(t *testing.T) {
	root := t.TempDir()
	service := "dd-runner-1"
	workdir := filepath.Join(root, "work", service)
	installTestRunnerProtocol(t, root, workdir)
	var container dockerInspectContainer
	container.ID = "container-id"
	container.Name = "/ci-runner-dd-runner-1-1"
	container.State.Running = true
	container.Config.Labels = map[string]string{
		"com.docker.compose.project":              filepath.Base(root),
		"com.docker.compose.project.working_dir":  root,
		"com.docker.compose.project.config_files": filepath.Join(root, "docker-compose.yml"),
		"com.docker.compose.oneoff":               "False",
		"com.docker.compose.service":              service,
	}
	container.Config.Env = []string{
		"RUNNER_WORKDIR=" + workdir,
		"RUNNER_NAME=gastown-dd-runner-1",
		"CI_JOB_HOOK_PROTOCOL_VERSION=" + artifact.CIHookProtocolVersion,
		"ACTIONS_RUNNER_HOOK_JOB_STARTED=" + filepath.Join(root, "scripts", "pre-job-workspace-heal.sh"),
		"ACTIONS_RUNNER_HOOK_JOB_COMPLETED=" + filepath.Join(root, "scripts", "ci-job-completed.sh"),
	}
	container.Mounts = []dockerInspectMount{
		{Type: "bind", Source: filepath.Join(root, "scripts"), Destination: filepath.Join(root, "scripts"), RW: false},
		{Type: "bind", Source: filepath.Join(root, "shared"), Destination: filepath.Join(root, "shared"), RW: true},
		{Type: "bind", Source: workdir, Destination: workdir, RW: true},
	}
	if _, err := validateLiveRunnerContainer(root, container); err != nil {
		t.Fatalf("exact runner container was rejected: %v", err)
	}

	container.Mounts[1].Source = filepath.Join(root, "lookalike-shared")
	if _, err := validateLiveRunnerContainer(root, container); err == nil {
		t.Fatal("runner with a lookalike shared bind source was accepted")
	}
	container.Mounts[1].Source = filepath.Join(root, "shared")
	container.Mounts[0].RW = true
	if _, err := validateLiveRunnerContainer(root, container); err == nil {
		t.Fatal("runner with writable protocol scripts was accepted")
	}
	container.Mounts[0].RW = false
	container.Config.Labels["com.docker.compose.project.working_dir"] = filepath.Join(root, "other")
	if _, err := validateLiveRunnerContainer(root, container); err == nil {
		t.Fatal("runner from a same-named noncanonical Compose project was accepted")
	}
}

func TestComposeRunnerServiceNamesAreExact(t *testing.T) {
	for _, service := range []string{"runner-1", "dd-runner-2", "dardasha-runner-3"} {
		if !isComposeRunnerService(service) {
			t.Fatalf("valid runner service %q was rejected", service)
		}
	}
	for _, service := range []string{"runner", "buildkit_runner-1", "runner-zero", "runner-0", "not-a-runner-1-extra"} {
		if isComposeRunnerService(service) {
			t.Fatalf("non-runner service %q was accepted", service)
		}
	}
}

func installTestRunnerProtocol(t *testing.T, root, workdir string) {
	t.Helper()
	if err := os.MkdirAll(workdir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workdir, ".ci-job-hooks-installed"), []byte(artifact.CIHookProtocolVersion+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	protocolFiles := map[string]string{
		"pre-job-workspace-heal.sh":  "ci_mark_runner_job_active",
		"ci-job-completed.sh":        "ci_clear_runner_job_active",
		"ci-docker-lib.sh":           "CI_JOB_HOOK_PROTOCOL_REQUIRED=\"mkdir-v1\"\n.ci-active-jobs\n.ci-protocol-mutex",
		"ci-maintenance-protocol.sh": "ci-docker-lib.sh\nactive_jobs_exist\nactive_handoffs_exist\nacquire)\nrelease)",
	}
	for name, body := range protocolFiles {
		path := filepath.Join(root, "scripts", name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
}
