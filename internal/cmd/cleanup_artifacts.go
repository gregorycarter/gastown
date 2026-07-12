package cmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/steveyegge/gastown/internal/artifact"
	"github.com/steveyegge/gastown/internal/config"
	"github.com/steveyegge/gastown/internal/workspace"
)

var (
	artifactRig               string
	artifactRoot              string
	artifactScope             string
	artifactApply             bool
	artifactJSON              bool
	artifactPaths             []string
	artifactMaxAge            string
	artifactMaxBytes          int64
	artifactVerifyNoActiveMR  bool
	artifactVerifyRunnersIdle bool
	artifactVerifyPolecatIdle bool
	diskReportRoot            string
	diskReportLimit           int
	diskReportJSON            bool
)

var cleanupArtifactsCmd = &cobra.Command{
	Use:   "artifacts",
	Short: "Safely report or clean allowlisted lifecycle artifacts",
	Long: `Evaluate a merged town/rig lifecycle cleanup policy.

The command is a dry run unless --apply is explicit. Every candidate must be
under the selected root, allowlisted, untracked, and outside protected paths.
Dirty worktrees, active merge requests, and active CI job/handoff markers block
deletion. Automatic hooks additionally require lifecycle.cleanup.enabled and
mode=apply in rig settings.`,
	Args: cobra.NoArgs,
	RunE: runCleanupArtifacts,
}

var cleanupDiskCmd = &cobra.Command{
	Use:   "disk",
	Short: "Report the largest immediate disk consumers",
	Args:  cobra.NoArgs,
	RunE:  runCleanupDiskReport,
}

func init() {
	cleanupArtifactsCmd.Flags().StringVar(&artifactRig, "rig", "", "Rig whose lifecycle policy should be used")
	cleanupArtifactsCmd.Flags().StringVar(&artifactRoot, "root", "", "Cleanup root (must remain inside the town workspace)")
	cleanupArtifactsCmd.Flags().StringVar(&artifactScope, "scope", "rig", "Cleanup scope: rig, polecat, ci, or dolt")
	cleanupArtifactsCmd.Flags().BoolVar(&artifactApply, "apply", false, "Apply the cleanup (default is dry-run)")
	cleanupArtifactsCmd.Flags().BoolVar(&artifactJSON, "json", false, "Emit only machine-readable JSON")
	cleanupArtifactsCmd.Flags().StringArrayVar(&artifactPaths, "path", nil, "Override the configured relative allowlist (repeatable)")
	cleanupArtifactsCmd.Flags().StringVar(&artifactMaxAge, "max-age", "", "Override maximum artifact age (for example 168h)")
	cleanupArtifactsCmd.Flags().Int64Var(&artifactMaxBytes, "max-bytes", -1, "Override retained high-water bytes (0 disables size threshold)")
	cleanupArtifactsCmd.Flags().BoolVar(&artifactVerifyNoActiveMR, "verify-no-active-mr", false, "Assert that MR state was checked and no active MR exists")
	cleanupArtifactsCmd.Flags().BoolVar(&artifactVerifyRunnersIdle, "verify-runners-idle", false, "Assert runners and cross-job handoffs were checked idle")
	cleanupArtifactsCmd.Flags().BoolVar(&artifactVerifyPolecatIdle, "verify-polecat-idle", false, "Assert the selected polecat is idle and unassigned")

	cleanupDiskCmd.Flags().StringVar(&diskReportRoot, "root", "", "Report root (must remain inside the town workspace)")
	cleanupDiskCmd.Flags().IntVar(&diskReportLimit, "top", 10, "Number of largest immediate children to show")
	cleanupDiskCmd.Flags().BoolVar(&diskReportJSON, "json", false, "Emit only machine-readable JSON")

	cleanupCmd.AddCommand(cleanupArtifactsCmd, cleanupDiskCmd)
}

func runCleanupArtifacts(cmd *cobra.Command, _ []string) (runErr error) {
	var releaseMaintenance func() error
	defer func() {
		if releaseMaintenance == nil {
			return
		}
		if err := releaseMaintenance(); err != nil {
			runErr = errors.Join(runErr, fmt.Errorf("releasing CI maintenance: %w", err))
		}
	}()

	if err := validateArtifactNumericFlags(artifactMaxBytes, cmd.Flags().Changed("max-bytes"), diskReportLimit); err != nil {
		return err
	}
	townRoot, err := workspace.FindFromCwdOrError()
	if err != nil {
		return fmt.Errorf("finding town workspace: %w", err)
	}
	rigName := artifactRig
	if rigName == "" && artifactScope != "ci" && artifactScope != "dolt" {
		rigName, err = inferRigFromCwd(townRoot)
		if err != nil {
			return fmt.Errorf("select a rig with --rig: %w", err)
		}
	}
	if strings.Contains(rigName, string(filepath.Separator)) || rigName == "." || strings.HasPrefix(rigName, ".") {
		return fmt.Errorf("invalid rig name %q", rigName)
	}
	rigPath := ""
	if rigName != "" {
		rigPath = filepath.Join(townRoot, rigName)
	}
	policy, err := config.ResolveArtifactCleanupPolicy(townRoot, rigPath)
	if err != nil {
		return err
	}
	if len(artifactPaths) > 0 {
		policy.Paths = append([]string(nil), artifactPaths...)
	}
	if artifactMaxAge != "" {
		policy.MaxAge, err = time.ParseDuration(artifactMaxAge)
		if err != nil || policy.MaxAge < 0 {
			return fmt.Errorf("invalid --max-age %q", artifactMaxAge)
		}
	}
	if artifactMaxBytes >= 0 {
		policy.MaxBytes = artifactMaxBytes
	}
	// Explicit manual --apply is the enable switch; config enabled/mode govern
	// unattended hooks, not an operator's one-shot request.
	if artifactApply {
		policy.Enabled = true
	}

	root, err := resolveArtifactRoot(townRoot, rigPath, artifactScope, artifactRoot)
	if err != nil {
		return err
	}
	apply := artifactApply
	if err := validateWorktreeApplyRoot(rigPath, root, artifactScope, apply); err != nil {
		return err
	}
	if err := validateCIApply(townRoot, root, artifactScope, apply, policy); err != nil {
		return err
	}
	if artifactScope == "dolt" {
		policy.Paths = []string{".dolt-data", ".dolt-backup"}
		apply = false // Dolt storage is permanently report-only here.
	}
	if err := policy.Validate(); err != nil {
		return err
	}
	liveRunnerHooksVerified := false
	verifiedRunnerSet := ""
	if apply && artifactScope == "ci" {
		containerID, runnerSet, verifyErr := verifyLiveCIRunnerHooks(root)
		if verifyErr != nil {
			return fmt.Errorf("verifying live CI runner protocol: %w", verifyErr)
		}
		releaseMaintenance, err = artifact.AcquireCIMaintenanceLock(root, containerID)
		if err != nil {
			return err
		}
		liveRunnerHooksVerified = true
		verifiedRunnerSet = runnerSet
	} else {
		liveRunnerHooksVerified = true
	}
	safety := artifact.DetectSafety(root, artifactScope)
	safety.RunnerHooksVerified = safety.RunnerHooksVerified && liveRunnerHooksVerified
	if apply && artifactScope == "ci" && !liveRunnerHooksVerified {
		safety.Reasons = append(safety.Reasons, "live-runner-hooks-unverified")
	}
	requireGit := artifactScope == "rig" || artifactScope == "polecat"
	if artifactVerifyNoActiveMR {
		safety.MRVerified = true
	}
	if artifactVerifyRunnersIdle {
		safety.RunnerVerified = safety.RunnerHooksVerified
	}
	if apply && artifactScope == "polecat" && !artifactVerifyPolecatIdle {
		safety.Reasons = append(safety.Reasons, "polecat-state-unverified")
	}
	safetyCheck := func() artifact.SafetyState {
		state := artifact.DetectSafety(root, artifactScope)
		currentRunnerHooksVerified := liveRunnerHooksVerified
		if apply && artifactScope == "ci" {
			_, currentRunnerSet, verifyErr := verifyLiveCIRunnerHooks(root)
			currentRunnerHooksVerified = verifyErr == nil && currentRunnerSet == verifiedRunnerSet
		}
		state.RunnerHooksVerified = state.RunnerHooksVerified && currentRunnerHooksVerified
		if apply && artifactScope == "ci" && !currentRunnerHooksVerified {
			state.Reasons = append(state.Reasons, "live-runner-hooks-unverified")
		}
		state.MRVerified = artifactVerifyNoActiveMR
		state.RunnerVerified = artifactVerifyRunnersIdle && state.RunnerHooksVerified
		if apply && artifactScope == "polecat" && !artifactVerifyPolecatIdle {
			state.Reasons = append(state.Reasons, "polecat-state-unverified")
		}
		return state
	}
	hookPoint := "manual"
	if artifactScope == "ci" {
		hookPoint = "ci-maintenance"
	}
	protectionRoot := ""
	if artifactScope == "ci" || artifactScope == "dolt" {
		protectionRoot = townRoot
	}
	result, err := artifact.Clean(artifact.Options{
		Root:                      root,
		ProtectionRoot:            protectionRoot,
		Policy:                    policy,
		Apply:                     apply,
		Scope:                     artifactScope,
		HookPoint:                 hookPoint,
		Safety:                    safety,
		RequireGit:                requireGit,
		RequireIgnored:            requireGit,
		RequireMRVerification:     requireGit,
		RequireRunnerVerification: artifactScope == "ci",
		SafetyCheck:               safetyCheck,
	})
	if err != nil {
		return err
	}
	if artifactScope == "dolt" {
		result.Recommendations = []string{
			"Never delete .dolt-data or .dolt-backup directly; verify bd vc status and configured Dolt remotes first.",
			"Treat the hq database as shared control-plane state; take and verify a backup before native Dolt garbage collection.",
			"Use native Dolt GC/backup retention only during a maintenance window with the server quiesced.",
		}
	}
	if err := writeArtifactResult(cmd.OutOrStdout(), result, artifactJSON); err != nil {
		return err
	}
	return artifactApplyError(apply, result)
}

func validateWorktreeApplyRoot(rigPath, root, scope string, apply bool) error {
	if !apply || (scope != "rig" && scope != "polecat") {
		return nil
	}
	if rigPath == "" {
		return fmt.Errorf("--rig is required for %s apply", scope)
	}
	rigCanonical, err := filepath.EvalSymlinks(rigPath)
	if err != nil {
		return err
	}
	actual, err := filepath.EvalSymlinks(root)
	if err != nil {
		return err
	}
	if scope == "rig" {
		expected := filepath.Join(rigCanonical, "mayor", "rig")
		if _, statErr := os.Stat(expected); os.IsNotExist(statErr) {
			expected = rigCanonical
		}
		expected, err = filepath.EvalSymlinks(expected)
		if err != nil {
			return err
		}
		if filepath.Clean(actual) != filepath.Clean(expected) {
			return fmt.Errorf("rig apply must use the canonical rig worktree %q", expected)
		}
		return nil
	}

	gitTop := exec.Command("git", "-C", actual, "rev-parse", "--show-toplevel") //nolint:gosec // fixed executable
	out, err := gitTop.Output()
	if err != nil {
		return fmt.Errorf("verifying polecat worktree root: %w", err)
	}
	top, err := filepath.EvalSymlinks(strings.TrimSpace(string(out)))
	if err != nil || filepath.Clean(top) != filepath.Clean(actual) {
		return fmt.Errorf("polecat apply root must be the worktree top level")
	}
	rel, err := filepath.Rel(rigCanonical, actual)
	if err != nil {
		return err
	}
	parts := strings.Split(filepath.ToSlash(rel), "/")
	if len(parts) < 2 || len(parts) > 3 || parts[0] != "polecats" {
		return fmt.Errorf("polecat apply root must be a registered worktree below %s", filepath.Join(rigCanonical, "polecats"))
	}
	registered, err := rigWorktreeRegistered(rigCanonical, actual)
	if err != nil {
		return fmt.Errorf("verifying registered polecat worktree: %w", err)
	}
	if !registered {
		return fmt.Errorf("polecat apply root is not registered with the selected rig repository")
	}
	return nil
}

func rigWorktreeRegistered(rigPath, candidate string) (bool, error) {
	rigWorktree := filepath.Join(rigPath, "mayor", "rig")
	if _, err := os.Stat(rigWorktree); os.IsNotExist(err) {
		rigWorktree = rigPath
	}
	rigWorktree, err := filepath.EvalSymlinks(rigWorktree)
	if err != nil {
		return false, err
	}
	cmd := exec.Command("git", "-C", rigWorktree, "worktree", "list", "--porcelain", "-z") //nolint:gosec // fixed executable and canonical rig path
	out, err := cmd.Output()
	if err != nil {
		return false, err
	}
	for _, field := range strings.Split(string(out), "\x00") {
		worktreePath, ok := strings.CutPrefix(field, "worktree ")
		if !ok {
			continue
		}
		registered, resolveErr := filepath.EvalSymlinks(worktreePath)
		if resolveErr == nil && filepath.Clean(registered) == filepath.Clean(candidate) {
			return true, nil
		}
	}
	return false, nil
}

type dockerInspectContainer struct {
	ID     string `json:"Id"`
	Name   string `json:"Name"`
	Config struct {
		Env    []string          `json:"Env"`
		Labels map[string]string `json:"Labels"`
	} `json:"Config"`
	State struct {
		Running bool `json:"Running"`
	} `json:"State"`
	Mounts []dockerInspectMount `json:"Mounts"`
}

type dockerInspectMount struct {
	Type        string `json:"Type"`
	Source      string `json:"Source"`
	Destination string `json:"Destination"`
	RW          bool   `json:"RW"`
}

type verifiedCIRunner struct {
	containerID string
	service     string
	workdir     string
	runnerName  string
}

func verifyLiveCIRunnerHooks(root string) (string, string, error) {
	if err := validateRunnerProtocolFiles(root); err != nil {
		return "", "", err
	}
	project := filepath.Base(filepath.Clean(root))
	list := exec.Command("docker", "ps", "--all", //nolint:gosec // fixed executable and arguments
		"--filter", "label=com.docker.compose.project="+project,
		"--format", "{{.ID}}")
	out, err := list.Output()
	if err != nil {
		return "", "", fmt.Errorf("listing live CI runners: %w", err)
	}
	var runners []verifiedCIRunner
	seenWorkdirs := make(map[string]bool)
	seenRunnerNames := make(map[string]bool)
	for _, containerID := range strings.Fields(string(out)) {
		container, inspectErr := inspectDockerContainer(containerID)
		if inspectErr != nil {
			return "", "", inspectErr
		}
		service := container.Config.Labels["com.docker.compose.service"]
		if !isComposeRunnerService(service) {
			continue
		}
		runner, validateErr := validateLiveRunnerContainer(root, container)
		if validateErr != nil {
			return "", "", fmt.Errorf("runner %s: %w", strings.TrimPrefix(container.Name, "/"), validateErr)
		}
		if err := verifyContainerRunnerProtocol(root, container.ID); err != nil {
			return "", "", fmt.Errorf("runner %s: %w", strings.TrimPrefix(container.Name, "/"), err)
		}
		if seenWorkdirs[runner.workdir] {
			return "", "", fmt.Errorf("multiple live runners claim workdir %s", runner.workdir)
		}
		if seenRunnerNames[runner.runnerName] {
			return "", "", fmt.Errorf("multiple live runners claim runner name %s", runner.runnerName)
		}
		seenWorkdirs[runner.workdir] = true
		seenRunnerNames[runner.runnerName] = true
		runners = append(runners, runner)
	}
	configured, err := countRunnerWorkdirs(root)
	if err != nil {
		return "", "", err
	}
	if len(runners) == 0 || len(runners) != configured {
		return "", "", fmt.Errorf("live runner set (%d) does not match configured workdirs (%d)", len(runners), configured)
	}
	sort.Slice(runners, func(i, j int) bool { return runners[i].service < runners[j].service })
	identity := make([]string, 0, len(runners))
	for _, runner := range runners {
		identity = append(identity, strings.Join([]string{runner.service, runner.containerID, runner.workdir, runner.runnerName}, "\x00"))
	}
	return runners[0].containerID, strings.Join(identity, "\x1e"), nil
}

func inspectDockerContainer(containerID string) (dockerInspectContainer, error) {
	inspect := exec.Command("docker", "inspect", containerID) //nolint:gosec // id comes from exact Compose project listing
	out, err := inspect.Output()
	if err != nil {
		return dockerInspectContainer{}, fmt.Errorf("inspecting CI runner container %s: %w", containerID, err)
	}
	var containers []dockerInspectContainer
	if err := json.Unmarshal(out, &containers); err != nil || len(containers) != 1 {
		return dockerInspectContainer{}, fmt.Errorf("decoding CI runner container %s inspection", containerID)
	}
	return containers[0], nil
}

func isComposeRunnerService(service string) bool {
	parts := strings.Split(service, "-")
	if len(parts) < 2 || parts[len(parts)-2] != "runner" {
		return false
	}
	n, err := strconv.Atoi(parts[len(parts)-1])
	return err == nil && n > 0
}

func validateLiveRunnerContainer(root string, container dockerInspectContainer) (verifiedCIRunner, error) {
	labels := container.Config.Labels
	service := labels["com.docker.compose.service"]
	project := filepath.Base(filepath.Clean(root))
	if !container.State.Running {
		return verifiedCIRunner{}, fmt.Errorf("Compose runner container is not running")
	}
	if labels["com.docker.compose.project"] != project ||
		filepath.Clean(labels["com.docker.compose.project.working_dir"]) != filepath.Clean(root) ||
		filepath.Clean(labels["com.docker.compose.project.config_files"]) != filepath.Join(filepath.Clean(root), "docker-compose.yml") ||
		!strings.EqualFold(labels["com.docker.compose.oneoff"], "false") ||
		!isComposeRunnerService(service) {
		return verifiedCIRunner{}, fmt.Errorf("container is not an exact service from the canonical CI Compose project")
	}
	env := dockerEnvironment(container.Config.Env)
	values := []string{
		env["RUNNER_WORKDIR"], env["RUNNER_NAME"], env["CI_JOB_HOOK_PROTOCOL_VERSION"],
		env["ACTIONS_RUNNER_HOOK_JOB_STARTED"], env["ACTIONS_RUNNER_HOOK_JOB_COMPLETED"],
	}
	expectedWorkdir := filepath.Join(filepath.Clean(root), "work", service)
	if filepath.Clean(values[0]) != expectedWorkdir {
		return verifiedCIRunner{}, fmt.Errorf("runner workdir is not the exact canonical service workdir")
	}
	if err := validateLiveRunnerProtocol(root, values); err != nil {
		return verifiedCIRunner{}, err
	}
	for _, mount := range []struct {
		source   string
		writable bool
	}{
		{source: filepath.Join(root, "scripts"), writable: false},
		{source: filepath.Join(root, "shared"), writable: true},
		{source: expectedWorkdir, writable: true},
	} {
		if err := validateExactBindMount(container.Mounts, mount.source, mount.source, mount.writable); err != nil {
			return verifiedCIRunner{}, err
		}
	}
	return verifiedCIRunner{
		containerID: container.ID,
		service:     service,
		workdir:     expectedWorkdir,
		runnerName:  values[1],
	}, nil
}

func dockerEnvironment(entries []string) map[string]string {
	env := make(map[string]string, len(entries))
	for _, entry := range entries {
		key, value, ok := strings.Cut(entry, "=")
		if ok {
			env[key] = value
		}
	}
	return env
}

func validateExactBindMount(mounts []dockerInspectMount, source, destination string, writable bool) error {
	mode := "read-only"
	if writable {
		mode = "writable"
	}
	found := 0
	for _, mount := range mounts {
		if filepath.Clean(mount.Destination) != filepath.Clean(destination) {
			continue
		}
		found++
		if mount.Type != "bind" || filepath.Clean(mount.Source) != filepath.Clean(source) || mount.RW != writable {
			return fmt.Errorf("runner mount %s is not the exact %s bind", destination, mode)
		}
	}
	if found != 1 {
		return fmt.Errorf("runner must have exactly one bind mount at %s", destination)
	}
	return nil
}

func countRunnerWorkdirs(root string) (int, error) {
	entries, err := os.ReadDir(filepath.Join(root, "work"))
	if err != nil {
		return 0, fmt.Errorf("reading runner workdirs: %w", err)
	}
	count := 0
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		workdir := filepath.Join(root, "work", entry.Name())
		isRunner := false
		for _, marker := range []string{"_actions", "_temp", ".pnpm-store", ".ci-job-hooks-installed"} {
			if _, err := os.Lstat(filepath.Join(workdir, marker)); err == nil {
				isRunner = true
				break
			}
		}
		if isRunner {
			count++
		}
	}
	return count, nil
}

func verifyContainerRunnerProtocol(root, containerID string) error {
	paths := []string{
		filepath.Join(root, "scripts", "pre-job-workspace-heal.sh"),
		filepath.Join(root, "scripts", "ci-job-completed.sh"),
		filepath.Join(root, "scripts", "ci-docker-lib.sh"),
		filepath.Join(root, "scripts", "ci-maintenance-protocol.sh"),
	}
	for _, filePath := range paths {
		hostData, err := os.ReadFile(filePath)
		if err != nil {
			return fmt.Errorf("reading host protocol file %s: %w", filepath.Base(filePath), err)
		}
		containerData := exec.Command("docker", "exec", containerID, "cat", filePath) //nolint:gosec // id from labeled docker ps, fixed cat path
		out, err := containerData.Output()
		if err != nil || string(out) != string(hostData) {
			return fmt.Errorf("container does not see current host protocol file %s", filepath.Base(filePath))
		}
	}
	for _, script := range paths {
		check := exec.Command("docker", "exec", containerID, "test", "-x", script) //nolint:gosec // id from labeled docker ps
		if err := check.Run(); err != nil {
			return fmt.Errorf("container protocol file %s is not executable", filepath.Base(script))
		}
	}
	return nil
}

func validateLiveRunnerProtocol(root string, values []string) error {
	if len(values) != 5 {
		return fmt.Errorf("incomplete runner hook metadata")
	}
	workdir := filepath.Clean(values[0])
	workRoot := filepath.Join(root, "work")
	if !pathInside(workRoot, workdir) {
		return fmt.Errorf("runner workdir is outside canonical work root")
	}
	if values[1] == "" || values[2] != artifact.CIHookProtocolVersion {
		return fmt.Errorf("runner hook protocol version is missing or stale")
	}
	expectedStart := filepath.Join(root, "scripts", "pre-job-workspace-heal.sh")
	expectedCompleted := filepath.Join(root, "scripts", "ci-job-completed.sh")
	if filepath.Clean(values[3]) != expectedStart || filepath.Clean(values[4]) != expectedCompleted {
		return fmt.Errorf("runner lifecycle hook paths are not installed")
	}
	if err := validateRunnerProtocolFiles(root); err != nil {
		return err
	}
	sentinel, err := os.ReadFile(filepath.Join(workdir, ".ci-job-hooks-installed"))
	if err != nil || string(sentinel) != artifact.CIHookProtocolVersion+"\n" {
		return fmt.Errorf("runner hook deployment sentinel is missing or stale")
	}
	return nil
}

func validateRunnerProtocolFiles(root string) error {
	required := map[string][]string{
		"pre-job-workspace-heal.sh": {"ci_mark_runner_job_active"},
		"ci-job-completed.sh":       {"ci_clear_runner_job_active"},
		"ci-docker-lib.sh":          {`CI_JOB_HOOK_PROTOCOL_REQUIRED="mkdir-v1"`, ".ci-active-jobs", ".ci-protocol-mutex"},
		"ci-maintenance-protocol.sh": {
			"ci-docker-lib.sh", "active_jobs_exist", "active_handoffs_exist", "acquire)", "release)",
		},
	}
	for name, markers := range required {
		filePath := filepath.Join(root, "scripts", name)
		info, err := os.Lstat(filePath)
		if err != nil || !info.Mode().IsRegular() || info.Mode()&0o111 == 0 {
			return fmt.Errorf("runner protocol file %s is missing, non-regular, or non-executable", name)
		}
		data, err := os.ReadFile(filePath)
		if err != nil {
			return fmt.Errorf("reading runner protocol file %s: %w", name, err)
		}
		for _, marker := range markers {
			if !strings.Contains(string(data), marker) {
				return fmt.Errorf("runner protocol file %s does not implement %s", name, artifact.CIHookProtocolVersion)
			}
		}
	}
	return nil
}

func pathInside(root, candidate string) bool {
	rel, err := filepath.Rel(root, candidate)
	return err == nil && rel != "." && rel != ".." && !filepath.IsAbs(rel) && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

func validateCIApply(townRoot, root, scope string, apply bool, policy artifact.Policy) error {
	if !apply || scope != "ci" {
		return nil
	}
	expected, err := filepath.EvalSymlinks(filepath.Join(townRoot, "ci-runner"))
	if err != nil {
		return fmt.Errorf("resolving canonical CI maintenance root: %w", err)
	}
	actual, err := filepath.EvalSymlinks(root)
	if err != nil {
		return fmt.Errorf("resolving requested CI maintenance root: %w", err)
	}
	if filepath.Clean(actual) != filepath.Clean(expected) {
		return fmt.Errorf("CI apply must use the canonical runner root %q", expected)
	}
	if !policy.OnCIMaintenance {
		return fmt.Errorf("CI apply requires lifecycle.cleanup.on_ci_maintenance=true in town settings")
	}
	return nil
}

func artifactApplyError(apply bool, result artifact.Result) error {
	if apply && (result.Refused || result.ApplyIncomplete) {
		return fmt.Errorf("artifact cleanup apply was refused or incomplete; inspect skipped paths and refusal reasons")
	}
	return nil
}

func validateArtifactNumericFlags(maxBytes int64, maxBytesSet bool, top int) error {
	if maxBytesSet && maxBytes < 0 {
		return fmt.Errorf("--max-bytes must not be negative")
	}
	if top <= 0 {
		return fmt.Errorf("--top must be greater than zero")
	}
	return nil
}

func resolveArtifactRoot(townRoot, rigPath, scope, explicit string) (string, error) {
	switch scope {
	case "rig", "polecat", "ci", "dolt":
	default:
		return "", fmt.Errorf("invalid cleanup scope %q", scope)
	}
	var root string
	if explicit != "" {
		var err error
		root, err = filepath.Abs(explicit)
		if err != nil {
			return "", err
		}
	} else {
		switch scope {
		case "rig":
			if rigPath == "" {
				return "", fmt.Errorf("--rig is required for rig scope")
			}
			root = filepath.Join(rigPath, "mayor", "rig")
			if _, err := os.Stat(root); os.IsNotExist(err) {
				root = rigPath
			}
		case "polecat":
			return "", fmt.Errorf("polecat scope requires an explicit --root worktree")
		case "ci":
			root = filepath.Join(townRoot, "ci-runner")
		case "dolt":
			root = townRoot
		}
	}
	rootCanonical, err := filepath.EvalSymlinks(root)
	if err != nil {
		return "", fmt.Errorf("resolving cleanup root: %w", err)
	}
	townCanonical, err := filepath.EvalSymlinks(townRoot)
	if err != nil {
		return "", fmt.Errorf("resolving town root: %w", err)
	}
	rel, err := filepath.Rel(townCanonical, rootCanonical)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
		return "", fmt.Errorf("cleanup root %q is outside town workspace %q", root, townRoot)
	}
	if rootCanonical == townCanonical && scope != "dolt" {
		return "", fmt.Errorf("cleanup root may not be the town root for scope %q", scope)
	}
	if scope == "rig" || scope == "polecat" {
		if rigPath == "" {
			return "", fmt.Errorf("--rig is required for %s scope", scope)
		}
		rigCanonical, err := filepath.EvalSymlinks(rigPath)
		if err != nil {
			return "", fmt.Errorf("resolving rig root: %w", err)
		}
		rigRel, err := filepath.Rel(rigCanonical, rootCanonical)
		if err != nil || rigRel == ".." || strings.HasPrefix(rigRel, ".."+string(filepath.Separator)) || filepath.IsAbs(rigRel) {
			return "", fmt.Errorf("cleanup root %q is outside selected rig %q", root, rigPath)
		}
	}
	return rootCanonical, nil
}

func writeArtifactResult(w io.Writer, result artifact.Result, asJSON bool) error {
	if asJSON {
		encoder := json.NewEncoder(w)
		return encoder.Encode(result)
	}
	mode := "dry-run"
	if !result.DryRun {
		mode = "apply"
	}
	fmt.Fprintf(w, "Artifact cleanup (%s, scope=%s, hook=%s)\n", mode, result.Scope, result.HookPoint)
	fmt.Fprintf(w, "  root: %s\n  considered: %d bytes\n  eligible: %d bytes\n  freed: %d bytes\n", result.Root, result.BytesConsidered, result.BytesEligible, result.BytesFreed)
	if result.Refused {
		fmt.Fprintf(w, "  refused: %s\n", strings.Join(result.RefusalReasons, ", "))
	}
	for _, path := range result.PathsCleaned {
		fmt.Fprintf(w, "  %s %s (%d bytes)\n", path.Action, path.Path, path.Bytes)
	}
	for _, path := range result.PathsSkipped {
		fmt.Fprintf(w, "  skipped %s: %s\n", path.Path, path.Reason)
	}
	for _, recommendation := range result.Recommendations {
		fmt.Fprintf(w, "  recommendation: %s\n", recommendation)
	}
	return nil
}

func runCleanupDiskReport(cmd *cobra.Command, _ []string) error {
	if err := validateArtifactNumericFlags(artifactMaxBytes, false, diskReportLimit); err != nil {
		return err
	}
	townRoot, err := workspace.FindFromCwdOrError()
	if err != nil {
		return fmt.Errorf("finding town workspace: %w", err)
	}
	root := diskReportRoot
	if root == "" {
		root = townRoot
	}
	root, err = resolveArtifactRoot(townRoot, "", "dolt", root)
	if err != nil {
		return err
	}
	report, err := artifact.ReportDisk(root, diskReportLimit)
	if err != nil {
		return err
	}
	if diskReportJSON {
		return json.NewEncoder(cmd.OutOrStdout()).Encode(report)
	}
	fmt.Fprintf(cmd.OutOrStdout(), "Disk usage under %s (%d bytes)\n", report.Root, report.TotalBytes)
	for _, entry := range report.Entries {
		fmt.Fprintf(cmd.OutOrStdout(), "  %12d  %s\n", entry.Bytes, entry.Path)
	}
	return nil
}
