package cmd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/steveyegge/gastown/internal/beads"
)

func checkRoleLocation(info RoleInfo) error {
	actual := detectRole(info.WorkDir, info.TownRoot)
	if info.Role == RoleUnknown || info.Home == "" {
		return fmt.Errorf("cannot verify an unknown role")
	}
	if actual.Role != info.Role || actual.Rig != info.Rig || actual.Polecat != info.Polecat {
		return fmt.Errorf("identity mismatch: environment=%s/%s/%s cwd=%s/%s/%s", info.Role, info.Rig, info.Polecat, actual.Role, actual.Rig, actual.Polecat)
	}
	home, err := filepath.EvalSymlinks(info.Home)
	if err != nil {
		return err
	}
	cwd, err := filepath.EvalSymlinks(info.WorkDir)
	if err != nil {
		return err
	}
	rel, err := filepath.Rel(home, cwd)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("cwd is outside canonical role home %s", home)
	}
	return nil
}

func primeCheckCommand(cwd, name string, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, name, args...)
	command.Dir = cwd
	command.WaitDelay = time.Second
	out, err := command.Output()
	if err != nil {
		return nil, fmt.Errorf("%s state read failed: %w", name, err)
	}
	return out, nil
}

func checkWorkIdentity(info RoleInfo, branch, _ string, issue *beads.Issue) error {
	if issue == nil {
		return fmt.Errorf("work bead is missing")
	}
	if info.Role == RolePolecat {
		if parseBranchName(branch).Issue != issue.ID && branch != "polecat/"+issue.ID {
			return fmt.Errorf("branch %q does not identify work bead %s", branch, issue.ID)
		}
		actor := strings.Trim(getAgentIdentity(info), "/")
		if strings.Trim(issue.Assignee, "/") != actor {
			return fmt.Errorf("bead %s is assigned to %q, not %q", issue.ID, issue.Assignee, actor)
		}
		if issue.Status != "in_progress" && issue.Status != beads.StatusHooked {
			return fmt.Errorf("bead %s is %s, not active work", issue.ID, issue.Status)
		}
	}
	if info.Role == RoleRefinery && issue.Type == "merge-request" {
		fields := beads.ParseMRFields(issue)
		if fields == nil || fields.Branch == "" || len(fields.CommitSHA) != 40 {
			return fmt.Errorf("MR has no branch and complete commit_sha receipt")
		}
		if _, err := hex.DecodeString(fields.CommitSHA); err != nil {
			return fmt.Errorf("MR commit_sha is not hexadecimal")
		}
		// The refinery normally starts on main or detached HEAD. The lander
		// validates submitted and rebased commits; this is not a new merge gate.
	}
	return nil
}

func runPrimeCheck(cmd *cobra.Command, info RoleInfo, beadID string) error {
	if err := checkRoleLocation(info); err != nil {
		return err
	}
	if err := ensureRoleWorktreeIntegrity(info.WorkDir, info.TownRoot, info.Role); err != nil {
		return err
	}
	readGit := func(args ...string) (string, error) {
		out, err := primeCheckCommand(info.WorkDir, "git", args...)
		return strings.TrimSpace(string(out)), err
	}
	branch, err := readGit("branch", "--show-current")
	if err != nil {
		return err
	}
	head, err := readGit("rev-parse", "HEAD")
	if err != nil {
		return err
	}
	status, err := readGit("status", "--porcelain=v1", "--untracked-files=normal")
	if err != nil {
		return err
	}
	if beadID == "" && info.Role == RolePolecat {
		beadID = parseBranchName(branch).Issue
		if beadID == "" && strings.HasPrefix(branch, "polecat/") && strings.Count(branch, "/") == 1 {
			beadID = strings.TrimPrefix(branch, "polecat/")
		}
		if beadID == "" {
			return fmt.Errorf("worker branch has no work bead; specify --work-bead")
		}
	}
	result := map[string]interface{}{"ok": true, "role": getAgentIdentity(info), "cwd": info.WorkDir, "branch": branch, "head": head, "changed_paths": 0, "checked_at": time.Now().UTC().Format(time.RFC3339)}
	if status != "" {
		result["changed_paths"] = len(strings.Split(status, "\n"))
	}
	if beadID != "" {
		dir := beads.ResolveHookDir(info.TownRoot, beadID, rigBeadsRoot(info))
		out, err := primeCheckCommand(info.WorkDir, "bd", "--readonly", "-C", dir, "show", beadID, "--json")
		if err != nil {
			return err
		}
		var issues []beads.Issue
		if err := json.Unmarshal(out, &issues); err != nil || len(issues) != 1 {
			return fmt.Errorf("expected exactly one work bead from %s", dir)
		}
		issue := &issues[0]
		if issue.ID != beadID {
			return fmt.Errorf("bead lookup returned a different ID")
		}
		if err := checkWorkIdentity(info, branch, head, issue); err != nil {
			return err
		}
		result["bead"] = map[string]string{"id": issue.ID, "status": issue.Status, "assignee": issue.Assignee, "updated_at": issue.UpdatedAt}
		if fields := beads.ParseMRFields(issue); issue.Type == "merge-request" && fields != nil {
			result["mr"] = map[string]string{"branch": fields.Branch, "commit_sha": fields.CommitSHA, "source_issue": fields.SourceIssue}
		}
	}
	// Content hashes let callers detect policy changes without ingesting the
	// documents on every write. No memory, mail, markers or hook state changes.
	hashes := map[string]string{}
	repoRoot, err := readGit("rev-parse", "--show-toplevel")
	if err != nil {
		return err
	}
	for _, path := range []string{filepath.Join(info.TownRoot, "CLAUDE.md"), filepath.Join(repoRoot, "AGENTS.md"), filepath.Join(info.TownRoot, ".beads", "formulas", "mol-"+string(info.Role)+"-patrol.formula.toml")} {
		if data, err := os.ReadFile(path); err == nil {
			sum := sha256.Sum256(data)
			hashes[filepath.Base(path)] = hex.EncodeToString(sum[:8])
		}
	}
	result["policy_hashes"] = hashes
	return json.NewEncoder(cmd.OutOrStdout()).Encode(result)
}
