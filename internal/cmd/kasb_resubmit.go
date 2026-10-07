package cmd

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/steveyegge/gastown/internal/beads"
	"github.com/steveyegge/gastown/internal/workspace"
)

func kasbResubmittedDescription(mr *beads.Issue, head string) (string, error) {
	if !mqResumeSHA.MatchString(head) {
		return "", fmt.Errorf("invalid submitted head")
	}
	lines := strings.Split(mr.Description, "\n")
	count := 0
	for i, line := range lines {
		key, _, ok := strings.Cut(line, ":")
		key = strings.ReplaceAll(strings.ToLower(strings.TrimSpace(key)), "-", "_")
		if ok && (key == "commit_sha" || key == "commitsha") {
			lines[i] = "commit_sha: " + head
			count++
		}
	}
	if count != 1 {
		return "", fmt.Errorf("ambiguous submitted head")
	}
	return strings.Join(lines, "\n"), nil
}

func init() {
	mqCmd.AddCommand(&cobra.Command{Use: "resubmit-recovery <rig> <mr-id>", Short: "Reopen the original Kasb MR after verifying its new patch receipt", Args: cobra.ExactArgs(2), RunE: func(cmd *cobra.Command, args []string) error {
		if args[0] != "kasb_core" {
			return fmt.Errorf("use the rig's existing resubmission path")
		}
		town, err := workspace.FindFromCwdOrError()
		if err != nil {
			return err
		}
		b := mqResumeBeads(town, args[0])
		state, err := validateMQResumeRecords(args[0], args[1], b.Show)
		if err != nil {
			return err
		}
		r := state.Record
		actor := r.Rig + "/polecats/" + r.Worker
		role := os.Getenv("GT_ROLE")
		if role != "" && role != actor {
			if err := validateMQResumeCaller(role, os.Getenv("GT_RIG"), os.Getenv("BD_ACTOR")); err != nil {
				return err
			}
		}
		if state.Source.Assignee != actor {
			return fmt.Errorf("original worker no longer owns source")
		}
		unlock, err := tryAcquireSlingBeadLock(town, r.Source)
		if err != nil {
			return err
		}
		defer unlock()
		workerUnlock, err := tryAcquireSlingAssigneeLock(town, actor)
		if err != nil {
			return err
		}
		defer workerUnlock()
		root := filepath.Join(town, r.Rig, "polecats", r.Worker, r.Rig)
		real, err := filepath.EvalSymlinks(root)
		if err != nil || real != root {
			return fmt.Errorf("worker checkout redirected")
		}
		branch, err := mqResumeGit(root, "branch", "--show-current")
		if err != nil || branch != r.Branch {
			return fmt.Errorf("worker branch changed")
		}
		top, err := mqResumeGit(root, "rev-parse", "--show-toplevel")
		if err != nil || top != root {
			return fmt.Errorf("worker root changed")
		}
		origin, err := mqResumeGit(root, "remote", "get-url", "origin")
		spec, _ := recoveryRig(r.Rig)
		if err != nil || strings.TrimSuffix(origin, ".git") != spec.Origin {
			return fmt.Errorf("foreign origin")
		}
		head, err := mqResumeGit(root, "rev-parse", "HEAD")
		if err != nil {
			return err
		}
		// Untracked receipts are ignored by Git; tracked changes cannot be proven.
		dirty, err := mqResumeGit(root, "status", "--porcelain", "--untracked-files=no")
		if err != nil || dirty != "" {
			return fmt.Errorf("commit changes before resubmission")
		}
		remote, err := mqResumeGit(root, "ls-remote", "--refs", "origin", "refs/heads/"+branch)
		if err != nil || remote != head+"\trefs/heads/"+branch {
			return fmt.Errorf("push the verified branch before resubmission")
		}
		run := func(args ...string) (string, error) {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			c := exec.CommandContext(ctx, "python3", args...)
			c.Dir = root
			out, err := c.CombinedOutput()
			if err != nil {
				return "", fmt.Errorf("receipt check: %s: %w", out, err)
			}
			return strings.TrimSpace(string(out)), nil
		}
		scope, err := run("scripts/delivery/work_scope.py", "classify", "--base", "origin/main", "--scope-only")
		if err != nil {
			return err
		}
		switch scope {
		case "infrastructure":
			_, err = run("scripts/delivery/work_scope.py", "verify", "--base", "origin/main", "--bead", r.Source)
		case "product":
			_, err = run("scripts/delivery/preflight.py", "verify-receipt", "--receipt", ".delivery/preflight-receipts", "--base", "origin/main", "--head", "HEAD", "--source-bead-id", r.Source)
		default:
			return fmt.Errorf("split mixed product/infrastructure patch before resubmission")
		}
		if err != nil {
			return err
		}
		fresh, err := validateMQResumeRecords(r.Rig, r.MR, b.Show)
		if err != nil || fresh.Record != r || fresh.Source.Assignee != actor {
			return fmt.Errorf("recovery custody changed")
		}
		actual, err := mqResumeGit(root, "rev-parse", "HEAD")
		if err != nil || actual != head {
			return fmt.Errorf("head changed after proof check")
		}
		description, err := kasbResubmittedDescription(fresh.MR, head)
		if err != nil {
			return err
		}
		err = BdCmd("update", r.MR, "--if-status=blocked", "--status=open", "--remove-label=needs-rebase", "--unset-metadata=delivery_recovery", "--body-file=-").Stdin(strings.NewReader(description)).Dir(root).WithBeadsDir(beads.ResolveBeadsDir(filepath.Join(town, r.Rig))).WithAutoCommit().Run()
		if err != nil {
			return err
		}
		confirmed, err := b.Show(r.MR)
		if err != nil {
			return err
		}
		f := beads.ParseMRFields(confirmed)
		if confirmed.Status != "open" || f == nil || f.CommitSHA != head || f.SourceIssue != r.Source || f.Branch != r.Branch {
			return fmt.Errorf("resubmission unconfirmed; preserve and inspect")
		}
		fmt.Fprintf(cmd.OutOrStdout(), "Reopened original MR %s at %s; run gt done to finish the existing source.\n", r.MR, head)
		return nil
	}})
}
