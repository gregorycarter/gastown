package polecat

import (
	"fmt"
	"os/exec"
	"strings"

	"github.com/steveyegge/gastown/internal/checkpoint"
)

// No checkout/reset/stash is permissible in the recovery startup path.
func validatePreservedSession(workDir string, opts SessionStartOptions) error {
	if opts.PreserveBranch == "" || opts.PreserveHead == "" || opts.Issue == "" || opts.BeforeLaunch == nil {
		return fmt.Errorf("incomplete preserved-session admission")
	}
	for _, check := range []struct {
		args []string
		want string
	}{
		{[]string{"branch", "--show-current"}, opts.PreserveBranch},
		{[]string{"rev-parse", "HEAD"}, opts.PreserveHead},
		{[]string{"status", "--porcelain"}, ""},
	} {
		cmd := exec.Command("git", check.args...)
		cmd.Dir = workDir
		out, err := cmd.Output()
		if err == nil && check.args[0] == "status" {
			_, err = checkpoint.ValidateRecoveryStatus(workDir, strings.TrimSpace(string(out)), opts.PreserveBranch, opts.PreserveHead, opts.Issue)
			if err == nil {
				continue
			}
		}
		if err != nil || strings.TrimSpace(string(out)) != check.want {
			return fmt.Errorf("preserved-session Git check refused (%s): %v", strings.Join(check.args, " "), err)
		}
	}
	return nil
}

func preparePreservedSession(workDir string, opts SessionStartOptions) error {
	if err := validatePreservedSession(workDir, opts); err != nil {
		return err
	}
	if err := opts.BeforeLaunch(); err != nil {
		return fmt.Errorf("preserved-session hook/admission refused before launch: %w", err)
	}
	if err := checkpoint.ArchiveRecovery(workDir, opts.PreserveBranch, opts.PreserveHead, opts.Issue); err != nil {
		return err
	}
	// Hooking must not change the worker's branch or data.
	return validatePreservedSession(workDir, opts)
}

// Keep admission, preserved Git checks and the final source guard on the same
// startup path. Ordinary session starts have no callback and still face the guard.
func prepareSessionLaunch(workDir string, opts SessionStartOptions, guard func() error) error {
	if opts.PreserveBranch != "" {
		if err := preparePreservedSession(workDir, opts); err != nil {
			return err
		}
	}
	return guard()
}
