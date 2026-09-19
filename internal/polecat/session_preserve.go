package polecat

import (
	"fmt"
	"os/exec"
	"strings"
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
	// Hooking must not change the worker's branch or data.
	return validatePreservedSession(workDir, opts)
}
