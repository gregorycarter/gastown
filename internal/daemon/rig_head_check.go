package daemon

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Rig-checkout drift warning.
//
// The Mayor's rig checkout (<town>/<rig>/mayor/rig) is where `bd cook` resolves
// formulas through the rig's .beads redirect, and where several operator
// workflows read scripts and gate definitions. When that checkout drifts behind
// origin/main — it was found 18 commits behind on 2026-09-01 — formula and gate
// fixes that landed on main are simply not present, and the failure mode is
// silence: everything runs, using stale content.
//
// The daemon has no business rebasing an operator's checkout, so this only
// warns, and only once an hour per rig.

const (
	// rigHeadWarnInterval is the minimum time between drift warnings per rig.
	rigHeadWarnInterval = time.Hour

	// rigHeadMaxBehind is how far behind origin/main the rig checkout may fall
	// before the daemon says so.
	rigHeadMaxBehind = 5

	// rigHeadGitTimeout bounds each git invocation.
	rigHeadGitTimeout = 10 * time.Second
)

// rigHeadStatus is the observable state of one rig checkout.
type rigHeadStatus struct {
	Branch   string // "HEAD" when detached
	Detached bool
	Behind   int
	Err      error
}

// rigHeadDriftReason returns the warning text for a rig checkout, or "" when
// the checkout is fine (or could not be inspected).
func rigHeadDriftReason(rigName string, st rigHeadStatus, maxBehind int) string {
	if st.Err != nil {
		return ""
	}
	if st.Detached {
		return fmt.Sprintf("%s/mayor/rig is in detached HEAD — formula and gate fixes on main are not present in this checkout", rigName)
	}
	if st.Behind > maxBehind {
		return fmt.Sprintf("%s/mayor/rig (%s) is %d commits behind origin/main — formula and gate fixes that landed on main are not in this checkout",
			rigName, st.Branch, st.Behind)
	}
	return ""
}

// gitOutput runs a git command in dir and returns trimmed stdout.
func gitOutput(dir string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), rigHeadGitTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "git", args...) //nolint:gosec // G204: fixed argv, dir is a town path
	cmd.Dir = dir
	setSysProcAttr(cmd)
	out, err := cmd.Output()
	return strings.TrimSpace(string(out)), err
}

// inspectRigHead reads the branch and behind-count of a rig checkout.
// Nothing is fetched: this reports on the refs already present locally.
func inspectRigHead(rigCheckout string) rigHeadStatus {
	if info, err := os.Stat(rigCheckout); err != nil || !info.IsDir() {
		return rigHeadStatus{Err: fmt.Errorf("no rig checkout at %s", rigCheckout)}
	}

	branch, err := gitOutput(rigCheckout, "rev-parse", "--abbrev-ref", "HEAD")
	if err != nil {
		return rigHeadStatus{Err: err}
	}
	if branch == "HEAD" {
		return rigHeadStatus{Branch: branch, Detached: true}
	}

	count, err := gitOutput(rigCheckout, "rev-list", "--count", "HEAD..origin/main")
	if err != nil {
		// No origin/main ref locally: nothing to compare against.
		return rigHeadStatus{Branch: branch, Err: err}
	}
	behind, err := strconv.Atoi(count)
	if err != nil {
		return rigHeadStatus{Branch: branch, Err: err}
	}
	return rigHeadStatus{Branch: branch, Behind: behind}
}

// checkRigCheckoutDrift warns (at most once per hour per rig) when a rig's
// mayor checkout has drifted behind origin/main or gone detached.
func (d *Daemon) checkRigCheckoutDrift() {
	if d.rigHeadWarned == nil {
		d.rigHeadWarned = make(map[string]time.Time)
	}

	for _, rigName := range d.getKnownRigs() {
		if last, ok := d.rigHeadWarned[rigName]; ok && time.Since(last) < rigHeadWarnInterval {
			continue
		}
		checkout := filepath.Join(d.config.TownRoot, rigName, "mayor", "rig")
		reason := rigHeadDriftReason(rigName, inspectRigHead(checkout), rigHeadMaxBehind)
		if reason == "" {
			continue
		}
		d.logger.Printf("Rig checkout drift: %s", reason)
		d.rigHeadWarned[rigName] = time.Now()
	}
}
