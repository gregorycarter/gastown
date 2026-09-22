package daemon

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/steveyegge/gastown/internal/beads"
)

// One bounded lifecycle sweep each heartbeat, even when dispatch is pressure
// deferred or all execution slots are occupied. The command includes parked rigs
// and rechecks assignment/session/git facts under the normal lifecycle locks.
func (d *Daemon) retireMergedPolecats() {
	ctx, cancel := context.WithTimeout(d.ctx, 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "gt", "polecat", "retire", "--all", "--limit", "2", "--json")
	setSysProcAttr(cmd)
	cmd.Dir = d.config.TownRoot
	cmd.Env = beads.BuildMutationRoutingBDEnv(os.Environ(), filepath.Join(d.config.TownRoot, ".beads"))
	out, err := cmd.CombinedOutput()
	if err != nil {
		d.logger.Printf("Polecat retirement failed: %v (%s)", err, out)
		return
	}
	if len(out) > 0 {
		d.logger.Printf("Polecat retirement: %s", out)
	}
}
