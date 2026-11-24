package asyncpreflight

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// ActiveForWorktree reports an async preflight that may still be using a
// worktree at or below root. Unlike RunningForWorktree it fails closed for
// sandbox reclamation (kasb-84hoc):
//   - a run counts while Live, and also while its recorded runner process
//     (PID plus kernel start time) still exists even with a stale heartbeat:
//     the runner kills the preflight group only when it exits;
//   - a marker that cannot be read is an error while it is young enough to
//     belong to a run in progress (DefaultTimeout + StartGrace), because it
//     might name this worktree.
func ActiveForWorktree(rigPath, root string, now time.Time) (string, error) {
	dir := Dir(rigPath)
	files, err := filepath.Glob(filepath.Join(dir, "*.json"))
	if err != nil {
		return "", err
	}
	for _, p := range files {
		if strings.HasSuffix(p, ".receipt.json") {
			continue
		}
		m, err := Load(p)
		if err != nil {
			if os.IsNotExist(err) {
				continue // retired between Glob and Load
			}
			info, statErr := os.Stat(p)
			if statErr == nil && now.Sub(info.ModTime()) > DefaultTimeout+StartGrace {
				continue // an old corrupt marker cannot be a run in progress
			}
			return "", fmt.Errorf("async preflight marker %s unreadable: %v", p, err)
		}
		if !pathWithin(root, m.Worktree) {
			continue
		}
		if Live(m, now) {
			return fmt.Sprintf("async_preflight=running source=%s head=%s pid=%d marker=%s", m.Source, short(m.Head), m.PID, p), nil
		}
		if (m.Status == StatusRunning || m.Status == StatusStarting) && runnerPresent(m) {
			return fmt.Sprintf("async_preflight=runner_alive source=%s head=%s pid=%d heartbeat=%s marker=%s", m.Source, short(m.Head), m.PID, firstNonEmpty(m.HeartbeatAt, "<none>"), p), nil
		}
	}
	return "", nil
}

// pathWithin reports whether path is root or below it, comparing both the
// cleaned and the symlink-resolved forms.
func pathWithin(root, path string) bool {
	if strings.TrimSpace(root) == "" || strings.TrimSpace(path) == "" {
		return false
	}
	for _, r := range resolvedForms(root) {
		for _, p := range resolvedForms(path) {
			rel, err := filepath.Rel(r, p)
			if err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) && !filepath.IsAbs(rel) {
				return true
			}
		}
	}
	return false
}

func resolvedForms(path string) []string {
	clean := filepath.Clean(path)
	forms := []string{clean}
	if resolved, err := filepath.EvalSymlinks(clean); err == nil && resolved != clean {
		forms = append(forms, resolved)
	}
	return forms
}
