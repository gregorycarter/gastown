package tmux

import (
	"fmt"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

// AgentPID returns the PID of the agent runtime process (claude, codex, …)
// running in a session's agent pane.
//
// Resolution mirrors matchesPaneRuntime: the pane's foreground process is
// checked first (the agent may be exec'd directly into the pane), then the
// process tree below it is walked for the first process whose executable name
// matches one of the session's configured process names.
//
// Returns an error when the session has no resolvable process names, when the
// pane PID cannot be read, or when no matching process is found.
func (t *Tmux) AgentPID(session string) (string, error) {
	names, err := t.resolveSessionProcessNamesChecked(session)
	if err != nil {
		return "", fmt.Errorf("resolving process names for %s: %w", session, err)
	}
	if len(names) == 0 {
		return "", fmt.Errorf("no process names configured for session %s", session)
	}

	target := session
	if pane, err := t.FindAgentPane(session); err == nil && pane != "" {
		target = pane
	}
	panePID, err := t.GetPanePID(target)
	if err != nil {
		return "", fmt.Errorf("reading pane pid for %s: %w", session, err)
	}
	root, ok := normalizeProcessID(panePID)
	if !ok {
		return "", fmt.Errorf("invalid pane pid %q for session %s", panePID, session)
	}

	// The pane process itself may be the agent (exec'd, no intermediate shell).
	if matches, _ := processMatchesNamesChecked(root, names); matches {
		return root, nil
	}

	out, err := exec.Command("ps", "-axo", "pid=,ppid=,comm=").Output()
	if err != nil {
		return "", fmt.Errorf("listing processes: %w", err)
	}
	if pid := findDescendantPIDWithNames(root, names, parseProcessSnapshot(out)); pid != "" {
		return pid, nil
	}
	return "", fmt.Errorf("no agent process (%s) found under pid %s for session %s",
		strings.Join(names, ","), root, session)
}

// findDescendantPIDWithNames walks a process snapshot breadth-first from root
// and returns the PID of the first descendant whose command name matches.
func findDescendantPIDWithNames(root string, names []string, snapshot processSnapshot) string {
	nameSet := make(map[string]bool, len(names))
	for _, n := range names {
		if n = strings.TrimSpace(n); n != "" {
			nameSet[n] = true
		}
	}
	if len(nameSet) == 0 {
		return ""
	}

	const maxDepth = 10
	seen := map[string]bool{root: true}
	type node struct {
		pid   string
		depth int
	}
	queue := []node{{pid: root, depth: 0}}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		if cur.depth >= maxDepth {
			continue
		}
		for _, child := range snapshot.children[cur.pid] {
			if seen[child] {
				continue
			}
			seen[child] = true
			if nameSet[filepath.Base(strings.TrimSpace(snapshot.names[child]))] {
				return child
			}
			queue = append(queue, node{pid: child, depth: cur.depth + 1})
		}
	}
	return ""
}

// ProcessRSSMB returns the resident set size of a process in megabytes.
// Uses `ps -o rss=` which reports kilobytes on Linux and macOS.
func ProcessRSSMB(pid string) (int, error) {
	clean, ok := normalizeProcessID(pid)
	if !ok {
		return 0, fmt.Errorf("invalid process ID %q", pid)
	}
	out, err := exec.Command("ps", "-o", "rss=", "-p", clean).Output()
	if err != nil {
		return 0, fmt.Errorf("reading rss for pid %s: %w", clean, err)
	}
	field := strings.TrimSpace(string(out))
	if field == "" {
		return 0, fmt.Errorf("no rss reported for pid %s", clean)
	}
	kb, err := strconv.ParseInt(strings.Fields(field)[0], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parsing rss %q: %w", field, err)
	}
	return int(kb / 1024), nil
}

// AgentRSSMB returns the RSS (in MB) of the agent process in a session.
func (t *Tmux) AgentRSSMB(session string) (int, error) {
	pid, err := t.AgentPID(session)
	if err != nil {
		return 0, err
	}
	return ProcessRSSMB(pid)
}
