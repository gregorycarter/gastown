package tmux

import (
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// agentStackShells are the shells that may sit between a pane and its agent
// (a pane started with a shell that then runs the agent binary).
var agentStackShells = map[string]bool{
	"sh": true, "bash": true, "zsh": true, "fish": true, "dash": true,
	"ksh": true, "tcsh": true, "csh": true, "login": true,
}

// AgentStackPIDs returns the PIDs of a session's own agent stack: the pane
// process, any shells between it and the agent binary (GT_PROCESS_NAMES),
// the agent process itself, and the agent's own helper processes named
// "<agent>-..." (for example codex-code-mode-host). Work the agent starts
// (test runs, builds, preflights) is not part of the stack, so a caller can
// tell an idle agent sitting in its worktree from one with work running
// there (kasb-2ifwk).
//
// When the agent binary cannot be found below the pane, only the pane and
// the shells walked are returned; callers that treat the remaining sandbox
// processes as activity then fail closed.
func (t *Tmux) AgentStackPIDs(session string) ([]int, error) {
	names, err := t.resolveSessionProcessNamesChecked(session)
	if err != nil {
		return nil, err
	}
	panePID, err := t.GetPanePID(session)
	if err != nil {
		return nil, err
	}
	root, ok := normalizeProcessID(panePID)
	if !ok {
		return nil, &strconv.NumError{Func: "AgentStackPIDs", Num: panePID, Err: strconv.ErrSyntax}
	}
	out, err := exec.Command("ps", "-axo", "pid=,ppid=,comm=").Output()
	if err != nil {
		return nil, err
	}
	return agentStackFromSnapshot(root, names, parseProcessSnapshot(out)), nil
}

func agentStackFromSnapshot(root string, names []string, snapshot processSnapshot) []int {
	nameSet := map[string]bool{}
	for _, n := range names {
		if n = strings.TrimSpace(n); n != "" {
			nameSet[n] = true
		}
	}
	base := func(pid string) string {
		return strings.TrimPrefix(filepath.Base(strings.TrimSpace(snapshot.names[pid])), "-")
	}
	var stack []string
	agent := ""
	// Walk down from the pane through shells only, until the agent binary.
	frontier := []string{root}
	seen := map[string]bool{}
	for depth := 0; depth <= 10 && len(frontier) > 0 && agent == ""; depth++ {
		var next []string
		for _, pid := range frontier {
			if seen[pid] {
				continue
			}
			seen[pid] = true
			name := base(pid)
			switch {
			case nameSet[name]:
				stack = append(stack, pid)
				if agent == "" {
					agent = pid
				}
			case pid == root || agentStackShells[name]:
				stack = append(stack, pid)
				next = append(next, snapshot.children[pid]...)
			}
		}
		frontier = next
	}
	if agent != "" {
		for _, child := range snapshot.children[agent] {
			name := base(child)
			for n := range nameSet {
				if strings.HasPrefix(name, n+"-") {
					stack = append(stack, child)
					break
				}
			}
		}
	}
	pids := make([]int, 0, len(stack))
	for _, pid := range stack {
		if n, err := strconv.Atoi(pid); err == nil {
			pids = append(pids, n)
		}
	}
	sort.Ints(pids)
	return pids
}
