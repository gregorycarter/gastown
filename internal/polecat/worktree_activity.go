package polecat

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/steveyegge/gastown/internal/asyncpreflight"
)

// ProcessPath is one open path (cwd, executable or file) of a live process.
type ProcessPath struct {
	PID     int
	Command string
	FD      string // lsof file descriptor: cwd, txt, mem, or a number ("" unknown)
	Access  string // lsof access mode of a numbered descriptor: r, w or u
	Path    string
}

// usesSandbox reports whether an open path is evidence that the process works
// in the sandbox: its cwd, code it executes (txt/mem: a worktree .venv), a
// file it writes, or a descriptor lsof did not classify (fail closed). A file
// another process merely reads (token/metrics scanners such as rig_tokens.py,
// indexers) is not: such readers come and go every minute.
func (p ProcessPath) usesSandbox() bool {
	switch p.FD {
	case "", "cwd", "rtd", "txt", "mem":
		return true
	}
	return strings.ContainsAny(p.Access, "wu")
}

// ListProcessPaths lists the open paths of every visible process. It is lsof
// by default; tests replace it.
var ListProcessPaths = lsofProcessPaths

// lsofTimeout bounds one host-wide lsof scan (about one second normally).
const lsofTimeout = 30 * time.Second

func lsofProcessPaths() ([]ProcessPath, error) {
	ctx, cancel := context.WithTimeout(context.Background(), lsofTimeout)
	defer cancel()
	// -n/-P: no name lookups; -w: no warnings; -F pcfan: machine-readable
	// pid, command, descriptor, access mode and name fields. All processes,
	// all open files, cwd included.
	cmd := exec.CommandContext(ctx, "lsof", "-n", "-P", "-w", "-F", "pcfan")
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	if ctx.Err() != nil {
		return nil, fmt.Errorf("lsof timed out after %s", lsofTimeout)
	}
	if err != nil {
		// lsof exits 1 when some files could not be reported (for example,
		// processes of other users) but still prints the rest. Only output
		// proves the scan ran.
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) || stdout.Len() == 0 {
			return nil, fmt.Errorf("lsof failed: %v %s", err, strings.TrimSpace(stderr.String()))
		}
	}
	return parseLsofFields(stdout.Bytes()), nil
}

// parseLsofFields parses `lsof -F pcfan` output: a p (and c) line per
// process, then f, a and n lines per open file.
func parseLsofFields(out []byte) []ProcessPath {
	var paths []ProcessPath
	pid, command, fd, access := 0, "", "", ""
	scanner := bufio.NewScanner(bytes.NewReader(out))
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		value := line[1:]
		switch line[0] {
		case 'p':
			pid, _ = strconv.Atoi(value)
			command, fd, access = "", "", ""
		case 'c':
			command = value
		case 'f':
			fd, access = value, ""
		case 'a':
			access = value
		case 'n':
			if pid > 0 && strings.HasPrefix(value, "/") {
				paths = append(paths, ProcessPath{PID: pid, Command: command, FD: fd, Access: access, Path: value})
			}
		}
	}
	return paths
}

// LiveProcessesUnder returns the processes (other than this one and its
// parent) whose cwd, executed code or a file open for writing lies at or
// below root.
func LiveProcessesUnder(root string) ([]ProcessPath, error) {
	paths, err := ListProcessPaths()
	if err != nil {
		return nil, err
	}
	roots := pathForms(root)
	self, parent := os.Getpid(), os.Getppid()
	seen := map[int]bool{}
	var hits []ProcessPath
	for _, p := range paths {
		if p.PID == self || p.PID == parent || seen[p.PID] || !p.usesSandbox() {
			continue
		}
		for _, r := range roots {
			if pathAtOrBelow(r, p.Path) {
				seen[p.PID] = true
				hits = append(hits, p)
				break
			}
		}
	}
	sort.Slice(hits, func(i, j int) bool { return hits[i].PID < hits[j].PID })
	return hits, nil
}

// WorktreeActivityBlocker returns a non-empty reason when the sandbox at
// sandboxDir (a polecat directory or its worktree) is in use by an async
// preflight run or any live process: a preflight, its pytest children, an
// editor, a shell. Removing such a sandbox kills the run and corrupts its
// receipt (kasb-84hoc). Detection failures are themselves a reason: an
// unverifiable sandbox is never reclaimable.
func WorktreeActivityBlocker(rigPath, sandboxDir string, now time.Time) string {
	if strings.TrimSpace(sandboxDir) == "" {
		return "worktree_activity=unknown: empty sandbox path"
	}
	if rigPath != "" {
		reason, err := asyncpreflight.ActiveForWorktree(rigPath, sandboxDir, now)
		if err != nil {
			return "worktree_activity=unknown: " + err.Error()
		}
		if reason != "" {
			return reason
		}
	}
	procs, err := LiveProcessesUnder(sandboxDir)
	if err != nil {
		return "worktree_activity=unknown: " + err.Error()
	}
	if len(procs) == 0 {
		return ""
	}
	var parts []string
	for i, p := range procs {
		if i == 3 {
			parts = append(parts, fmt.Sprintf("+%d more", len(procs)-3))
			break
		}
		parts = append(parts, fmt.Sprintf("pid=%d(%s)", p.PID, p.Command))
	}
	return "live_process_in_worktree " + strings.Join(parts, " ")
}

func pathForms(path string) []string {
	clean := filepath.Clean(path)
	forms := []string{clean}
	if resolved, err := filepath.EvalSymlinks(clean); err == nil && resolved != clean {
		forms = append(forms, resolved)
	}
	return forms
}

func pathAtOrBelow(root, path string) bool {
	path = filepath.Clean(path)
	return path == root || strings.HasPrefix(path, root+string(filepath.Separator))
}
