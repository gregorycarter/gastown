package polecat

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/steveyegge/gastown/internal/beads"
	"github.com/steveyegge/gastown/internal/git"
)

// Retirement removes only a dormant, completed sandbox. Product/MR/molecule
// beads and local/remote branches are retained; this is not wisp GC or a nuke.
type Retirement struct {
	Rig        string `json:"rig"`
	Worker     string `json:"worker"`
	Status     string `json:"status"`
	Reason     string `json:"reason,omitempty"`
	Source     string `json:"source,omitempty"`
	Branch     string `json:"branch,omitempty"`
	Head       string `json:"head,omitempty"`
	Target     string `json:"target,omitempty"`
	TargetHead string `json:"target_head,omitempty"`
	Archive    string `json:"archive,omitempty"`
	Empty      bool   `json:"empty_directory"`
}

var retirementName = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]*$`)

func reusablePoolState(state State) bool { return state == StateIdle || state == StateDone }

// inspectRetirement is read-only and is repeated while holding the same
// per-polecat lock as Add/Reuse/Remove. The command also holds the sling lock.
func (m *Manager) inspectRetirement(name string) (Retirement, *beads.Issue, error) {
	r := Retirement{Rig: m.rig.Name, Worker: name, Status: "retained"}
	refuse := func(reason string) (Retirement, *beads.Issue, error) { r.Reason = reason; return r, nil, nil }
	if !retirementName.MatchString(name) {
		return refuse("invalid worker name")
	}
	dir := m.polecatDir(name)
	info, err := os.Lstat(dir)
	if err != nil {
		return r, nil, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return refuse("worker path is not a real directory")
	}
	if _, err := os.Stat(m.pendingPath(name)); err == nil {
		return refuse("allocation pending")
	} else if !os.IsNotExist(err) {
		return r, nil, err
	}
	if blocker := m.brokenIdleReclaimSessionBlocker(name); blocker != "" {
		return refuse(blocker)
	}
	agent, fields, err := m.agentBeads().GetAgentBead(m.agentBeadID(name))
	if err != nil {
		return r, nil, err
	}
	if agent == nil || fields == nil {
		return refuse("agent identity unavailable")
	}
	if fields.AgentState != "done" && fields.AgentState != "idle" && fields.AgentState != "nuked" {
		return refuse("agent_state=" + fields.AgentState)
	}
	if fields.HookBead != "" {
		return refuse("hook remains set: " + fields.HookBead)
	}
	if fields.PushFailed || fields.MRFailed {
		return refuse("unfinished push or MR recovery")
	}
	assigned, err := m.beads.ListByAssignee(m.assigneeID(name))
	if err != nil {
		return r, nil, err
	}
	for _, issue := range assigned {
		if issue != nil && !beads.IsAgentBead(issue) && !beads.IssueStatus(issue.Status).IsTerminal() {
			return refuse("assigned work remains: " + issue.ID)
		}
	}
	if fields.ActiveMR != "" {
		mr, err := m.beads.Show(fields.ActiveMR)
		if err != nil && !errors.Is(err, beads.ErrNotFound) {
			return r, nil, err
		}
		if mr != nil && !beads.IssueStatus(mr.Status).IsTerminal() {
			return refuse("pending MR: " + mr.ID)
		}
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return r, nil, err
	}
	r.Empty = len(entries) == 0
	r.Source = fields.LastSourceIssue
	r.Branch = fields.Branch
	if !r.Empty {
		clone := m.clonePath(name)
		// Refuse symlinks and incomplete/foreign checkouts; never recurse as fallback.
		cloneInfo, err := os.Lstat(clone)
		if err != nil {
			return r, nil, err
		}
		if !cloneInfo.IsDir() || cloneInfo.Mode()&os.ModeSymlink != 0 {
			return refuse("checkout is not a real directory")
		}
		if err := VerifyWorktreeExists(clone); err != nil {
			return refuse("worktree unavailable: " + err.Error())
		}
		marker, err := os.Lstat(filepath.Join(clone, ".git"))
		if err != nil {
			return r, nil, err
		}
		if !marker.Mode().IsRegular() {
			return refuse("only registered worktrees may be retired automatically")
		}
		g := git.NewGit(clone)
		r.Branch, err = g.CurrentBranch()
		if err != nil {
			return r, nil, err
		}
		meta, ok := ParseBranchName(r.Branch)
		if !ok || meta.Polecat != name {
			return refuse("branch does not belong to worker")
		}
		if r.Source == "" {
			r.Source = meta.Issue
		}
		if r.Source == "" || r.Source != meta.Issue {
			return refuse("source identity mismatch")
		}
		state, err := g.CheckUncommittedWork()
		if err != nil {
			return r, nil, err
		}
		if !state.CleanExcludingRuntime() || state.StashCount > 0 {
			return refuse("uncommitted files, conflicts or stash")
		}
		mr, err := m.beads.FindMRForBranch(r.Branch)
		if err != nil {
			return r, nil, err
		}
		if mr != nil && !beads.IssueStatus(mr.Status).IsTerminal() {
			return refuse("pending branch MR: " + mr.ID)
		}
		r.Head, err = g.Rev("HEAD")
		if err != nil {
			return r, nil, err
		}
		r.Target = g.CleanBaseRef("origin", m.rig.DefaultBranch(), "")
		r.TargetHead, err = g.Rev(r.Target)
		if err != nil {
			return r, nil, err
		}
		// Check ONLY the actual default target, not the pushed feature branch or
		// an arbitrary upstream. Rebases may change SHAs, but not patch identity.
		merged, err := g.IsAncestor(r.Head, r.TargetHead)
		if err != nil {
			return r, nil, err
		}
		if !merged {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			command := exec.CommandContext(ctx, "git", "rev-list", "--merges", r.TargetHead+".."+r.Head)
			command.Dir = clone
			merges, mergeErr := command.Output()
			cancel()
			if mergeErr != nil {
				return r, nil, mergeErr
			}
			if strings.TrimSpace(string(merges)) != "" {
				return refuse("unlanded merge commits require review")
			}
			cherry, err := g.Cherry(r.TargetHead, r.Head)
			if err != nil {
				return r, nil, err
			}
			if git.CountCherryUnmergedCommits(cherry) > 0 {
				return refuse("commits not represented on " + r.Target)
			}
		}
	}
	if r.Source != "" {
		source, err := m.beads.Show(r.Source)
		if err != nil {
			return r, nil, err
		}
		if source == nil || !beads.IssueStatus(source.Status).IsTerminal() {
			return refuse("source is not terminal: " + r.Source)
		}
	} else if !r.Empty {
		return refuse("source identity unavailable")
	}
	r.Status = "eligible"
	return r, agent, nil
}

// RetireMerged performs no deletion on uncertain state. Dry runs use exactly
// the same predicates. Successful records and test evidence live outside the
// disposable directory so history remains available after the name is reused.
func (m *Manager) RetireMerged(name string, dryRun bool) (Retirement, error) {
	if !retirementName.MatchString(name) {
		return Retirement{}, fmt.Errorf("invalid worker name")
	}
	fl, err := m.lockPolecat(name)
	if err != nil {
		return Retirement{}, err
	}
	defer fl.Unlock()
	r, agent, err := m.inspectRetirement(name)
	if err != nil || r.Status != "eligible" || dryRun {
		return r, err
	}
	cwd, err := os.Getwd()
	if err != nil {
		return r, err
	}
	rel, err := filepath.Rel(m.polecatDir(name), cwd)
	if err != nil {
		return r, err
	}
	if rel == "." || (!strings.HasPrefix(rel, ".."+string(os.PathSeparator)) && rel != "..") {
		return r, fmt.Errorf("current directory is inside the sandbox")
	}
	fresh, latest, err := m.inspectRetirement(name)
	if err != nil {
		return r, err
	}
	if fresh != r || latest == nil || agent.Description != latest.Description {
		return r, fmt.Errorf("retirement facts changed; preserving sandbox")
	}
	archive := filepath.Join(m.rig.Path, ".runtime", "polecat-retirements", name, time.Now().UTC().Format("20060102T150405.000000000Z"))
	if err := os.MkdirAll(archive, 0700); err != nil {
		return r, err
	}
	r.Archive = archive
	if err := writeRetirementEvidence(archive, r, agent); err != nil {
		return r, err
	}
	if r.Empty {
		if err := os.Remove(m.polecatDir(name)); err != nil {
			return r, err
		}
	} else {
		repo, err := m.repoBase()
		if err != nil {
			return r, err
		}
		if err := retireSandboxFiles(repo, m.clonePath(name), m.polecatDir(name), archive); err != nil {
			return r, err
		}
	}
	r.Status = "retired"
	if err := m.resetAgentBeadForReuse(m.agentBeadID(name), "merged sandbox retired"); err != nil {
		r.Reason = "sandbox removed; agent reset failed: " + err.Error()
	}
	// InUse is derived from directories on each allocation; no stale pool file
	// write is needed (and cannot overwrite another allocator's overflow counter).
	m.namePool.Release(name)
	if err := writeRetirementEvidence(archive, r, agent); err != nil {
		return r, err
	}
	return r, nil
}

func writeRetirementEvidence(archive string, r Retirement, agent *beads.Issue) error {
	data, err := json.MarshalIndent(struct {
		Retirement Retirement   `json:"retirement"`
		Agent      *beads.Issue `json:"agent"`
	}{r, agent}, "", "  ")
	if err != nil {
		return err
	}
	temp := filepath.Join(archive, "retirement.json.tmp")
	if err := os.WriteFile(temp, append(data, '\n'), 0600); err != nil {
		return err
	}
	return os.Rename(temp, filepath.Join(archive, "retirement.json"))
}

func retireSandboxFiles(repo *git.Git, clone, parent, archive string) error {
	evidence := filepath.Join(clone, ".delivery")
	saved := filepath.Join(archive, "delivery")
	moved := false
	if info, err := os.Lstat(evidence); err == nil {
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("delivery evidence is not a real directory")
		}
		if err := os.Rename(evidence, saved); err != nil {
			return err
		}
		moved = true
	} else if !os.IsNotExist(err) {
		return err
	}
	// --force allows disposable ignored files (.codegraph, caches, runtime)
	// after the live git/assignment/MR checks. No RemoveAll fallback on failure.
	if err := repo.WorktreeRemove(clone, true); err != nil {
		if moved {
			if restore := os.Rename(saved, evidence); restore != nil {
				return fmt.Errorf("remove: %v; evidence retained at %s: %v", err, saved, restore)
			}
		}
		return err
	}
	if parent != clone {
		if err := os.Remove(parent); err != nil {
			return fmt.Errorf("worktree removed, parent retained: %w", err)
		}
	}
	return nil
}
