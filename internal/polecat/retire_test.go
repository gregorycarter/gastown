package polecat

import (
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/steveyegge/gastown/internal/beads"
	"github.com/steveyegge/gastown/internal/git"
	"github.com/steveyegge/gastown/internal/rig"
	"github.com/steveyegge/gastown/internal/tmux"
)

type retirementFixture struct {
	manager                *Manager
	repo, clone, modelFile string
	model                  map[string]any
	fields                 beads.AgentFields
}

func (f *retirementFixture) save(t *testing.T) {
	t.Helper()
	f.model["description"] = beads.FormatAgentDescription("worker", &f.fields)
	data, _ := json.Marshal(f.model)
	if err := os.WriteFile(f.modelFile, data, 0600); err != nil {
		t.Fatal(err)
	}
}
func retirementGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	c := exec.Command("git", args...)
	c.Dir = dir
	out, err := c.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v %s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}
func newRetirementFixture(t *testing.T) *retirementFixture {
	t.Helper()
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 fixture helper unavailable")
	}
	town := t.TempDir()
	rigPath := filepath.Join(town, "testrig")
	repo := filepath.Join(rigPath, "mayor", "rig")
	clone := filepath.Join(rigPath, "polecats", "toast", "testrig")
	for _, p := range []string{repo, filepath.Join(town, ".beads"), filepath.Join(rigPath, ".beads"), filepath.Join(repo, ".beads"), filepath.Join(town, "bin")} {
		if err := os.MkdirAll(p, 0755); err != nil {
			t.Fatal(err)
		}
	}
	retirementGit(t, repo, "init", "-b", "main")
	retirementGit(t, repo, "config", "user.name", "Test")
	retirementGit(t, repo, "config", "user.email", "test@example.invalid")
	if err := os.WriteFile(filepath.Join(repo, ".gitignore"), []byte(".beads/\n.delivery/\n.codegraph/\n"), 0644); err != nil {
		t.Fatal(err)
	}
	retirementGit(t, repo, "add", ".gitignore")
	retirementGit(t, repo, "commit", "-m", "base")
	retirementGit(t, repo, "remote", "add", "origin", repo)
	retirementGit(t, repo, "update-ref", "refs/remotes/origin/main", "HEAD")
	retirementGit(t, repo, "worktree", "add", "-b", "polecat/toast/gt-task+old", clone)
	if err := os.WriteFile(filepath.Join(rigPath, ".beads", "redirect"), []byte("mayor/rig/.beads\n"), 0644); err != nil {
		t.Fatal(err)
	}
	f := &retirementFixture{repo: repo, clone: clone, modelFile: filepath.Join(town, "model.json"), model: map[string]any{"sourceStatus": "closed", "assigned": []any{}, "mrs": []any{}}, fields: beads.AgentFields{RoleType: "polecat", Rig: "testrig", AgentState: "done", CleanupStatus: "clean", LastSourceIssue: "gt-task", Branch: "polecat/toast/gt-task+old"}}
	f.save(t)
	script := `#!/usr/bin/env python3
import os,sys,json
p=os.environ['GT_RETIRE_TEST_MODEL'];m=json.load(open(p));a=sys.argv[1:]
if os.path.basename(sys.argv[0])=='lsof':
 if m.get('openFileError'): sys.exit(2)
 print('p1\nn'+m.get('openFile','/unrelated'));sys.exit(0)
if os.path.basename(sys.argv[0])=='tmux':
 if m.get('sessionError'): print('tmux unavailable',file=sys.stderr);sys.exit(2)
 if m.get('session'): sys.exit(0)
 print("can't find session",file=sys.stderr);sys.exit(1)
if m.get('lookupError'): print('synthetic lookup failure',file=sys.stderr);sys.exit(1)
if 'show' in a:
 ident=a[a.index('show')+1]
 if 'polecat' in ident: print(json.dumps([{'id':ident,'title':'worker','status':'open','issue_type':'task','labels':['gt:agent'],'description':m['description']}]))
 elif ident=='gt-task': print(json.dumps([{'id':ident,'status':m['sourceStatus'],'issue_type':'task'}]))
 elif ident=='gt-mr': print(json.dumps([{'id':ident,'status':'blocked','labels':['gt:merge-request']}]))
 else: print('unknown issue',file=sys.stderr);sys.exit(1)
elif 'list' in a: print(json.dumps(m['mrs'] if any('gt:merge-request' in x for x in a) else m['assigned']))
elif 'update' in a:
 if '--description' in a: m['description']=a[a.index('--description')+1]
 json.dump(m,open(p,'w'));print('{}')
else: print('[]')
`
	for _, name := range []string{"bd", "tmux", "lsof"} {
		if err := os.WriteFile(filepath.Join(town, "bin", name), []byte(script), 0755); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("GT_RETIRE_TEST_MODEL", f.modelFile)
	t.Setenv("PATH", filepath.Join(town, "bin")+string(os.PathListSeparator)+os.Getenv("PATH"))
	f.manager = NewManager(&rig.Rig{Name: "testrig", Path: rigPath}, git.NewGit(repo), tmux.NewTmux())
	return f
}

func TestRetireMergedRemovesSandboxAndPreservesEvidence(t *testing.T) {
	f := newRetirementFixture(t)
	for _, dir := range []string{".delivery", ".codegraph"} {
		if err := os.MkdirAll(filepath.Join(f.clone, dir), 0700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(f.clone, ".delivery", "receipt.json"), []byte(`{"status":"passed"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(f.clone, ".codegraph", "stale.db"), []byte("disposable"), 0600); err != nil {
		t.Fatal(err)
	}
	r, err := f.manager.RetireMerged("toast", true)
	if err != nil || r.Status != "eligible" {
		t.Fatalf("dry run: %+v %v", r, err)
	}
	if _, err := os.Stat(f.clone); err != nil {
		t.Fatal("dry run deleted checkout")
	}
	r, err = f.manager.RetireMerged("toast", false)
	if err != nil || r.Status != "retired" {
		t.Fatalf("retirement: %+v %v", r, err)
	}
	if _, err := os.Stat(filepath.Dir(f.clone)); !os.IsNotExist(err) {
		t.Fatal("directory slot was not released")
	}
	if data, err := os.ReadFile(filepath.Join(r.Archive, "delivery", "receipt.json")); err != nil || string(data) != `{"status":"passed"}` {
		t.Fatal("test receipt was not preserved")
	}
	if _, err := os.Stat(filepath.Join(r.Archive, "retirement.json")); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(retirementGit(t, f.repo, "worktree", "list", "--porcelain"), f.clone) {
		t.Fatal("worktree registry leaked")
	}
	retirementGit(t, f.repo, "rev-parse", "refs/heads/polecat/toast/gt-task+old")
}

func TestRetirementRefusesPreservedOrUncertainWork(t *testing.T) {
	cases := []struct {
		name   string
		change func(*testing.T, *retirementFixture)
	}{
		{"live session", func(t *testing.T, f *retirementFixture) { f.model["session"] = true }},
		{"unknown session", func(t *testing.T, f *retirementFixture) { f.model["sessionError"] = true }},
		{"unknown beads", func(t *testing.T, f *retirementFixture) { f.model["lookupError"] = true }},
		{"active source", func(t *testing.T, f *retirementFixture) { f.model["sourceStatus"] = "in_progress" }},
		{"hook", func(t *testing.T, f *retirementFixture) { f.fields.HookBead = "gt-task" }},
		{"pending MR", func(t *testing.T, f *retirementFixture) { f.fields.ActiveMR = "gt-mr" }},
		{"blocked assignment", func(t *testing.T, f *retirementFixture) {
			f.model["assigned"] = []any{map[string]any{"id": "gt-other", "status": "blocked", "issue_type": "task"}}
		}},
		{"new allocation", func(t *testing.T, f *retirementFixture) {
			os.WriteFile(f.manager.pendingPath("toast"), []byte("pending"), 0600)
		}},
		{"dirty source", func(t *testing.T, f *retirementFixture) {
			os.WriteFile(filepath.Join(f.clone, "product.txt"), []byte("uncommitted"), 0600)
		}},
		{"stash", func(t *testing.T, f *retirementFixture) {
			os.WriteFile(filepath.Join(f.clone, ".gitignore"), []byte("changed"), 0600)
			retirementGit(t, f.clone, "stash", "push")
		}},
		{"pushed but unmerged", func(t *testing.T, f *retirementFixture) {
			os.WriteFile(filepath.Join(f.clone, "product.txt"), []byte("new"), 0600)
			retirementGit(t, f.clone, "add", "product.txt")
			retirementGit(t, f.clone, "commit", "-m", "unmerged")
			retirementGit(t, f.clone, "update-ref", "refs/remotes/origin/polecat/toast/gt-task+old", "HEAD")
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newRetirementFixture(t)
			tc.change(t, f)
			f.save(t)
			r, err := f.manager.RetireMerged("toast", false)
			if err == nil && r.Status != "retained" {
				t.Fatalf("unsafe retirement: %+v", r)
			}
			if _, err := os.Stat(f.clone); err != nil {
				t.Fatal("preserved work removed")
			}
		})
	}
}

func TestRetirementAcceptsRebasedPatchAndEmptyDirectory(t *testing.T) {
	t.Run("rebased patch", func(t *testing.T) {
		f := newRetirementFixture(t)
		os.WriteFile(filepath.Join(f.clone, "product.txt"), []byte("done"), 0600)
		retirementGit(t, f.clone, "add", "product.txt")
		retirementGit(t, f.clone, "commit", "-m", "worker patch")
		head := retirementGit(t, f.clone, "rev-parse", "HEAD")
		retirementGit(t, f.repo, "commit", "--allow-empty", "-m", "main moved")
		retirementGit(t, f.repo, "cherry-pick", head)
		retirementGit(t, f.repo, "update-ref", "refs/remotes/origin/main", "HEAD")
		r, err := f.manager.RetireMerged("toast", false)
		if err != nil || r.Status != "retired" {
			t.Fatalf("rebased: %+v %v", r, err)
		}
	})
	t.Run("empty parent", func(t *testing.T) {
		f := newRetirementFixture(t)
		retirementGit(t, f.repo, "worktree", "remove", f.clone)
		r, err := f.manager.RetireMerged("toast", false)
		if err != nil || r.Status != "retired" || !r.Empty {
			t.Fatalf("empty: %+v %v", r, err)
		}
	})
}

func TestFindIdlePolecatIncludesCompletedSafeWorkers(t *testing.T) {
	f := newRetirementFixture(t)
	p, err := f.manager.FindIdlePolecat()
	if err != nil || p == nil || p.Name != "toast" {
		t.Fatalf("done worker not reusable: %+v %v", p, err)
	}
	f.model["session"] = true
	f.save(t)
	p, err = f.manager.FindIdlePolecat()
	if err != nil || p != nil {
		t.Fatalf("live done worker was reusable: %+v %v", p, err)
	}
}

func TestRetirementAndSessionStartShareLifecycleLock(t *testing.T) {
	f := newRetirementFixture(t)
	held, err := f.manager.lockPolecat("absent")
	if err != nil {
		t.Fatal(err)
	}
	manager := NewSessionManager(tmux.NewTmux(), f.manager.rig)
	started := make(chan error, 1)
	go func() { started <- manager.Start("absent", SessionStartOptions{}) }()
	select {
	case err := <-started:
		t.Fatalf("startup bypassed lifecycle lock: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	held.Unlock()
	select {
	case err := <-started:
		if !errors.Is(err, ErrPolecatNotFound) {
			t.Fatalf("unexpected startup result: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("startup did not release lock")
	}
}

func TestRetirementRechecksAfterWaitingForLifecycleLock(t *testing.T) {
	f := newRetirementFixture(t)
	held, err := f.manager.lockPolecat("toast")
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan Retirement, 1)
	go func() { r, _ := f.manager.RetireMerged("toast", false); result <- r }()
	select {
	case <-result:
		t.Fatal("retirement bypassed lifecycle lock")
	case <-time.After(50 * time.Millisecond):
	}
	if err := os.WriteFile(filepath.Join(f.clone, "new-work.txt"), []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	held.Unlock()
	select {
	case r := <-result:
		if r.Status != "retained" {
			t.Fatalf("new work lost: %+v", r)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("retirement did not return")
	}
	if _, err := os.Stat(filepath.Join(f.clone, "new-work.txt")); err != nil {
		t.Fatal("new work was removed")
	}
}

func TestRetirementReclaimsSiblingCache(t *testing.T) {
	f := newRetirementFixture(t)
	parent := filepath.Dir(f.clone)
	cache := filepath.Join(parent, ".cache", "polecat-deps", "venvs", "locked")
	if err := os.MkdirAll(cache, 0700); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), "preserve")
	if err := os.WriteFile(outside, []byte("external"), 0400); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(cache, "python")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cache, "payload"), []byte("regenerable"), 0400); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(cache, 0500); err != nil {
		t.Fatal(err)
	}
	r, err := f.manager.RetireMerged("toast", true)
	if err != nil || r.Status != "eligible" || r.Cache == "" {
		t.Fatalf("dry: %+v %v", r, err)
	}
	if _, err := os.Stat(cache); err != nil {
		t.Fatal("dry run removed cache")
	}
	r, err = f.manager.RetireMerged("toast", false)
	if err != nil || r.Status != "retired" {
		t.Fatalf("apply: %+v %v", r, err)
	}
	if _, err := os.Stat(parent); !os.IsNotExist(err) {
		t.Fatal("cache/worktree parent remains")
	}
	if data, err := os.ReadFile(outside); err != nil || string(data) != "external" {
		t.Fatal("followed external cache symlink")
	}
	if info, _ := os.Stat(outside); info.Mode().Perm() != 0400 {
		t.Fatal("changed external permissions")
	}
	if r.HostFreeBefore == nil || r.HostFreeAfter == nil || r.HostFreeDelta == nil || *r.HostFreeDelta != *r.HostFreeAfter-*r.HostFreeBefore {
		t.Fatalf("missing physical observations: %+v", r)
	}
	retirementGit(t, f.repo, "rev-parse", "refs/heads/polecat/toast/gt-task+old")
}

func TestRetirementPreservesUnknownAndBusyCaches(t *testing.T) {
	for _, name := range []string{"unknown sibling", "unknown cache entry", "redirected root", "redirected deps", "open files", "unknown open files", "new assignment"} {
		t.Run(name, func(t *testing.T) {
			f := newRetirementFixture(t)
			parent := filepath.Dir(f.clone)
			cache := filepath.Join(parent, ".cache")
			deps := filepath.Join(cache, "polecat-deps")
			if err := os.MkdirAll(filepath.Join(deps, "npm-cache"), 0700); err != nil {
				t.Fatal(err)
			}
			switch name {
			case "unknown sibling":
				os.WriteFile(filepath.Join(parent, "keep.txt"), []byte("keep"), 0600)
			case "unknown cache entry":
				os.WriteFile(filepath.Join(deps, "keep.txt"), []byte("keep"), 0600)
			case "redirected root":
				os.Rename(cache, cache+"-real")
				os.Symlink(cache+"-real", cache)
			case "redirected deps":
				os.Rename(deps, deps+"-real")
				os.Symlink(deps+"-real", deps)
			case "open files":
				f.model["openFile"] = filepath.Join(deps, "npm-cache", "busy")
			case "unknown open files":
				f.model["openFileError"] = true
			case "new assignment":
				f.fields.HookBead = "gt-other"
			}
			f.save(t)
			r, err := f.manager.RetireMerged("toast", false)
			if err == nil && r.Status != "retained" {
				t.Fatalf("unsafe: %+v", r)
			}
			if _, err := os.Stat(f.clone); err != nil {
				t.Fatal("checkout removed before cache validation")
			}
			if _, err := os.Lstat(cache); err != nil {
				t.Fatal("cache removed")
			}
		})
	}
}
