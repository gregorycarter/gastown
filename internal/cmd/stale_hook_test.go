package cmd

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/steveyegge/gastown/internal/beads"
	"github.com/steveyegge/gastown/internal/config"
	"github.com/steveyegge/gastown/internal/polecat"
	"github.com/steveyegge/gastown/internal/session"
)

// hq-ee6vdf: kasb-kasb_core-polecat-deathclaw has no hook column but its
// description still says hook_bead: kasb-qyar.185.2, which is hooked to
// scavenger. The stale line must not become a hook blocker.
func TestAgentHookBead_StaleDescriptionGivesNoHookBlocker(t *testing.T) {
	const me = "kasb_core/polecats/deathclaw"
	cases := []struct {
		name      string
		hooked    *beads.Issue
		wantHook  string
		wantStale string
	}{
		{"hooked to another polecat", &beads.Issue{ID: "kasb-qyar.185.2", Status: "hooked", Assignee: "kasb_core/polecats/scavenger"}, "", "kasb-qyar.185.2"},
		{"parked (unassigned)", &beads.Issue{ID: "kasb-qyar.185.2", Status: "open"}, "", "kasb-qyar.185.2"},
		{"in progress elsewhere", &beads.Issue{ID: "kasb-qyar.185.2", Status: "in_progress", Assignee: "kasb_core/scavenger"}, "", "kasb-qyar.185.2"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fields := &beads.AgentFields{AgentState: "working", HookBead: "kasb-qyar.185.2"}
			agentIssue := &beads.Issue{ID: "kasb-kasb_core-polecat-deathclaw"} // hook column empty
			bd := fakeIssueShower{issue: tc.hooked}

			hook, stale := agentHookBead(bd, agentIssue, fields, me)
			if hook != tc.wantHook || stale != tc.wantStale {
				t.Fatalf("agentHookBead = (%q, %q), want (%q, %q)", hook, stale, tc.wantHook, tc.wantStale)
			}
			safe, _, blocker := hookBeadSafeForCleanup(bd, hook)
			if !safe || blocker != "" {
				t.Fatalf("stale hook produced a cleanup blocker: safe=%v blocker=%q", safe, blocker)
			}
			disposition := polecat.DecideWorkstate(polecat.WorkstateInput{
				State:         polecat.StateIdle,
				CleanupStatus: polecat.CleanupClean,
				HookBead:      hook,
				SessionKnown:  true,
			})
			for _, b := range disposition.Blockers {
				if strings.Contains(b, "has work on hook") || strings.Contains(b, "hook-still-set") {
					t.Fatalf("stale description hook still blocks: %+v", disposition.Blockers)
				}
			}
			if got := withoutStaleHook(fields, stale); got.HookBead != "" || fields.HookBead == "" {
				t.Fatalf("withoutStaleHook must drop the line on a copy: got %q, original %q", got.HookBead, fields.HookBead)
			}
		})
	}
}

// The hook record still wins: the column, live work assigned to this agent,
// a spawning agent, and an unreadable bead all keep the hook.
func TestAgentHookBead_KeepsRealHook(t *testing.T) {
	const me = "kasb_core/polecats/deathclaw"
	mine := &beads.Issue{ID: "kasb-a", Status: "hooked", Assignee: me}
	elsewhere := &beads.Issue{ID: "kasb-a", Status: "hooked", Assignee: "kasb_core/polecats/scavenger"}

	if hook, _ := agentHookBead(fakeIssueShower{issue: elsewhere}, &beads.Issue{HookBead: "kasb-col"}, &beads.AgentFields{HookBead: "kasb-a"}, me); hook != "kasb-col" {
		t.Errorf("hook column not authoritative: %q", hook)
	}
	if hook, stale := agentHookBead(fakeIssueShower{issue: mine}, nil, &beads.AgentFields{AgentState: "working", HookBead: "kasb-a"}, me); hook != "kasb-a" || stale != "" {
		t.Errorf("own hooked work dropped: (%q, %q)", hook, stale)
	}
	if hook, stale := agentHookBead(fakeIssueShower{issue: &beads.Issue{ID: "kasb-a", Status: "open"}}, nil, &beads.AgentFields{AgentState: "spawning", HookBead: "kasb-a"}, me); hook != "kasb-a" || stale != "" {
		t.Errorf("spawning agent's hook dropped: (%q, %q)", hook, stale)
	}
	if hook, stale := agentHookBead(fakeIssueShower{err: errors.New("dolt down")}, nil, &beads.AgentFields{AgentState: "working", HookBead: "kasb-a"}, me); hook != "kasb-a" || stale != "" {
		t.Errorf("unverifiable hook must be kept (fail closed): (%q, %q)", hook, stale)
	}
	// A landed (closed) bead stays the hook; hookBeadSafeForCleanup treats it as terminal.
	closed := fakeIssueShower{issue: &beads.Issue{ID: "kasb-a", Status: "closed", Assignee: "kasb_core/polecats/scavenger"}}
	hook, _ := agentHookBead(closed, nil, &beads.AgentFields{AgentState: "working", HookBead: "kasb-a"}, me)
	if safe, terminal, _ := hookBeadSafeForCleanup(closed, hook); !safe || !terminal {
		t.Errorf("landed hook not safe+terminal: safe=%v terminal=%v", safe, terminal)
	}
}

type fakeHookClearer struct {
	id, hook string
	fields   *beads.AgentFields
	cleared  bool
	err      error
}

// ClearAgentStaleHookIfMatches mimics the locked re-check: it clears only
// when stillStale (re-reading the work bead) still holds.
func (f *fakeHookClearer) ClearAgentStaleHookIfMatches(id, hook string, stillStale func(*beads.AgentFields) bool) (bool, error) {
	f.id, f.hook = id, hook
	if f.err != nil {
		return false, f.err
	}
	f.cleared = stillStale(f.fields)
	return f.cleared, nil
}

func TestClearStaleDescriptionHook(t *testing.T) {
	const me = "kasb_core/polecats/deathclaw"
	fields := &beads.AgentFields{AgentState: "working", HookBead: "kasb-qyar.185.2"}
	elsewhere := fakeIssueShower{issue: &beads.Issue{ID: "kasb-qyar.185.2", Status: "hooked", Assignee: "kasb_core/polecats/scavenger"}}

	status := &RecoveryStatus{}
	clearer := &fakeHookClearer{fields: fields}
	clearStaleDescriptionHook(clearer, elsewhere, "kasb-kasb_core-polecat-deathclaw", me, "kasb-qyar.185.2", status)
	if !clearer.cleared || clearer.id != "kasb-kasb_core-polecat-deathclaw" || clearer.hook != "kasb-qyar.185.2" {
		t.Fatalf("clear not applied: %+v", clearer)
	}
	if len(status.Diagnostics) != 1 || status.Diagnostics[0] != "stale_hook_cleared=kasb-qyar.185.2" {
		t.Fatalf("diagnostics = %v", status.Diagnostics)
	}
	if status.NeedsRecovery {
		t.Fatal("clearing a stale hook must not mark the polecat as needing recovery")
	}

	// Re-check under the lock: the bead was re-hooked to this polecat in the
	// meantime, so nothing is cleared.
	status = &RecoveryStatus{}
	clearer = &fakeHookClearer{fields: fields}
	mine := fakeIssueShower{issue: &beads.Issue{ID: "kasb-qyar.185.2", Status: "hooked", Assignee: me}}
	clearStaleDescriptionHook(clearer, mine, "kasb-kasb_core-polecat-deathclaw", me, "kasb-qyar.185.2", status)
	if clearer.cleared || len(status.Diagnostics) != 0 {
		t.Fatalf("cleared a hook that is real again: cleared=%v diag=%v", clearer.cleared, status.Diagnostics)
	}

	// Spawning agent: never stale.
	clearer = &fakeHookClearer{fields: &beads.AgentFields{AgentState: "spawning", HookBead: "kasb-qyar.185.2"}}
	clearStaleDescriptionHook(clearer, elsewhere, "kasb-kasb_core-polecat-deathclaw", me, "kasb-qyar.185.2", &RecoveryStatus{})
	if clearer.cleared {
		t.Fatal("cleared a spawning agent's hook")
	}
}

// setupUnhookNoSessionTown builds a town whose tmux has no server and whose
// bd stub serves: nothing hooked to gastown/polecats/nux, an agent bead whose
// description still names zz-stale, and zz-stale hooked to another polecat.
func setupUnhookNoSessionTown(t *testing.T) (logPath string) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("shell stubs")
	}
	townRoot := t.TempDir()
	for _, dir := range []string{"mayor/rig", "gastown/polecats/nux", "gastown/mayor/rig/.beads", ".beads", "bin"} {
		if err := os.MkdirAll(filepath.Join(townRoot, dir), 0755); err != nil {
			t.Fatal(err)
		}
	}
	if err := config.SaveTownConfig(filepath.Join(townRoot, "mayor", "town.json"), &config.TownConfig{
		Type: "town", Version: config.CurrentTownVersion, Name: "unhook-test", CreatedAt: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	rigs := &config.RigsConfig{Version: 1, Rigs: map[string]config.RigEntry{
		"gastown": {GitURL: "git@github.com:test/gastown.git", AddedAt: time.Now().Truncate(time.Second),
			BeadsConfig: &config.BeadsConfig{Repo: "local", Prefix: "zz-"}},
	}}
	if err := config.SaveRigsConfig(filepath.Join(townRoot, "mayor", "rigs.json"), rigs); err != nil {
		t.Fatal(err)
	}
	routes := `{"prefix":"zz-","path":"gastown/mayor/rig"}` + "\n"
	if err := os.WriteFile(filepath.Join(townRoot, ".beads", "routes.jsonl"), []byte(routes), 0644); err != nil {
		t.Fatal(err)
	}

	reg := session.NewPrefixRegistry()
	reg.Register("zz", "gastown")
	old := session.DefaultRegistry()
	session.SetDefaultRegistry(reg)
	t.Cleanup(func() { session.SetDefaultRegistry(old) })

	logPath = filepath.Join(townRoot, "bd.log")
	// Responses are steered per test through env vars:
	//   STUB_AGENT_STATE   agent_state in the agent description (default working)
	//   STUB_STALE_ASSIGNEE assignee of zz-stale (default another polecat)
	//   STUB_LIST_FAIL=1   every bd list fails
	//   STUB_IN_PROGRESS=1 an in_progress bead is assigned to the agent
	bdScript := `#!/bin/sh
echo "$*" >> "${BD_LOG}"
show_id=""; prev=""; prev_cmd=""; status=""
for a in "$@"; do
  if [ "$prev" = "show" ]; then show_id="$a"; fi
  case "$a" in --status=*) status="${a#--status=}" ;; esac
  case "$a" in --*) ;; *) [ -z "$prev_cmd" ] && prev_cmd="$a" ;; esac
  prev="$a"
done
state="${STUB_AGENT_STATE:-working}"
assignee="${STUB_STALE_ASSIGNEE-gastown/polecats/scavenger}"
case "$prev_cmd" in
  list)
    [ "${STUB_LIST_FAIL:-}" = "1" ] && { echo "dolt down" >&2; exit 1; }
    if [ "${STUB_IN_PROGRESS:-}" = "1" ] && [ "$status" = "in_progress" ]; then
      echo '[{"id":"zz-live","title":"Live","status":"in_progress","assignee":"gastown/polecats/nux"}]'
    else
      echo '[]'
    fi ;;
  show)
    case "$show_id" in
      zz-gastown-polecat-nux) printf '[{"id":"zz-gastown-polecat-nux","title":"zz-gastown-polecat-nux","status":"open","labels":["gt:agent"],"description":"zz-gastown-polecat-nux\\n\\nrole_type: polecat\\nrig: gastown\\nagent_state: %s\\nhook_bead: zz-stale\\ncleanup_status: null\\nactive_mr: null\\nnotification_level: null"}]\n' "$state" ;;
      zz-stale) printf '[{"id":"zz-stale","title":"Stale","status":"hooked","assignee":"%s"}]\n' "$assignee" ;;
      *) echo '[]' ;;
    esac ;;
  update) ;;
esac
exit 0
`
	_ = writeBDStub(t, filepath.Join(townRoot, "bin"), bdScript, "")
	tmux := "#!/bin/sh\necho 'no server running on /tmp/tmux' >&2\nexit 1\n"
	if err := os.WriteFile(filepath.Join(townRoot, "bin", "tmux"), []byte(tmux), 0755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("BD_LOG", logPath)
	t.Setenv("PATH", filepath.Join(townRoot, "bin")+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv(EnvGTRole, "mayor")
	t.Setenv("GT_TEST_NO_NUDGE", "1")
	cwd, _ := os.Getwd()
	t.Cleanup(func() { _ = os.Chdir(cwd) })
	if err := os.Chdir(filepath.Join(townRoot, "mayor", "rig")); err != nil {
		t.Fatal(err)
	}
	return logPath
}

// hq-ee6vdf: `gt unhook <target>` / `gt hook detach` must not need a live
// pane ("getting pane for kasb-<p>"), and clears the stale description line.
func TestUnslingTargetWorksWithoutSessionAndClearsStaleHook(t *testing.T) {
	logPath := setupUnhookNoSessionTown(t)

	if _, _, _, err := resolveTargetAgent("gastown/polecats/nux"); err == nil {
		t.Fatal("precondition: resolveTargetAgent should fail with no tmux session")
	}
	agentID, err := resolveTargetAgentID("gastown/polecats/nux")
	if err != nil || agentID != "gastown/polecats/nux" {
		t.Fatalf("resolveTargetAgentID = (%q, %v)", agentID, err)
	}

	cmd := &cobra.Command{}
	if err := runUnslingWith(cmd, []string{"gastown/polecats/nux"}, false, false); err != nil {
		t.Fatalf("unhook with no session failed: %v", err)
	}
	log, _ := os.ReadFile(logPath)
	var cleared bool
	for _, line := range strings.Split(string(log), "\n") {
		if strings.Contains(line, "update") && strings.Contains(line, "zz-gastown-polecat-nux") {
			if strings.Contains(line, "hook_bead: zz-stale") {
				t.Fatalf("description update kept the stale hook: %s", line)
			}
			cleared = true
		}
		if strings.Contains(line, "update") && strings.Contains(line, "zz-stale") && !strings.Contains(line, "zz-gastown-polecat-nux") {
			t.Fatalf("unhook modified the other polecat's bead: %s", line)
		}
	}
	if !cleared {
		t.Fatalf("stale hook_bead was not cleared; bd log:\n%s", log)
	}
}

// agentDescriptionCleared reports whether the bd log shows the agent bead's
// description rewritten without the stale hook.
func agentDescriptionCleared(t *testing.T, logPath string) bool {
	t.Helper()
	log, _ := os.ReadFile(logPath)
	for _, line := range strings.Split(string(log), "\n") {
		if strings.Contains(line, "update") && strings.Contains(line, "zz-gastown-polecat-nux") && !strings.Contains(line, "hook_bead: zz-stale") {
			return true
		}
	}
	return false
}

// The clear needs proof every time, explicit bead or not (review item 6).
func TestUnslingRefusesUnprovenStaleHookClear(t *testing.T) {
	cases := []struct {
		name string
		env  map[string]string
		args []string
	}{
		{"explicit bead still mine", map[string]string{"STUB_STALE_ASSIGNEE": "gastown/polecats/nux"}, []string{"zz-stale", "gastown/polecats/nux"}},
		{"list error", map[string]string{"STUB_LIST_FAIL": "1"}, []string{"gastown/polecats/nux"}},
		{"in_progress work assigned", map[string]string{"STUB_IN_PROGRESS": "1"}, []string{"gastown/polecats/nux"}},
		{"agent spawning", map[string]string{"STUB_AGENT_STATE": "spawning", "STUB_STALE_ASSIGNEE": ""}, []string{"gastown/polecats/nux"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			logPath := setupUnhookNoSessionTown(t)
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			_ = runUnslingWith(&cobra.Command{}, tc.args, false, false)
			if agentDescriptionCleared(t, logPath) {
				t.Fatal("description hook cleared without proof that it is stale")
			}
		})
	}
}

// An explicit bead that is assigned elsewhere is cleared.
func TestUnslingExplicitStaleBeadIsCleared(t *testing.T) {
	logPath := setupUnhookNoSessionTown(t)
	if err := runUnslingWith(&cobra.Command{}, []string{"zz-stale", "gastown/polecats/nux"}, false, false); err != nil {
		t.Fatalf("detach failed: %v", err)
	}
	if !agentDescriptionCleared(t, logPath) {
		t.Fatal("explicit stale bead was not cleared")
	}
}

func TestSameAgentAddressTrailingSlash(t *testing.T) {
	if !beads.SameAgentAddress("mayor/", "mayor") || !beads.SameAgentAddress("gastown/polecats/nux", "gastown/nux/") {
		t.Fatal("trailing slash must not matter")
	}
	if beads.SameAgentAddress("", "/") {
		t.Fatal("empty addresses must not match")
	}
}
