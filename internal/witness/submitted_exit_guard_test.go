package witness

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/steveyegge/gastown/internal/beads"
	"github.com/steveyegge/gastown/internal/polecat"
	"github.com/steveyegge/gastown/internal/tmux"
)

// withSubmittedExitProbes replaces the worktree probes; tests using it must
// not run in parallel.
func withSubmittedExitProbes(t *testing.T, dirty string, dirtyErr error, activity string) *[]string {
	t.Helper()
	oldDirty, oldActivity := submittedExitWorktreeDirty, submittedExitSandboxActivity
	var probed []string
	submittedExitWorktreeDirty = func(string) (string, error) {
		probed = append(probed, "dirty")
		return dirty, dirtyErr
	}
	submittedExitSandboxActivity = func(*tmux.Tmux, string, string, string) string {
		probed = append(probed, "activity")
		return activity
	}
	t.Cleanup(func() { submittedExitWorktreeDirty, submittedExitSandboxActivity = oldDirty, oldActivity })
	return &probed
}

// submittedBd answers bd show for hook beads and bd list for assigned work.
func submittedBd(shows map[string]string, list string, listErr error) *BdCli {
	bd, _ := mockBd(func(args []string) (string, error) {
		switch args[0] {
		case "show":
			if out, ok := shows[args[1]]; ok {
				return out, nil
			}
			return "", errors.New("issue not found")
		case "list":
			return list, listErr
		}
		return "", nil
	}, func([]string) error { return nil })
	return bd
}

func TestSubmittedExitNudgeVeto(t *testing.T) {
	const nuka = "kasb_core/polecats/nuka"
	town := t.TempDir()
	for _, tc := range []struct {
		name      string
		snapHook  string
		shows     map[string]string
		list      string
		listErr   error
		dirty     string
		dirtyErr  error
		activity  string
		wantVeto  string // substring; "" means the nudge may go
		wantProbe int    // worktree probes run
	}{
		{
			name:     "no hook column but a bead hooked to the polecat (nuka, 24 Nov)",
			list:     `[{"id":"kasb-qyar.121.11","status":"hooked"}]`,
			wantVeto: "assigned_work=kasb-qyar.121.11 status=hooked",
		},
		{
			name:     "open hook assigned to the polecat",
			snapHook: "kasb-x.1",
			shows:    map[string]string{"kasb-x.1": `[{"id":"kasb-x.1","status":"in_progress","assignee":"` + nuka + `"}]`},
			list:     `[]`,
			wantVeto: "assigned_work=kasb-x.1 status=in_progress",
		},
		{
			name:     "assigned-work lookup fails",
			list:     ``,
			listErr:  errors.New("dolt timeout"),
			wantVeto: "assigned_work=unknown",
		},
		{
			name:     "hook lookup fails",
			snapHook: "kasb-gone",
			list:     `[]`,
			wantVeto: "assigned_work=unknown",
		},
		{
			name:      "dirty worktree",
			list:      `[]`,
			dirty:     "worktree_dirty=1 uncommitted tracked change(s)",
			wantVeto:  "worktree_dirty=1",
			wantProbe: 1,
		},
		{
			name:      "git status fails",
			list:      `[]`,
			dirtyErr:  errors.New("not a git repository"),
			wantVeto:  "worktree_dirty=unknown",
			wantProbe: 1,
		},
		{
			name:      "tests running in the sandbox",
			list:      `[]`,
			activity:  "live_process_in_worktree pid=35041(python3)",
			wantVeto:  "live_process_in_worktree",
			wantProbe: 2,
		},
		{
			name:      "async preflight running",
			list:      `[]`,
			activity:  "async_preflight=running source=kasb-qyar.121.11",
			wantVeto:  "async_preflight=running",
			wantProbe: 2,
		},
		{
			name:      "held-for-merge hook (unassigned by gt done), clean and idle",
			snapHook:  "kasb-x.2",
			shows:     map[string]string{"kasb-x.2": `[{"id":"kasb-x.2","status":"in_progress","assignee":""}]`},
			list:      `[]`,
			wantProbe: 2,
		},
		{
			name:      "nothing assigned, clean and idle",
			list:      `[]`,
			wantProbe: 2,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			probed := withSubmittedExitProbes(t, tc.dirty, tc.dirtyErr, tc.activity)
			bd := submittedBd(tc.shows, tc.list, tc.listErr)
			got := submittedExitNudgeVeto(bd, town, town, "kasb_core", "nuka", "kasb-nuka", tc.snapHook, &agentBeadSnapshot{HookBead: tc.snapHook}, nil)
			if tc.wantVeto == "" && got != "" {
				t.Errorf("veto = %q, want none", got)
			}
			if tc.wantVeto != "" && !strings.Contains(got, tc.wantVeto) {
				t.Errorf("veto = %q, want %q", got, tc.wantVeto)
			}
			if len(*probed) != tc.wantProbe {
				t.Errorf("worktree probes = %v, want %d", *probed, tc.wantProbe)
			}
		})
	}
}

// kasb-2ifwk: the scan must return no zombie (and send no nudge) for a
// submitted-looking worker that holds a live assignment. A nil tmux would
// panic if the nudge were attempted.
func TestDetectSubmittedStillRunningSkipsAssignedWorker(t *testing.T) {
	withSubmittedExitProbes(t, "", nil, "")
	snap := &agentBeadSnapshot{
		AgentState: string(beads.AgentStateWorking),
		UpdatedAt:  time.Now().Add(-15 * time.Minute).Format(time.RFC3339),
		Fields:     &beads.AgentFields{CleanupStatus: "clean", ActiveMR: "kasb-wisp-old"},
	}
	stale := &polecat.SessionHeartbeat{Timestamp: time.Now().Add(-15 * time.Minute), State: polecat.HeartbeatWorking}
	bd := submittedBd(nil, `[{"id":"kasb-qyar.121.11","status":"hooked"}]`, nil)
	town := t.TempDir()
	if z, found := detectSubmittedStillRunning(bd, town, town, "kasb_core", "nuka", "kasb-nuka", nil, stale, snap, time.Minute); found {
		t.Fatalf("detectSubmittedStillRunning = %+v, want no nudge", z)
	}
}
