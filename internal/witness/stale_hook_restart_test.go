package witness

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/steveyegge/gastown/internal/beads"
)

// staleHookBd serves a dead polecat's lookups: show returns hookJSON for the
// description hook bead, list returns listJSON (its hooked/in-progress work).
func staleHookBd(hookJSON, listJSON string, listErr error) (*BdCli, *mockBdCalls) {
	return mockBd(
		func(args []string) (string, error) {
			if len(args) == 0 {
				return "", nil
			}
			switch args[0] {
			case "show":
				return hookJSON, nil
			case "list":
				return listJSON, listErr
			}
			return "", nil
		},
		func(args []string) error { return nil },
	)
}

func workingSnap(hook string) *agentBeadSnapshot {
	return &agentBeadSnapshot{
		AgentState: "working",
		Fields:     &beads.AgentFields{AgentState: "working", HookBead: hook},
	}
}

// hq-ee6vdf: deathclaw's description says hook_bead: kasb-qyar.185.2, which is
// hooked to scavenger; deathclaw holds nothing. Its dead session is not a
// session-dead-active zombie and must not be restarted.
func TestDetectZombieDeadSession_SkipsRestartForEmptyRealHook(t *testing.T) {
	bd, calls := staleHookBd(`[{"id":"kasb-qyar.185.2","status":"hooked","assignee":"kasb_core/polecats/scavenger"}]`, `[]`, nil)

	zombie, found := detectZombieDeadSession(bd, t.TempDir(), t.TempDir(), "kasb_core", "deathclaw",
		"kasb-deathclaw", nil, nil, time.Now(), nil, workingSnap("kasb-qyar.185.2"))
	if found {
		t.Fatalf("empty-hook polecat classified as zombie: %+v", zombie)
	}
	for _, c := range calls.calls {
		if strings.HasPrefix(c, "update") || strings.HasPrefix(c, "create") {
			t.Fatalf("restart path wrote beads: %v", calls.calls)
		}
	}
}

func TestDeadSessionHoldsWork(t *testing.T) {
	elsewhere := `[{"id":"kasb-x","status":"hooked","assignee":"kasb_core/polecats/scavenger"}]`
	mine := `[{"id":"kasb-x","status":"hooked","assignee":"kasb_core/polecats/deathclaw"}]`
	parked := `[{"id":"kasb-x","status":"open"}]`
	landed := `[{"id":"kasb-x","status":"closed","assignee":"kasb_core/polecats/deathclaw"}]`
	assigned := `[{"id":"kasb-y","status":"in_progress","assignee":"kasb_core/polecats/deathclaw"}]`

	cases := []struct {
		name     string
		snapHook string
		snap     *agentBeadSnapshot
		show     string
		list     string
		listErr  error
		want     bool
	}{
		{"stale: hooked to another polecat", "", workingSnap("kasb-x"), elsewhere, "[]", nil, false},
		{"stale: parked", "", workingSnap("kasb-x"), parked, "[]", nil, false},
		{"stale: landed", "", workingSnap("kasb-x"), landed, "[]", nil, false},
		{"no hook at all, working state", "", workingSnap(""), "", "[]", nil, false},
		{"description hook is still mine", "", workingSnap("kasb-x"), mine, "[]", nil, true},
		{"stale line but other work assigned", "", workingSnap("kasb-x"), elsewhere, assigned, nil, true},
		{"hook column trusted", "kasb-col", workingSnap(""), "", "[]", nil, true},
		{"list failure keeps restart", "", workingSnap(""), "", "", errors.New("dolt down"), true},
		{"spawning keeps its hook", "", &agentBeadSnapshot{AgentState: "spawning", Fields: &beads.AgentFields{AgentState: "spawning", HookBead: "kasb-x"}}, parked, "[]", nil, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bd, _ := staleHookBd(tc.show, tc.list, tc.listErr)
			if got := deadSessionHoldsWork(bd, t.TempDir(), "kasb_core", "deathclaw", tc.snapHook, tc.snap); got != tc.want {
				t.Fatalf("deadSessionHoldsWork = %v, want %v", got, tc.want)
			}
		})
	}
}
