package witness

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
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
			if got := deadSessionHoldsWork(bd, t.TempDir(), t.TempDir(), "kasb_core", "deathclaw", tc.snapHook, tc.snap); got != tc.want {
				t.Fatalf("deadSessionHoldsWork = %v, want %v", got, tc.want)
			}
		})
	}
}

// Review item 5: the fallback counts open and blocked work too (as the
// daemon's hasAssignedOpenWork), lists in the rig database, and reads the
// description hook bead from its own database by prefix.
func TestDeadSessionHoldsWork_StatusesAndRouting(t *testing.T) {
	townRoot := t.TempDir()
	for _, dir := range []string{".beads", "kasb_core/mayor/rig/.beads", "hisn/mayor/rig/.beads"} {
		if err := os.MkdirAll(filepath.Join(townRoot, dir), 0755); err != nil {
			t.Fatal(err)
		}
	}
	routes := `{"prefix":"kasb-","path":"kasb_core/mayor/rig"}` + "\n" + `{"prefix":"hisn-","path":"hisn/mayor/rig"}` + "\n"
	if err := os.WriteFile(filepath.Join(townRoot, ".beads", "routes.jsonl"), []byte(routes), 0644); err != nil {
		t.Fatal(err)
	}
	type call struct {
		dir  string
		args string
	}
	var calls []call
	bd := &BdCli{
		Exec: func(workDir string, args ...string) (string, error) {
			calls = append(calls, call{workDir, strings.Join(args, " ")})
			if args[0] == "show" {
				return `[{"id":"hisn-x","status":"hooked","assignee":"kasb_core/polecats/other"}]`, nil
			}
			return `[{"id":"kasb-open","status":"open","assignee":"kasb_core/polecats/deathclaw"}]`, nil
		},
		Run: func(string, ...string) error { return nil },
	}
	if !deadSessionHoldsWork(bd, filepath.Join(townRoot, "witness"), townRoot, "kasb_core", "deathclaw", "", workingSnap("hisn-x")) {
		t.Fatal("assigned open work not counted")
	}
	if len(calls) != 2 {
		t.Fatalf("calls = %+v", calls)
	}
	if calls[0].dir != filepath.Join(townRoot, "hisn/mayor/rig") {
		t.Errorf("hook bead read from %q, want its prefix database", calls[0].dir)
	}
	if calls[1].dir != filepath.Join(townRoot, "kasb_core/mayor/rig") {
		t.Errorf("assigned work listed in %q, want the rig database", calls[1].dir)
	}
	if !strings.Contains(calls[1].args, "--status=hooked,in_progress,open,blocked") {
		t.Errorf("list statuses = %q, want hooked,in_progress,open,blocked", calls[1].args)
	}
}

// Review item 7: slotOpenDecision's description-hook check uses the same
// stale rule.
func TestWitnessDescriptionHookStale(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell stub")
	}
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, ".beads"), 0755); err != nil {
		t.Fatal(err)
	}
	binDir := t.TempDir()
	stub := "#!/bin/sh\nfor a in \"$@\"; do case \"$a\" in show) echo \"[{\\\"id\\\":\\\"kasb-x\\\",\\\"status\\\":\\\"hooked\\\",\\\"assignee\\\":\\\"${STUB_ASSIGNEE}\\\"}]\"; exit 0;; esac; done\nexit 0\n"
	if err := os.WriteFile(filepath.Join(binDir, "bd"), []byte(stub), 0755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	fields := &beads.AgentFields{AgentState: "working", HookBead: "kasb-x"}

	t.Setenv("STUB_ASSIGNEE", "kasb_core/polecats/scavenger")
	if !witnessDescriptionHookStale(beads.New(dir), fields, "kasb_core/polecats/deathclaw") {
		t.Error("hook assigned to another polecat not stale")
	}
	t.Setenv("STUB_ASSIGNEE", "kasb_core/polecats/deathclaw")
	if witnessDescriptionHookStale(beads.New(dir), fields, "kasb_core/polecats/deathclaw") {
		t.Error("own hook judged stale")
	}
}
