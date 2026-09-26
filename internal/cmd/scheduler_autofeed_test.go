package cmd

import (
	"encoding/json"
	"testing"

	"github.com/steveyegge/gastown/internal/scheduler/capacity"
)

func autoFeedTestCandidate(id string, priority int, created string, labels ...string) autoFeedCandidate {
	return autoFeedCandidate{
		ID:        id,
		Title:     id + " title",
		Status:    "open",
		Type:      "task",
		Priority:  priority,
		CreatedAt: created,
		Labels:    labels,
		Rig:       "testrig",
	}
}

func TestAutoFeedResumesPushedHandoffBranch(t *testing.T) {
	branch := "polecat/opal/hisn-a"
	head := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	for _, value := range []any{map[string]string{"branch": branch, "head": head},
		`{"branch":"polecat/opal/hisn-a","head":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`} {
		raw, err := json.Marshal(map[string]any{"handoff": value})
		if err != nil {
			t.Fatal(err)
		}
		if got := autoFeedResumeBranch(raw); got != branch {
			t.Fatalf("resume branch = %q", got)
		}
	}
	if got := autoFeedResumeBranch(json.RawMessage(`{"handoff":{"branch":"main","head":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}`)); got != "" {
		t.Fatalf("unsafe resume branch = %q", got)
	}
}

func autoFeedIDs(candidates []autoFeedCandidate) []string {
	ids := make([]string, 0, len(candidates))
	for _, c := range candidates {
		ids = append(ids, c.ID)
	}
	return ids
}

func rejectionReason(rejections []autoFeedRejection, id string) string {
	for _, r := range rejections {
		if r.ID == id {
			return r.Reason
		}
	}
	return ""
}

func defaultAutoFeedPolicy() autoFeedPolicy {
	return autoFeedPolicy{
		ExcludeLabels: capacity.DefaultAutoFeedExcludeLabels,
		MaxOpsSlots:   capacity.DefaultAutoFeedMaxOpsSlots,
	}
}

func TestControlPlaneIsTopicAndDispatchHoldIsExplicit(t *testing.T) {
	candidates := []autoFeedCandidate{
		autoFeedTestCandidate("hisn-topic", 2, "2026-01-01T00:00:00Z", "control-plane"),
		autoFeedTestCandidate("hisn-held", 2, "2026-01-02T00:00:00Z", "control-plane", "dispatch:hold"),
	}
	selected, rejected := selectAutoFeedCandidates(candidates, defaultAutoFeedPolicy(), nil, 2)
	if len(selected) != 1 || selected[0].ID != "hisn-topic" {
		t.Fatalf("selected %v, want only the topic-labelled bead", autoFeedIDs(selected))
	}
	if got := rejectionReason(rejected, "hisn-held"); got != "exclude-label=dispatch:hold" {
		t.Fatalf("held rejection = %q", got)
	}
	if isOpsBead([]string{"control-plane"}) {
		t.Fatal("control-plane topic consumed an operations slot")
	}
	if isOpsBead([]string{"deep-dive"}) {
		t.Fatal("deep-dive analysis consumed an operations slot")
	}
}

func TestDeepDiveIsDispatchable(t *testing.T) {
	selected, rejected := selectAutoFeedCandidates([]autoFeedCandidate{
		autoFeedTestCandidate("hisn-analysis", 2, "2026-01-01T00:00:00Z", "deep-dive"),
	}, defaultAutoFeedPolicy(), nil, 1)
	if len(selected) != 1 || selected[0].ID != "hisn-analysis" || len(rejected) != 0 {
		t.Fatalf("deep-dive selected=%v rejected=%v", autoFeedIDs(selected), rejected)
	}
}

func TestSortAutoFeedCandidates_PriorityThenAgeThenID(t *testing.T) {
	candidates := []autoFeedCandidate{
		autoFeedTestCandidate("bt-c", 2, "2026-01-01T00:00:00Z"),
		autoFeedTestCandidate("bt-a", 0, "2026-03-01T00:00:00Z"),
		autoFeedTestCandidate("bt-b", 1, "2026-02-01T00:00:00Z"),
		autoFeedTestCandidate("bt-d", 2, "2026-01-01T00:00:00Z"),
		autoFeedTestCandidate("bt-e", 2, "2025-12-01T00:00:00Z"),
	}
	sortAutoFeedCandidates(candidates)

	want := []string{"bt-a", "bt-b", "bt-e", "bt-c", "bt-d"}
	got := autoFeedIDs(candidates)
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("sort order = %v, want %v", got, want)
		}
	}
}

func TestSelectAutoFeedCandidates_HonorsFloorLimit(t *testing.T) {
	candidates := []autoFeedCandidate{
		autoFeedTestCandidate("bt-1", 2, "2026-01-01T00:00:00Z"),
		autoFeedTestCandidate("bt-2", 1, "2026-01-01T00:00:00Z"),
		autoFeedTestCandidate("bt-3", 3, "2026-01-01T00:00:00Z"),
	}

	selected, rejected := selectAutoFeedCandidates(candidates, defaultAutoFeedPolicy(), nil, 2)
	if len(selected) != 2 {
		t.Fatalf("selected %v, want 2", autoFeedIDs(selected))
	}
	if selected[0].ID != "bt-2" || selected[1].ID != "bt-1" {
		t.Fatalf("selected %v, want [bt-2 bt-1]", autoFeedIDs(selected))
	}
	if got := rejectionReason(rejected, "bt-3"); got != "floor-met" {
		t.Fatalf("bt-3 rejection = %q, want floor-met", got)
	}
}

func TestSelectAutoFeedCandidates_RejectionReasons(t *testing.T) {
	notOpen := autoFeedTestCandidate("bt-status", 2, "2026-01-01T00:00:00Z")
	notOpen.Status = "in_progress"

	assigned := autoFeedTestCandidate("bt-assigned", 2, "2026-01-01T00:00:00Z")
	assigned.Assignee = "testrig/polecats/nux"

	epic := autoFeedTestCandidate("bt-epic", 2, "2026-01-01T00:00:00Z")
	epic.Type = "epic"

	candidates := []autoFeedCandidate{
		notOpen,
		assigned,
		epic,
		autoFeedTestCandidate("bt-excluded", 2, "2026-01-01T00:00:00Z", "dispatch:hold"),
		autoFeedTestCandidate("bt-operator", 2, "2026-01-01T00:00:00Z", "needs-operator"),
		autoFeedTestCandidate("bt-scheduled", 2, "2026-01-01T00:00:00Z"),
		autoFeedTestCandidate("bt-good", 2, "2026-01-01T00:00:00Z"),
	}
	scheduled := map[string]bool{"bt-scheduled": true}

	selected, rejected := selectAutoFeedCandidates(candidates, defaultAutoFeedPolicy(), scheduled, 10)
	if len(selected) != 1 || selected[0].ID != "bt-good" {
		t.Fatalf("selected %v, want [bt-good]", autoFeedIDs(selected))
	}

	cases := map[string]string{
		"bt-status":    "status=in_progress",
		"bt-assigned":  "assignee=testrig/polecats/nux",
		"bt-epic":      "type=epic",
		"bt-excluded":  "exclude-label=dispatch:hold",
		"bt-operator":  "exclude-label=needs-operator",
		"bt-scheduled": "already-scheduled",
	}
	for id, want := range cases {
		if got := rejectionReason(rejected, id); got != want {
			t.Errorf("%s rejection = %q, want %q", id, got, want)
		}
	}
}

func TestSelectAutoFeedCandidates_IntrinsicReasonsBeatFloorMet(t *testing.T) {
	// With the floor already met, an operator still needs the real reason a
	// bead is undispatchable — "exclude-label=needs-operator", not
	// "floor-met".
	candidates := []autoFeedCandidate{
		autoFeedTestCandidate("bt-first", 0, "2026-01-01T00:00:00Z"),
		autoFeedTestCandidate("bt-held", 1, "2026-01-01T00:00:00Z", "needs-operator"),
		autoFeedTestCandidate("bt-next", 2, "2026-01-01T00:00:00Z"),
	}

	selected, rejected := selectAutoFeedCandidates(candidates, defaultAutoFeedPolicy(), nil, 1)
	if got := autoFeedIDs(selected); len(got) != 1 || got[0] != "bt-first" {
		t.Fatalf("selected %v, want [bt-first]", got)
	}
	if got := rejectionReason(rejected, "bt-held"); got != "exclude-label=needs-operator" {
		t.Errorf("bt-held rejection = %q, want exclude-label=needs-operator", got)
	}
	if got := rejectionReason(rejected, "bt-next"); got != "floor-met" {
		t.Errorf("bt-next rejection = %q, want floor-met", got)
	}
}

func TestSelectAutoFeedCandidates_AllowList(t *testing.T) {
	policy := defaultAutoFeedPolicy()
	policy.AllowLabels = []string{"product"}

	candidates := []autoFeedCandidate{
		autoFeedTestCandidate("bt-yes", 2, "2026-01-01T00:00:00Z", "product"),
		autoFeedTestCandidate("bt-no", 1, "2026-01-01T00:00:00Z", "docs"),
	}

	selected, rejected := selectAutoFeedCandidates(candidates, policy, nil, 10)
	if len(selected) != 1 || selected[0].ID != "bt-yes" {
		t.Fatalf("selected %v, want [bt-yes]", autoFeedIDs(selected))
	}
	if got := rejectionReason(rejected, "bt-no"); got != "not-in-allow-list" {
		t.Fatalf("bt-no rejection = %q, want not-in-allow-list", got)
	}
}

func TestSelectAutoFeedCandidates_OpsSlotCap(t *testing.T) {
	candidates := []autoFeedCandidate{
		autoFeedTestCandidate("bt-ops1", 2, "2026-01-01T00:00:00Z", "ci-train-failure"),
		autoFeedTestCandidate("bt-ops2", 2, "2026-01-02T00:00:00Z", "watchdog"),
		autoFeedTestCandidate("bt-product", 2, "2026-01-03T00:00:00Z"),
	}

	selected, rejected := selectAutoFeedCandidates(candidates, defaultAutoFeedPolicy(), nil, 10)
	if got := autoFeedIDs(selected); len(got) != 2 || got[0] != "bt-ops1" || got[1] != "bt-product" {
		t.Fatalf("selected %v, want [bt-ops1 bt-product]", got)
	}
	if got := rejectionReason(rejected, "bt-ops2"); got != "ops-slot-cap=1" {
		t.Fatalf("bt-ops2 rejection = %q, want ops-slot-cap=1", got)
	}
}

func TestSelectAutoFeedCandidates_OpsSlotCapCountsWorkingPolecats(t *testing.T) {
	policy := defaultAutoFeedPolicy()
	policy.OpsWorking = 1

	candidates := []autoFeedCandidate{
		autoFeedTestCandidate("bt-ops", 2, "2026-01-01T00:00:00Z", "ci"),
	}
	selected, rejected := selectAutoFeedCandidates(candidates, policy, nil, 10)
	if len(selected) != 0 {
		t.Fatalf("selected %v, want none (ops slot already used)", autoFeedIDs(selected))
	}
	if got := rejectionReason(rejected, "bt-ops"); got != "ops-slot-cap=1" {
		t.Fatalf("bt-ops rejection = %q, want ops-slot-cap=1", got)
	}
}

func TestSelectAutoFeedCandidates_OpsCapBypassedForP0P1(t *testing.T) {
	policy := defaultAutoFeedPolicy()
	policy.OpsWorking = 5

	candidates := []autoFeedCandidate{
		autoFeedTestCandidate("bt-p1", 1, "2026-01-01T00:00:00Z", "ci-train-failure"),
		autoFeedTestCandidate("bt-p2", 2, "2026-01-01T00:00:00Z", "ci-train-failure"),
	}
	selected, _ := selectAutoFeedCandidates(candidates, policy, nil, 10)
	if got := autoFeedIDs(selected); len(got) != 1 || got[0] != "bt-p1" {
		t.Fatalf("selected %v, want [bt-p1] (P1 ops work bypasses the cap)", got)
	}
}

func TestSchedulerConfigAutoFeedDefaults(t *testing.T) {
	var cfg *capacity.SchedulerConfig
	if got := cfg.GetQueueFloor(); got != 0 {
		t.Errorf("nil GetQueueFloor() = %d, want 0", got)
	}
	if got := cfg.GetAutoFeedMaxOpsSlots(); got != 1 {
		t.Errorf("nil GetAutoFeedMaxOpsSlots() = %d, want 1", got)
	}
	if got := cfg.GetAutoFeedExcludeLabels(); len(got) != len(capacity.DefaultAutoFeedExcludeLabels) {
		t.Errorf("nil GetAutoFeedExcludeLabels() = %v, want defaults", got)
	}
	if got := cfg.GetAutoFeedLabels(); got != nil {
		t.Errorf("nil GetAutoFeedLabels() = %v, want nil", got)
	}

	empty := []string{}
	explicit := &capacity.SchedulerConfig{AutoFeedExcludeLabels: empty}
	if got := explicit.GetAutoFeedExcludeLabels(); len(got) != 0 {
		t.Errorf("explicit empty exclude list = %v, want empty", got)
	}
}

func TestIsOpsBead(t *testing.T) {
	if !isOpsBead([]string{"CI-Train-Failure"}) {
		t.Error("ci-train-failure should be an ops label (case-insensitive)")
	}
	if !isOpsBead([]string{"needs-operator"}) {
		t.Error("needs-operator should be an ops label")
	}
	if isOpsBead([]string{"auth", "oauth"}) {
		t.Error("product labels should not be ops labels")
	}
}

func TestAutoFeedPrefersReadyPreservedPrerequisiteWithinPriority(t *testing.T) {
	rows := []autoFeedCandidate{
		{ID: "hisn-fresh", Rig: "hisn", Status: "open", Priority: 2, CreatedAt: "2026-09-01"},
		{ID: "hisn-blocker", Rig: "hisn", Status: "open", Priority: 2, CreatedAt: "2026-09-20", UnblocksPreserved: 2},
		{ID: "hisn-urgent", Rig: "hisn", Status: "open", Priority: 1, CreatedAt: "2026-09-20"},
		{ID: "hisn-held", Rig: "hisn", Status: "open", Priority: 2, UnblocksPreserved: 5, Labels: []string{"needs-operator"}},
	}
	selected, _ := selectAutoFeedCandidates(rows, autoFeedPolicy{ExcludeLabels: []string{"needs-operator"}}, nil, 3)
	for i, want := range []string{"hisn-urgent", "hisn-blocker", "hisn-fresh"} {
		if len(selected) != 3 || selected[i].ID != want {
			t.Fatalf("unexpected selection: %+v", selected)
		}
	}
}

// Spikes are leaf research work: the feeder must enqueue them rather than
// reject them as an unslingable type (hisn-4s8b.1).
func TestSelectAutoFeedCandidates_EnqueuesSpikes(t *testing.T) {
	spike := autoFeedTestCandidate("hisn-spike", 3, "2026-01-01T00:00:00Z")
	spike.Type = "spike"

	selected, rejected := selectAutoFeedCandidates([]autoFeedCandidate{spike}, defaultAutoFeedPolicy(), nil, 10)
	if len(selected) != 1 || selected[0].ID != "hisn-spike" {
		t.Fatalf("selected %v, rejected %v, want [hisn-spike]", autoFeedIDs(selected), rejected)
	}
}

// Parked and docked rigs are not scanned, so their ready work never enters
// the shared dispatch queue (hisn-4s8b.1).
func TestAutoFeedRigs_SkipsParkedAndDockedRigs(t *testing.T) {
	state := map[string]string{"bridge_town_core": "parked", "old_rig": "docked"}
	feed, skipped := autoFeedRigs([]string{"bridge_town_core", "hisn", "old_rig"}, func(rigName string) (bool, string) {
		reason, ok := state[rigName]
		return ok, reason
	})

	if len(feed) != 1 || feed[0] != "hisn" {
		t.Fatalf("feed = %v, want [hisn]", feed)
	}
	want := []autoFeedSkippedRig{{Rig: "bridge_town_core", Reason: "parked"}, {Rig: "old_rig", Reason: "docked"}}
	if len(skipped) != len(want) {
		t.Fatalf("skipped = %v, want %v", skipped, want)
	}
	for i := range want {
		if skipped[i] != want[i] {
			t.Errorf("skipped[%d] = %v, want %v", i, skipped[i], want[i])
		}
	}
}
