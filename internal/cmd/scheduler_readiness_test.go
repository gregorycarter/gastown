package cmd

import (
	"testing"

	"github.com/steveyegge/gastown/internal/beads"
	"github.com/steveyegge/gastown/internal/scheduler/capacity"
)

func TestScheduledParentConvertedToTrackingContainer(t *testing.T) {
	for _, status := range []string{"open", "in_progress"} {
		info := beadStatusInfo{Status: status, Labels: []string{"backlog-container"}}
		if isScheduledWorkBeadReady("hisn-parent", info, true, nil, nil) {
			t.Fatal("an already queued parent must stop dispatching after decomposition")
		}
		assessment := scheduledContextAssessment{found: true, info: info}
		if got := assessment.pauseReason(); got != "tracking-container (dispatch its children)" {
			t.Fatalf("missing parent disposition: %s", got)
		}
	}
	if !isScheduledWorkBeadReady("hisn-parent.1", beadStatusInfo{Status: "open"}, true, nil, nil) {
		t.Fatal("the independent child remains dispatchable")
	}
}

func TestIsScheduledWorkBeadReady_InProgressUnassigned(t *testing.T) {
	info := beadStatusInfo{Status: "in_progress"}
	if !isScheduledWorkBeadReady("bt-1", info, true, nil, nil) {
		t.Fatal("in_progress with no assignee should be ready (stalled claim, not work in flight)")
	}
}

func TestIsScheduledWorkBeadReady_InProgressLiveAssignee(t *testing.T) {
	prev := isHookedAgentDeadFn
	isHookedAgentDeadFn = func(string) bool { return false }
	defer func() { isHookedAgentDeadFn = prev }()

	info := beadStatusInfo{Status: "in_progress", Assignee: "bt/polecats/nux"}
	if isScheduledWorkBeadReady("bt-1", info, true, nil, nil) {
		t.Fatal("in_progress with a live assignee session must not be ready")
	}
}

func TestIsScheduledWorkBeadReady_InProgressDeadAssignee(t *testing.T) {
	prev := isHookedAgentDeadFn
	isHookedAgentDeadFn = func(string) bool { return true }
	defer func() { isHookedAgentDeadFn = prev }()

	info := beadStatusInfo{Status: "in_progress", Assignee: "bt/polecats/nux"}
	if !isScheduledWorkBeadReady("bt-1", info, true, nil, nil) {
		t.Fatal("in_progress whose assignee session is gone should be ready")
	}
}

func TestIsScheduledWorkBeadReady_BlockedAndTerminal(t *testing.T) {
	open := beadStatusInfo{Status: "open"}
	if isScheduledWorkBeadReady("bt-1", open, true, map[string]bool{"bt-1": true}, nil) {
		t.Error("blocked bead must not be ready")
	}
	if isScheduledWorkBeadReady("bt-1", open, true, nil, map[string]bool{"bt-1": true}) {
		t.Error("bead with unknown blocked state must not be ready")
	}
	if isScheduledWorkBeadReady("bt-1", beadStatusInfo{Status: "hooked"}, true, nil, nil) {
		t.Error("hooked bead must not be ready")
	}
	if isScheduledWorkBeadReady("bt-1", open, false, nil, nil) {
		t.Error("missing bead must not be ready")
	}
}

func TestScheduledContextAssessmentPauseReason(t *testing.T) {
	cases := []struct {
		name string
		a    scheduledContextAssessment
		want string
	}{
		{
			name: "ready rows have no reason",
			a:    scheduledContextAssessment{ready: true, fields: &capacity.SlingContextFields{}},
			want: "",
		},
		{
			name: "circuit-broken context",
			a:    scheduledContextAssessment{fields: &capacity.SlingContextFields{DispatchFailures: maxDispatchFailures}},
			want: "respawn-limit",
		},
		{
			name: "blocked names its blockers",
			a: scheduledContextAssessment{
				fields:   &capacity.SlingContextFields{},
				blocked:  true,
				blockers: []string{"bt-a", "bt-wisp-b"},
			},
			want: "blocked-by bt-a,bt-wisp-b",
		},
		{
			name: "blocked with no blocker list",
			a:    scheduledContextAssessment{fields: &capacity.SlingContextFields{}, blocked: true},
			want: "blocked-by <unknown>",
		},
		{
			name: "unknown blocked state",
			a:    scheduledContextAssessment{fields: &capacity.SlingContextFields{}, blockedUnknown: true},
			want: "blocked-unknown (bd blocked query failed)",
		},
		{
			name: "missing work bead",
			a:    scheduledContextAssessment{fields: &capacity.SlingContextFields{}},
			want: "work bead not found",
		},
		{
			name: "status and assignee",
			a: scheduledContextAssessment{
				fields: &capacity.SlingContextFields{},
				found:  true,
				info:   beadStatusInfo{Status: "in_progress", Assignee: "bt/polecats/nux"},
			},
			want: "status=in_progress assignee=bt/polecats/nux",
		},
		{
			name: "status with no assignee",
			a: scheduledContextAssessment{
				fields: &capacity.SlingContextFields{},
				found:  true,
				info:   beadStatusInfo{Status: "pinned"},
			},
			want: "status=pinned assignee=<none>",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.a.pauseReason(); got != tc.want {
				t.Fatalf("pauseReason() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestSortScheduledContextAssessments(t *testing.T) {
	mk := func(ctxID string, priority int, enqueued string) scheduledContextAssessment {
		return scheduledContextAssessment{
			context: slingContextRecord{issue: &beads.Issue{ID: ctxID}},
			fields:  &capacity.SlingContextFields{EnqueuedAt: enqueued},
			info:    beadStatusInfo{Priority: priority},
		}
	}

	assessments := []scheduledContextAssessment{
		mk("ctx-3", 3, "2026-01-01T00:00:00Z"),
		mk("ctx-0", 0, "2026-06-01T00:00:00Z"),
		mk("ctx-2b", 2, "2026-02-01T00:00:00Z"),
		mk("ctx-2a", 2, "2026-01-01T00:00:00Z"),
	}
	sortScheduledContextAssessments(assessments)

	want := []string{"ctx-0", "ctx-2a", "ctx-2b", "ctx-3"}
	for i, w := range want {
		if got := assessments[i].context.issue.ID; got != w {
			t.Fatalf("position %d = %s, want %s", i, got, w)
		}
	}
}

func TestScheduledWorkBeadReady_AwaitingMergeHoldIsNotReady(t *testing.T) {
	info := beadStatusInfo{Status: "in_progress", Assignee: "", Labels: []string{"awaiting-merge:bt-wisp-abc12"}}
	if isScheduledWorkBeadReady("bt-1", info, true, nil, nil) {
		t.Fatal("a bead held for merge (in_progress, unassigned, awaiting-merge:) must not be re-dispatched")
	}
}

func TestSortScheduledRecoveryFinishesWorkBeforeFreshBeads(t *testing.T) {
	row := func(id string, priority int, date string, mr bool, dependency bool) scheduledContextAssessment {
		fields := &capacity.SlingContextFields{TargetRig: "hisn", EnqueuedAt: date, ResumeDependency: dependency}
		if mr {
			fields.ResumeMR = "hisn-wisp-existing"
		}
		return scheduledContextAssessment{context: slingContextRecord{issue: &beads.Issue{ID: id}}, fields: fields, info: beadStatusInfo{Priority: priority}}
	}
	rows := []scheduledContextAssessment{
		row("fresh-old", 2, "2026-09-19T05:00:00Z", false, false),
		row("dependency", 2, "2026-09-20T05:00:00Z", false, true),
		row("mr-newer", 2, "2026-09-20T07:00:00Z", true, false),
		row("urgent", 1, "2026-09-20T08:00:00Z", false, false),
		row("mr-older", 2, "2026-09-20T06:00:00Z", true, false),
		row("low-priority-mr", 3, "2026-09-19T04:00:00Z", true, false),
	}
	sortScheduledContextAssessments(rows)
	want := []string{"urgent", "mr-older", "mr-newer", "dependency", "fresh-old", "low-priority-mr"}
	for i, id := range want {
		if rows[i].context.issue.ID != id {
			t.Fatalf("position %d = %s, want %s", i, rows[i].context.issue.ID, id)
		}
	}
}

func TestPreservedPrerequisitesPrecedeFreshWorkWithoutOverridingPriority(t *testing.T) {
	blockers := map[string][]string{"hisn-product": {"hisn-mid", "hisn-ready"}, "hisn-mid": {"hisn-ready", "hisn-product"}, "hisn-held": {"hisn-other"}}
	sources := map[string]beadStatusInfo{"hisn-product": {Status: "blocked", Assignee: "hisn/polecats/obsidian"}, "hisn-held": {Status: "blocked", Assignee: "hisn/polecats/shale", Labels: []string{"needs-operator"}}}
	counts := preservedPrerequisiteCounts(blockers, sources)
	if counts["hisn-ready"] != 1 || counts["hisn-product"] != 0 || counts["hisn-other"] != 0 {
		t.Fatalf("incorrect bounded dependency ranking: %+v", counts)
	}
	rows := []scheduledContextAssessment{}
	for _, id := range []string{"fresh", "hisn-ready", "urgent"} {
		priority := 2
		if id == "urgent" {
			priority = 1
		}
		rows = append(rows, scheduledContextAssessment{context: slingContextRecord{issue: &beads.Issue{ID: id}}, fields: &capacity.SlingContextFields{TargetRig: "hisn", WorkBeadID: id}, info: beadStatusInfo{Priority: priority}, unblocksPreserved: counts[id]})
	}
	sortScheduledContextAssessments(rows)
	for i, want := range []string{"urgent", "hisn-ready", "fresh"} {
		if rows[i].context.issue.ID != want {
			t.Fatalf("wrong order at %d: %+v", i, rows)
		}
	}
}
