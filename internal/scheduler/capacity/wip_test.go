package capacity

import (
	"encoding/json"
	"strings"
	"testing"
)

func wipTestCap(pending, limit int) WIPCap {
	return WIPCap{
		PendingMRs:   map[string]int{"hisn": pending},
		Limit:        func(string) int { return limit },
		ExemptLabels: DefaultWIPCapExemptLabels,
	}
}

func newWork(id string) PendingBead {
	return PendingBead{ID: "ctx-" + id, WorkBeadID: id, TargetRig: "hisn",
		Context: &SlingContextFields{WorkBeadID: id, TargetRig: "hisn"}, Priority: 2, HasPriority: true}
}

func TestWIPCapUnderLimitDispatches(t *testing.T) {
	plan, deferred := PlanDispatchWithWIPCap(4, 2, []PendingBead{newWork("hisn-a"), newWork("hisn-b")}, wipTestCap(7, 8))
	if len(deferred) != 0 || len(plan.ToDispatch) != 2 {
		t.Fatalf("under the cap all new work must dispatch: plan=%+v deferred=%+v", plan, deferred)
	}
	if strings.Contains(plan.Reason, "wip-cap") {
		t.Fatalf("reason must not mention wip-cap under the limit: %q", plan.Reason)
	}
}

func TestWIPCapAtAndOverLimitDefersNewWork(t *testing.T) {
	for _, pending := range []int{8, 19} {
		plan, deferred := PlanDispatchWithWIPCap(4, 2, []PendingBead{newWork("hisn-a"), newWork("hisn-b")}, wipTestCap(pending, 8))
		if len(plan.ToDispatch) != 0 || len(deferred) != 2 {
			t.Fatalf("pending=%d: new work must be deferred: plan=%+v deferred=%+v", pending, plan, deferred)
		}
		if plan.Reason != "wip-cap" || plan.Skipped != 2 {
			t.Fatalf("pending=%d: want reason wip-cap skipped 2, got %+v", pending, plan)
		}
		want := WIPCapReason(pending, 8)
		if deferred[0].Reason != want || deferred[0].Rig != "hisn" {
			t.Fatalf("pending=%d: deferral = %+v, want reason %q", pending, deferred[0], want)
		}
	}
	if got := WIPCapReason(19, 8); got != "wip-cap: 19 pending MRs >= 8" {
		t.Fatalf("reason format = %q", got)
	}
}

func TestWIPCapExemptionsStillDispatch(t *testing.T) {
	mrRecovery := newWork("hisn-mr")
	mrRecovery.Context.ResumeMR = "hisn-wisp-mr1"
	depRecovery := newWork("hisn-dep")
	depRecovery.Context.ResumeDependency = true
	p0 := newWork("hisn-p0")
	p0.Priority = 0
	remediation := newWork("hisn-rem")
	remediation.Labels = []string{"release-remediation"}
	trainFailure := newWork("hisn-ci")
	trainFailure.Labels = []string{"bug", "ci-train-failure"}
	fresh := newWork("hisn-new")
	unknownPriority := newWork("hisn-unknown")
	unknownPriority.Priority, unknownPriority.HasPriority = 0, false

	ready := []PendingBead{mrRecovery, depRecovery, p0, remediation, trainFailure, fresh, unknownPriority}
	plan, deferred := PlanDispatchWithWIPCap(10, 10, ready, wipTestCap(19, 8))
	var got []string
	for _, b := range plan.ToDispatch {
		got = append(got, b.WorkBeadID)
	}
	want := "hisn-mr,hisn-dep,hisn-p0,hisn-rem,hisn-ci"
	if strings.Join(got, ",") != want {
		t.Fatalf("dispatched %v, want %s", got, want)
	}
	if len(deferred) != 2 || deferred[0].Bead.WorkBeadID != "hisn-new" || deferred[1].Bead.WorkBeadID != "hisn-unknown" {
		t.Fatalf("deferred = %+v, want fresh and unknown-priority work", deferred)
	}
	if !strings.HasSuffix(plan.Reason, "+wip-cap") || plan.Skipped != 2 {
		t.Fatalf("partial deferral must be reported: %+v", plan)
	}
}

func TestWIPCapLimitZeroDisables(t *testing.T) {
	plan, deferred := PlanDispatchWithWIPCap(4, 4, []PendingBead{newWork("hisn-a")}, wipTestCap(50, 0))
	if len(deferred) != 0 || len(plan.ToDispatch) != 1 {
		t.Fatalf("limit 0 must disable the cap: plan=%+v deferred=%+v", plan, deferred)
	}
}

func TestWIPCapIsPerRig(t *testing.T) {
	other := newWork("gt-a")
	other.TargetRig = "gastown"
	plan, deferred := PlanDispatchWithWIPCap(4, 4, []PendingBead{newWork("hisn-a"), other}, wipTestCap(19, 8))
	if len(plan.ToDispatch) != 1 || plan.ToDispatch[0].WorkBeadID != "gt-a" || len(deferred) != 1 {
		t.Fatalf("a capped rig must not hold other rigs: plan=%+v deferred=%+v", plan, deferred)
	}
}

func TestGetMaxPendingMRs(t *testing.T) {
	var nilCfg *SchedulerConfig
	if got := nilCfg.GetMaxPendingMRs("hisn"); got != 8 {
		t.Fatalf("default = %d, want 8", got)
	}
	var cfg SchedulerConfig
	if err := json.Unmarshal([]byte(`{"max_pending_mrs": 5, "max_pending_mrs_by_rig": {"hisn": 12, "quiet": 0}}`), &cfg); err != nil {
		t.Fatal(err)
	}
	if got := cfg.GetMaxPendingMRs("gastown"); got != 5 {
		t.Fatalf("town value = %d, want 5", got)
	}
	if got := cfg.GetMaxPendingMRs("hisn"); got != 12 {
		t.Fatalf("rig override = %d, want 12", got)
	}
	if got := cfg.GetMaxPendingMRs("quiet"); got != 0 {
		t.Fatalf("rig override 0 = %d, want 0 (disabled)", got)
	}
	neg := -3
	if got := (&SchedulerConfig{MaxPendingMRs: &neg}).GetMaxPendingMRs("x"); got != 0 {
		t.Fatalf("negative clamps to 0, got %d", got)
	}
	if got := nilCfg.GetWIPCapExemptLabels(); strings.Join(got, ",") != "release-remediation,ci-train-failure" {
		t.Fatalf("default exempt labels = %v", got)
	}
	if got := (&SchedulerConfig{WIPCapExemptLabels: []string{}}).GetWIPCapExemptLabels(); len(got) != 0 {
		t.Fatalf("explicit empty exempt list must stay empty: %v", got)
	}
}
