package cmd

import (
	"testing"

	"github.com/steveyegge/gastown/internal/scheduler/capacity"
)

func TestFilterDispatchPlanRecoveryOnlyRefillsFromBackfill(t *testing.T) {
	fresh := func(id string) capacity.PendingBead {
		return capacity.PendingBead{ID: id, Context: &capacity.SlingContextFields{}}
	}
	recov := func(id string) capacity.PendingBead {
		return capacity.PendingBead{ID: id, Context: &capacity.SlingContextFields{ResumeMR: "hisn-wisp-" + id}}
	}
	plan := capacity.DispatchPlan{
		ToDispatch: []capacity.PendingBead{fresh("a"), fresh("b")},
		Backfill:   []capacity.PendingBead{fresh("c"), recov("r1"), fresh("d"), recov("r2"), recov("r3")},
	}
	filterDispatchPlan(&plan, recoveryOnlyFilter)
	if len(plan.ToDispatch) != 2 || plan.ToDispatch[0].ID != "r1" || plan.ToDispatch[1].ID != "r2" {
		t.Fatalf("ToDispatch = %+v, want r1,r2", plan.ToDispatch)
	}
	if len(plan.Backfill) != 1 || plan.Backfill[0].ID != "r3" {
		t.Fatalf("Backfill = %+v, want r3", plan.Backfill)
	}
}

func TestFilterDispatchPlanNilKeepsPlan(t *testing.T) {
	plan := capacity.DispatchPlan{ToDispatch: []capacity.PendingBead{{ID: "a"}}, Backfill: []capacity.PendingBead{{ID: "b"}}}
	filterDispatchPlan(&plan, nil)
	if len(plan.ToDispatch) != 1 || len(plan.Backfill) != 1 {
		t.Fatalf("nil filter changed plan: %+v", plan)
	}
}

func TestRecoveryOnlyFilterAdmitsLandingUnblock(t *testing.T) {
	fresh := capacity.PendingBead{ID: "a", Context: &capacity.SlingContextFields{}}
	unblock := capacity.PendingBead{ID: "b", Context: &capacity.SlingContextFields{}, Labels: []string{"m1d", "landing-unblock"}}
	other := capacity.PendingBead{ID: "c", Context: &capacity.SlingContextFields{}, Labels: []string{"m1d"}}
	if recoveryOnlyFilter(fresh) || recoveryOnlyFilter(other) {
		t.Fatal("ordinary new work must stay deferred under pressure")
	}
	if !recoveryOnlyFilter(unblock) {
		t.Fatal("landing-unblock work must be admitted under pressure")
	}
}
