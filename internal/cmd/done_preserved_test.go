package cmd

import "testing"

func TestBlockedDoneKeepsSourceAndReleasesOnlyHisnWork(t *testing.T) {
	for _, exit := range []string{ExitDeferred, ExitEscalated} {
		if !shouldCheckpointBlockedDone("hisn", exit, "hisn-product") {
			t.Fatal("blocked Hisn work not checkpointed")
		}
		if shouldFinishDoneSource(exit, "hisn-product") {
			t.Fatal("blocked source would be closed")
		}
		for _, id := range []string{"", "hisn-wisp-abc", "hisn-wfs-abc", "bt-product"} {
			if shouldCheckpointBlockedDone("hisn", exit, id) {
				t.Fatal("invalid checkpoint source", id)
			}
		}
		if shouldCheckpointBlockedDone("bridge_town_core", exit, "bt-product") {
			t.Fatal("parked rig scope widened")
		}
	}
	if !shouldFinishDoneSource(ExitCompleted, "hisn-product") || !shouldFinishDoneSource(ExitDeferred, "hisn-wfs-step") {
		t.Fatal("successful/workflow completion regressed")
	}
}
