package cmd

import (
	"encoding/json"
	"github.com/steveyegge/gastown/internal/beads"
	"strings"
	"testing"
)

func TestKasbSameMRRepairKeepsBindingAndHolds(t *testing.T) {
	for _, scenario := range []string{"ready", "missing-reason", "malformed-reason", "ambiguous-kind", "review-held-mr", "operator-held-mr", "rollout-held-source", "dispatch-held-source", "closed-source", "reassigned-source"} {
		t.Run(scenario, func(t *testing.T) {
			mr, source, show := kasbRecoveryFixture()
			mr.Labels = []string{"gt:merge-request", "needs-repair"}
			reason := "Remove page-mount mutation; preserve explicit authorized intent"
			mr.Metadata, _ = json.Marshal(map[string]any{"correction_reason": reason})
			switch scenario {
			case "missing-reason":
				mr.Metadata = nil
			case "malformed-reason":
				mr.Metadata = json.RawMessage(`{"correction_reason":{}}`)
			case "ambiguous-kind":
				mr.Labels = append(mr.Labels, "needs-rebase")
			case "review-held-mr":
				mr.Labels = append(mr.Labels, "needs-review")
			case "operator-held-mr":
				mr.Labels = append(mr.Labels, "needs-operator")
			case "rollout-held-source":
				source.Labels = append(source.Labels, "needs-operator-rollout")
			case "dispatch-held-source":
				source.Labels = append(source.Labels, "dispatch:hold")
			case "closed-source":
				source.Status = "closed"
			case "reassigned-source":
				source.Assignee = "kasb_core/polecats/other"
			}
			state, err := kasbRecoveryCandidate(mr, strings.Repeat("b", 40), show)
			if (err == nil) != (scenario == "ready") {
				t.Fatalf("state=%v err=%v", state, err)
			}
			if err != nil {
				return
			}
			if state.Record.Kind != "same-mr-repair" || state.Record.MR != mr.ID || state.Record.Branch != beads.ParseMRFields(mr).Branch || !strings.Contains(state.Record.Instructions, reason) {
				t.Fatalf("lost exact repair binding: %+v", state.Record)
			}
			// Changing the correction after queuing must invalidate the old instructions.
			var metadata map[string]json.RawMessage
			_ = json.Unmarshal(state.MR.Metadata, &metadata)
			metadata["correction_reason"] = json.RawMessage(`"different correction"`)
			state.MR.Metadata, _ = json.Marshal(metadata)
			_, err = validateMQResumeRecords("kasb_core", mr.ID, func(id string) (*beads.Issue, error) {
				if id == mr.ID {
					return state.MR, nil
				}
				return show(id)
			})
			if err == nil {
				t.Fatal("stale correction receipt accepted")
			}
		})
	}
}

func TestHisnDoesNotOptIntoKasbRepair(t *testing.T) {
	issues, show := mqResumeFixture(t)
	mr := issues["hisn-wisp-test"]
	mr.Labels = []string{"gt:merge-request", "needs-repair"}
	if _, err := validateMQResumeRecords("hisn", mr.ID, show); err == nil {
		t.Fatal("Kasb repair changed Hisn admission")
	}
}
