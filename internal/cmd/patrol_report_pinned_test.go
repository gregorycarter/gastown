package cmd

import (
	"strings"
	"testing"
)

func TestPinnedPatrolAuditUsesCookedSteps(t *testing.T) {
	ids, err := cookedPatrolStepIDs([]byte(`{"formula":"mol-refinery-patrol","steps":[{"id":"inbox-check"},{"id":"queue"},{"id":"process"},{"id":"report"},{"id":"context"},{"id":"loop"}]}`), "mol-refinery-patrol")
	if err != nil {
		t.Fatal(err)
	}
	if got := formatStepAudit(ids, ""); got != "Steps: NOT REPORTED (?/6)" {
		t.Fatal(got)
	}
	if got := formatStepAudit(ids, "queue:OK,process:OK"); !strings.HasSuffix(got, "(2/6)") || !strings.Contains(got, "loop SKIP") {
		t.Fatal(got)
	}
}

func TestPinnedPatrolAuditRejectsWrongMissingAndPartialContracts(t *testing.T) {
	for _, data := range []string{
		`{broken`, `{}`, `{"formula":"foreign","steps":[{"id":"x"}]}`,
		`{"formula":"mol-test","steps":[{}]}`,
		`{"formula":"mol-test","steps":[{"id":"x"},{"id":"x"}]}`,
		`{"formula":"mol-test","steps":[{"id":"x","children":[{"id":"y"}]}]}`,
	} {
		if _, err := cookedPatrolStepIDs([]byte(data), "mol-test"); err == nil {
			t.Fatalf("accepted %s", data)
		}
	}
}
