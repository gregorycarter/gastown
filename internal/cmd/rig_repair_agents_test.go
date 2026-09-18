package cmd

import (
	"fmt"
	"github.com/steveyegge/gastown/internal/beads"
	"io"
	"testing"
)

type fakeRigIdentityStore struct {
	issues   map[string]*beads.Issue
	creates  int
	fail     bool
	readFail bool
}

func (f *fakeRigIdentityStore) GetAgentBead(id string) (*beads.Issue, *beads.AgentFields, error) {
	if f.readFail {
		return nil, nil, fmt.Errorf("DB unavailable")
	}
	issue, ok := f.issues[id]
	if !ok {
		return nil, nil, nil
	}
	return issue, beads.ParseAgentFields(issue.Description), nil
}

func TestRepairRigLifecycleIdentitiesReadFailureDoesNotCreate(t *testing.T) {
	db := &fakeRigIdentityStore{issues: map[string]*beads.Issue{}, readFail: true}
	if err := repairRigLifecycleIdentities(db, "hisn", "hisn", io.Discard); err == nil {
		t.Fatal("read failure accepted")
	}
	if db.creates != 0 {
		t.Fatal("created after failed lookup")
	}
}
func (f *fakeRigIdentityStore) CreateAgentBead(id, title string, fields *beads.AgentFields) (*beads.Issue, error) {
	if f.fail {
		return nil, fmt.Errorf("DB unavailable")
	}
	f.creates++
	issue := &beads.Issue{ID: id, Status: "open", Description: beads.FormatAgentDescription(title, fields)}
	f.issues[id] = issue
	return issue, nil
}
func TestRepairRigLifecycleIdentitiesScopedIdempotent(t *testing.T) {
	db := &fakeRigIdentityStore{issues: map[string]*beads.Issue{"bt-refinery": {ID: "bt-refinery", Status: "open", Description: "untouched"}}}
	if err := repairRigLifecycleIdentities(db, "hisn", "hisn", io.Discard); err != nil {
		t.Fatal(err)
	}
	if db.creates != 2 {
		t.Fatalf("created %d", db.creates)
	}
	if err := repairRigLifecycleIdentities(db, "hisn", "hisn", io.Discard); err != nil {
		t.Fatal(err)
	}
	if db.creates != 2 || db.issues["bt-refinery"].Description != "untouched" {
		t.Fatal("idempotence/other rig changed")
	}
	db.issues["hisn-witness"].Status = "closed"
	if err := repairRigLifecycleIdentities(db, "hisn", "hisn", io.Discard); err == nil {
		t.Fatal("closed role reopened")
	}
}
func TestRepairRigLifecycleIdentitiesRefusesWrongRoleOrFailedDB(t *testing.T) {
	db := &fakeRigIdentityStore{issues: map[string]*beads.Issue{}, fail: true}
	if err := repairRigLifecycleIdentities(db, "hisn", "hisn", io.Discard); err == nil {
		t.Fatal("failed DB accepted")
	}
	db.fail = false
	db.issues["hisn-witness"] = &beads.Issue{ID: "hisn-witness", Status: "open", Description: "role_type: refinery\nrig: bridge_town_core"}
	if err := repairRigLifecycleIdentities(db, "hisn", "hisn", io.Discard); err == nil {
		t.Fatal("wrong role overwritten")
	}
	if db.creates != 0 {
		t.Fatal("mutated after refusing identity")
	}
}
