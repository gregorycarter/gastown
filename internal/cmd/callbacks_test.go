package cmd

import (
	"testing"

	"github.com/steveyegge/gastown/internal/mail"
)

func TestClassifyCallback(t *testing.T) {
	tests := []struct {
		subject string
		want    CallbackType
	}{
		{"POLECAT_DONE chrome", CallbackPolecatDone},
		{"Merge Request Rejected: polecat/chrome/gt-abc", CallbackMergeRejected},
		{"Merge Request Completed: polecat/chrome/gt-abc", CallbackMergeCompleted},
		{"HELP: stuck on build", CallbackHelp},
		{"ESCALATION: Dolt connection refused", CallbackEscalation},
		{"SLING_REQUEST: gt-abc123", CallbackSling},
		{"DOG_DONE alpha", CallbackDogDone},
		{"CONVOY_NEEDS_FEEDING hq-cv123", CallbackConvoyNeedsFeeding},
		{"RECOVERED_BEAD gt-wfs-abc", CallbackRecoveredBead},
		{"SPAWN_STORM RECOVERED_BEAD gt-wfs-abc", CallbackSpawnStorm},
		{"Deacon stuck_heartbeat_1565s detected by stuck-agent-dog [escalation] !", CallbackEscalation},
		{"[HIGH] Deacon stuck_heartbeat_1548s detected by stuck-agent-dog", CallbackEscalation},
		{"Refinery MR rebase failure: bt-hfq claims conflicts [escalation] !", CallbackEscalation},
		{"POLECAT_DIED: 2 polecat(s) died with active work in gastown_rig", CallbackPolecatDied},
		{"Wisp Compaction: 2026-05-01", CallbackWispCompaction},
		{"Weekly Wisp Compaction: 2026-04-28 to 2026-05-01", CallbackWispCompaction},
		{"random unknown message", CallbackUnknown},
		{"", CallbackUnknown},
	}

	for _, tt := range tests {
		t.Run(tt.subject, func(t *testing.T) {
			got := classifyCallback(tt.subject)
			if got != tt.want {
				t.Errorf("classifyCallback(%q) = %q, want %q", tt.subject, got, tt.want)
			}
		})
	}
}

func TestProcessCallback_DogDone(t *testing.T) {
	msg := &mail.Message{
		ID:      "bd-dog1",
		From:    "deacon/dogs/alpha",
		Subject: "DOG_DONE alpha",
		Body:    "Task: orphan-scan\nStatus: success\n",
	}

	result := processCallback("/tmp/fake-town", msg, true)
	if result.CallbackType != CallbackDogDone {
		t.Errorf("expected CallbackDogDone, got %q", result.CallbackType)
	}
	if result.Error != nil {
		t.Errorf("unexpected error: %v", result.Error)
	}
	if !result.Handled {
		t.Error("expected handled")
	}
	if result.Action == "" {
		t.Error("expected action description")
	}
}

func TestProcessCallback_ConvoyNeedsFeeding(t *testing.T) {
	msg := &mail.Message{
		ID:      "bd-cv1",
		From:    "gastown/refinery",
		Subject: "CONVOY_NEEDS_FEEDING hq-cv123",
		Body:    "ConvoyID: hq-cv123\nSourceIssue: hq-abc\nRig: gastown\n",
	}

	result := processCallback("/tmp/fake-town", msg, true)
	if result.CallbackType != CallbackConvoyNeedsFeeding {
		t.Errorf("expected CallbackConvoyNeedsFeeding, got %q", result.CallbackType)
	}
	if result.Error != nil {
		t.Errorf("unexpected error: %v", result.Error)
	}
	if !result.Handled {
		t.Error("expected handled")
	}
}

func TestProcessCallback_RecoveredBead(t *testing.T) {
	msg := &mail.Message{
		ID:      "bd-rec1",
		From:    "gastown/witness",
		Subject: "RECOVERED_BEAD gt-abc123",
		Body:    "Bead: gt-abc123\nPolecat: gastown/polecats/chrome\n",
	}

	result := processCallback("/tmp/fake-town", msg, true)
	if result.CallbackType != CallbackRecoveredBead {
		t.Errorf("expected CallbackRecoveredBead, got %q", result.CallbackType)
	}
	if result.Error != nil {
		t.Errorf("unexpected error: %v", result.Error)
	}
	if !result.Handled {
		t.Error("expected handled")
	}
}

func TestProcessCallback_SpawnStorm(t *testing.T) {
	msg := &mail.Message{
		ID:      "bd-storm1",
		From:    "gastown/witness",
		Subject: "SPAWN_STORM RECOVERED_BEAD gt-abc123 (respawned 5x)",
		Body:    "Bead: gt-abc123\nRespawn Count: 5\n",
	}

	result := processCallback("/tmp/fake-town", msg, true)
	if result.CallbackType != CallbackSpawnStorm {
		t.Errorf("expected CallbackSpawnStorm, got %q", result.CallbackType)
	}
	if result.Error != nil {
		t.Errorf("unexpected error: %v", result.Error)
	}
	if !result.Handled {
		t.Error("expected handled")
	}
}

func TestProcessCallback_EscalationSuffix(t *testing.T) {
	msg := &mail.Message{
		ID:      "bd-esc1",
		From:    "overseer",
		Subject: "Deacon stuck_heartbeat_1565s detected by stuck-agent-dog [escalation] !",
		Body:    "Agent deacon has not updated heartbeat in 1565s",
	}

	result := processCallback("/tmp/fake-town", msg, true)
	if result.CallbackType != CallbackEscalation {
		t.Errorf("expected CallbackEscalation, got %q", result.CallbackType)
	}
	if result.Error != nil {
		t.Errorf("unexpected error: %v", result.Error)
	}
	if !result.Handled {
		t.Error("expected handled")
	}
	// In dry-run mode, action should indicate archiving without overseer spam
	if result.Action == "" {
		t.Error("expected action description")
	}
}

func TestProcessCallback_StuckHeartbeatNoSuffix(t *testing.T) {
	msg := &mail.Message{
		ID:      "bd-esc2",
		From:    "overseer",
		Subject: "[HIGH] Deacon stuck_heartbeat_1548s detected by stuck-agent-dog",
		Body:    "Agent deacon has not updated heartbeat in 1548s",
	}

	result := processCallback("/tmp/fake-town", msg, true)
	if result.CallbackType != CallbackEscalation {
		t.Errorf("expected CallbackEscalation, got %q", result.CallbackType)
	}
	if result.Error != nil {
		t.Errorf("unexpected error: %v", result.Error)
	}
	if !result.Handled {
		t.Error("expected handled")
	}
	if result.Action == "" {
		t.Error("expected action description")
	}
}

func TestProcessCallback_PolecatDied(t *testing.T) {
	msg := &mail.Message{
		ID:      "bd-died1",
		From:    "gastown/witness",
		Subject: "POLECAT_DIED: 2 polecat(s) died with active work in gastown_rig",
		Body:    "- rust: session-dead-active (hook=gt-wfs-abc, action=restarted)\n- dust: session-dead-active (hook=gt-wfs-def, action=restarted)\n",
	}

	result := processCallback("/tmp/fake-town", msg, true)
	if result.CallbackType != CallbackPolecatDied {
		t.Errorf("expected CallbackPolecatDied, got %q", result.CallbackType)
	}
	if result.Error != nil {
		t.Errorf("unexpected error: %v", result.Error)
	}
	if !result.Handled {
		t.Error("expected handled")
	}
	if result.Action == "" {
		t.Error("expected action description")
	}
}

func TestProcessCallback_WispCompaction(t *testing.T) {
	msg := &mail.Message{
		ID:      "bd-compact1",
		From:    "gastown/thunder",
		Subject: "Wisp Compaction: 2026-05-01",
		Body:    "Summary\n| Category | Deleted | Promoted | Active |\n",
	}

	result := processCallback("/tmp/fake-town", msg, true)
	if result.CallbackType != CallbackWispCompaction {
		t.Errorf("expected CallbackWispCompaction, got %q", result.CallbackType)
	}
	if result.Error != nil {
		t.Errorf("unexpected error: %v", result.Error)
	}
	if !result.Handled {
		t.Error("expected handled")
	}
	if result.Action == "" {
		t.Error("expected action description")
	}
}

func TestProcessCallback_Unknown(t *testing.T) {
	msg := &mail.Message{
		ID:      "bd-unk1",
		From:    "someone",
		Subject: "Hello world",
		Body:    "Just saying hi",
	}

	result := processCallback("/tmp/fake-town", msg, true)
	if result.CallbackType != CallbackUnknown {
		t.Errorf("expected CallbackUnknown, got %q", result.CallbackType)
	}
	if result.Handled {
		t.Error("expected not handled")
	}
}
