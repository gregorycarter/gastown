package mail

import "testing"

// Subjects taken from the bt-ahq0 incident (2026-08-27/29) plus the receipt the
// Witness sent by hand on 2026-09-01 while working around the loop.
func TestIsReplyReminderExempt(t *testing.T) {
	exempt := []string{
		"MERGED toast",
		"RE: MERGED toast",
		"MERGE_FAILED bt-hgno.3",
		"MERGE_READY bt-j6uu.8",
		"ACK MERGED morsov HOLD PRESERVED",
		"ACK ACKNOWLEDGED RETRY RECEIPT",
		"RECEIPT ONLY: FINAL ACK nux",
		"POLECAT_DONE glory",
		"HEALTH_OK refinery responsive",
		"lifecycle: session recycled",
		"FYI train 33421170384 landed",
		"no action required: queue drained",
	}
	for _, subject := range exempt {
		if !isReplyReminderExempt(&Message{Subject: subject}) {
			t.Errorf("subject %q should be exempt from reply reminders", subject)
		}
	}

	// Real requests must still earn a reminder, or the control is useless.
	notExempt := []string{
		"Please rebase bt-hgno.12 onto main",
		"FIX_NEEDED: lint failing on your branch",
		"Question about the merge gate tiers",
		"HELP: polecat stuck on conflict",
		"Escalation: scheduler stalled",
	}
	for _, subject := range notExempt {
		if isReplyReminderExempt(&Message{Subject: subject}) {
			t.Errorf("subject %q must NOT be exempt — it asks for something", subject)
		}
	}

	if !isReplyReminderExempt(nil) {
		t.Error("nil message should be treated as exempt rather than panicking")
	}
}
