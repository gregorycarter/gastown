package nudge

import (
	"strings"
	"testing"
)

func TestFormatForInjectionWithFooter(t *testing.T) {
	nudges := []QueuedNudge{{Sender: "deacon", Message: "HEALTH_CHECK"}}

	plain := FormatForInjectionWithFooter(nudges, "")
	if strings.Contains(plain, PatrolReentryFooter) {
		t.Error("empty footer must not be rendered")
	}
	if !strings.HasSuffix(plain, "</system-reminder>\n") {
		t.Errorf("block must end with the closing tag, got %q", plain)
	}

	withFooter := FormatForInjectionWithFooter(nudges, PatrolReentryFooter)
	if !strings.Contains(withFooter, PatrolReentryFooter) {
		t.Error("footer missing from output")
	}
	footerIdx := strings.Index(withFooter, PatrolReentryFooter)
	closeIdx := strings.Index(withFooter, "</system-reminder>")
	if footerIdx > closeIdx {
		t.Error("footer must be inside the system-reminder block")
	}
}

func TestFormatForInjectionEmpty(t *testing.T) {
	if got := FormatForInjectionWithFooter(nil, PatrolReentryFooter); got != "" {
		t.Errorf("no nudges must produce empty output, got %q", got)
	}
}

func TestFormatForInjectionUnchangedForNonPatrol(t *testing.T) {
	nudges := []QueuedNudge{{Sender: "mayor", Message: "status?"}}
	if FormatForInjection(nudges) != FormatForInjectionWithFooter(nudges, "") {
		t.Error("FormatForInjection must equal the no-footer form")
	}
}

func TestIsPatrolSession(t *testing.T) {
	tests := []struct {
		session string
		want    bool
	}{
		{"gt-witness", true},
		{"gt-refinery", true},
		{"hq-deacon", true},
		{"hq-boot", false},
		{"hq-mayor", false},
		{"gt-furiosa", false},
		{"gt-crew-max", false},
		{"", false},
		{"not a session name", false},
	}
	for _, tc := range tests {
		if got := IsPatrolSession(tc.session); got != tc.want {
			t.Errorf("IsPatrolSession(%q) = %v, want %v", tc.session, got, tc.want)
		}
	}
}

func TestFormatForInjectionForSession(t *testing.T) {
	nudges := []QueuedNudge{{Sender: "daemon", Message: "PARKED"}}
	if !strings.Contains(FormatForInjectionForSession(nudges, "gt-witness"), PatrolReentryFooter) {
		t.Error("patrol session should get the footer")
	}
	if strings.Contains(FormatForInjectionForSession(nudges, "hq-mayor"), PatrolReentryFooter) {
		t.Error("mayor session must not get the footer")
	}
}
