package session

import (
	"strings"
	"testing"
)

func TestDeaconStartupInstructions_UseEnsurePatrol(t *testing.T) {
	if !strings.Contains(DeaconStartupInstructions, "gt deacon ensure-patrol") {
		t.Fatalf("DeaconStartupInstructions should use gt deacon ensure-patrol, got %q", DeaconStartupInstructions)
	}
	if strings.Contains(DeaconStartupInstructions, "gt sling mol-deacon-patrol deacon") {
		t.Fatalf("DeaconStartupInstructions should not use raw patrol self-sling, got %q", DeaconStartupInstructions)
	}
}
