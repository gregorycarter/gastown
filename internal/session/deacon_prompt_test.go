package session

import (
	"strings"
	"testing"
)

func TestDeaconStartupInstructions_UseEnsurePatrolAndPrime(t *testing.T) {
	if !strings.Contains(DeaconStartupInstructions, "gt deacon ensure-patrol") {
		t.Fatalf("DeaconStartupInstructions should use gt deacon ensure-patrol, got %q", DeaconStartupInstructions)
	}
	if !strings.Contains(DeaconStartupInstructions, "gt prime --hook") {
		t.Fatalf("DeaconStartupInstructions should re-prime hook context after ensure-patrol, got %q", DeaconStartupInstructions)
	}
	if strings.Contains(DeaconStartupInstructions, "gt sling mol-deacon-patrol deacon") {
		t.Fatalf("DeaconStartupInstructions should not use raw patrol self-sling, got %q", DeaconStartupInstructions)
	}
}
