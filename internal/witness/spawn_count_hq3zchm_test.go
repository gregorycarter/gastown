package witness

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/steveyegge/gastown/internal/config"
)

func respawnTown(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "witness"), 0o755); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestRefundBeadRespawn_UndoesAttemptWithoutSession(t *testing.T) {
	town := respawnTown(t)
	for i := 0; i < config.DefaultWitnessMaxBeadRespawns; i++ {
		RecordBeadRespawn(town, "bead-r")
	}
	if !ShouldBlockRespawn(town, "bead-r") {
		t.Fatal("expected the breaker to trip at the limit")
	}
	RefundBeadRespawn(town, "bead-r")
	if ShouldBlockRespawn(town, "bead-r") {
		t.Fatal("a refunded attempt still counts toward the breaker")
	}
	for i := 0; i < config.DefaultWitnessMaxBeadRespawns; i++ {
		RefundBeadRespawn(town, "bead-r")
	}
	if got := RecordBeadRespawn(town, "bead-r"); got != 1 {
		t.Fatalf("count after refunds below zero = %d, want 1", got)
	}
	RefundBeadRespawn(town, "never-recorded") // must not panic or create a record
}

func TestRespawnCountExpiresAfterWindow(t *testing.T) {
	town := respawnTown(t)
	for i := 0; i < config.DefaultWitnessMaxBeadRespawns; i++ {
		RecordBeadRespawn(town, "bead-w")
	}
	if !ShouldBlockRespawn(town, "bead-w") {
		t.Fatal("expected the breaker to trip inside the window")
	}
	state := loadBeadRespawnState(town)
	state.Beads["bead-w"].LastRespawn = time.Now().Add(-BeadRespawnWindow - time.Minute)
	if err := saveBeadRespawnState(town, state); err != nil {
		t.Fatal(err)
	}
	if ShouldBlockRespawn(town, "bead-w") {
		t.Fatal("a stale count (older than the window) still blocks dispatch")
	}
	if got := RecordBeadRespawn(town, "bead-w"); got != 1 {
		t.Fatalf("first respawn after the window = %d, want 1 (count restarts)", got)
	}
}
