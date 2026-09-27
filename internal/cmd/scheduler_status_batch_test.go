package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/steveyegge/gastown/internal/beads"
)

func writeTwoRigRoutes(t *testing.T) string {
	t.Helper()
	townRoot := t.TempDir()
	townBeadsDir := filepath.Join(townRoot, ".beads")
	for _, dir := range []string{townBeadsDir, filepath.Join(townRoot, "rig-a", ".beads"), filepath.Join(townRoot, "rig-b", ".beads")} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", dir, err)
		}
	}
	if err := beads.WriteRoutes(townBeadsDir, []beads.Route{
		{Prefix: "a-", Path: "rig-a"},
		{Prefix: "b-", Path: "rig-b"},
	}); err != nil {
		t.Fatalf("write routes: %v", err)
	}
	return townRoot
}

// N contexts across two rigs must cost at most ceil(N/bdShowBatchSize) bd
// show calls per rig, never one per context (hisn-4s8b.2).
func TestBatchFetchBeadInfoByIDsChunksPerRig(t *testing.T) {
	townRoot := writeTwoRigRoutes(t)

	const perRig = 95
	var ids []string
	for i := 0; i < perRig; i++ {
		ids = append(ids, fmt.Sprintf("a-%03d", i), fmt.Sprintf("b-%03d", i))
	}
	ids = append(ids, "a-000") // duplicate must not cost an extra lookup
	const missing = "b-042"

	callsByDir := map[string]int{}
	origShow, origFallback := bdShowBatchFn, beadInfoFallbackFn
	t.Cleanup(func() { bdShowBatchFn, beadInfoFallbackFn = origShow, origFallback })
	bdShowBatchFn = func(beadsDir string, chunk []string) ([]byte, error) {
		callsByDir[beadsDir]++
		if len(chunk) > bdShowBatchSize {
			t.Fatalf("chunk of %d IDs exceeds batch size %d", len(chunk), bdShowBatchSize)
		}
		type item struct {
			ID     string `json:"id"`
			Status string `json:"status"`
			Title  string `json:"title"`
		}
		var items []item
		for _, id := range chunk {
			if id == missing {
				continue
			}
			items = append(items, item{ID: id, Status: "open", Title: "T " + id})
		}
		return json.Marshal(items)
	}
	var fallbacks []string
	beadInfoFallbackFn = func(_ string, id string) (*beadInfo, error) {
		fallbacks = append(fallbacks, id)
		return nil, fmt.Errorf("bead '%s' not found", id)
	}

	got := batchFetchBeadInfoByIDs(townRoot, ids)

	want := (perRig + bdShowBatchSize - 1) / bdShowBatchSize
	if len(callsByDir) != 2 {
		t.Fatalf("bd show ran against %d databases, want 2: %v", len(callsByDir), callsByDir)
	}
	for dir, n := range callsByDir {
		if n > want {
			t.Fatalf("%s: %d bd show calls, want at most %d", dir, n, want)
		}
	}
	if len(got) != 2*perRig-1 {
		t.Fatalf("got %d beads, want %d", len(got), 2*perRig-1)
	}
	if _, ok := got[missing]; ok {
		t.Fatalf("missing bead %s must stay absent (work bead not found)", missing)
	}
	if len(fallbacks) != 1 || fallbacks[0] != missing {
		t.Fatalf("fallback lookups = %v, want only %s", fallbacks, missing)
	}
	if got["a-007"].Title != "T a-007" || got["a-007"].Status != "open" {
		t.Fatalf("bead info not decoded: %+v", got["a-007"])
	}
}

func TestChunkIDs(t *testing.T) {
	ids := make([]string, 81)
	for i := range ids {
		ids[i] = fmt.Sprint(i)
	}
	chunks := chunkIDs(ids, 40)
	if len(chunks) != 3 || len(chunks[0]) != 40 || len(chunks[1]) != 40 || len(chunks[2]) != 1 {
		t.Fatalf("unexpected chunk shape: %d chunks", len(chunks))
	}
	if chunkIDs(nil, 40) != nil {
		t.Fatalf("empty input should yield no chunks")
	}
}

// A work bead ID that bd show cannot find must not cause a bd blocked scan of
// the database it happens to route to (the town DB for "[deleted:...]").
func TestBlockedQuerySkipsDatabasesWithOnlyMissingWorkBeads(t *testing.T) {
	townRoot := writeTwoRigRoutes(t)
	ids := []string{"a-1", "b-1", "[deleted:x-9]"}
	info := map[string]beadStatusInfo{"a-1": {Status: "open"}, "b-1": {Status: "open"}}

	queried := map[string]int{}
	_, _, unknown, err := listBlockedWorkBeadBlockersAndSourcesWithRunner(townRoot, foundWorkBeadIDs(ids, info), func(beadsDir string, _ []string) ([]byte, error) {
		queried[beadsDir]++
		return []byte(`[]`), nil
	})
	if err != nil {
		t.Fatalf("blocked query: %v", err)
	}
	if len(unknown) != 0 {
		t.Fatalf("unexpected unknown: %v", unknown)
	}
	if n := queried[filepath.Join(townRoot, ".beads")]; n != 0 {
		t.Fatalf("town DB scanned %d times for a missing work bead", n)
	}
	if len(queried) != 2 {
		t.Fatalf("queried %d databases, want 2 (one per rig): %v", len(queried), queried)
	}
}
