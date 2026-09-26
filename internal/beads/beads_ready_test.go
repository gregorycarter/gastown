package beads

import (
	"os"
	"path/filepath"
	"testing"
)

// installReadyCapBDStub mimics bd ready's default 50-row cap: without
// "-n 0" it returns only the first row, with it it returns both.
func installReadyCapBDStub(t *testing.T) {
	t.Helper()
	ResetBdAllowStaleCacheForTest()
	t.Cleanup(ResetBdAllowStaleCacheForTest)

	binDir := t.TempDir()
	script := `#!/bin/sh
if [ "${1:-}" = "--allow-stale" ]; then
  if [ "${2:-}" = "version" ]; then
    echo "Error: unknown flag: --allow-stale" >&2
    exit 0
  fi
  shift
fi
case "${1:-}" in
  ready)
    case "$*" in
      *"-n 0"*)
        printf '%s\n' '[{"id":"gt-epic","title":"Epic","status":"open","priority":2,"issue_type":"epic"},{"id":"gt-spike","title":"Spike","status":"open","priority":3,"issue_type":"spike"}]'
        ;;
      *)
        printf '%s\n' '[{"id":"gt-epic","title":"Epic","status":"open","priority":2,"issue_type":"epic"}]'
        ;;
    esac
    exit 0
    ;;
  *)
    printf '%s\n' '[]'
    exit 0
    ;;
esac
`
	if err := os.WriteFile(filepath.Join(binDir, "bd"), []byte(script), 0755); err != nil {
		t.Fatalf("write bd stub: %v", err)
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
}

func TestReadyAllReadsPastDefaultCap(t *testing.T) {
	installReadyCapBDStub(t)

	b := New(t.TempDir())
	capped, err := b.Ready()
	if err != nil {
		t.Fatalf("Ready() error = %v", err)
	}
	if len(capped) != 1 {
		t.Fatalf("Ready() returned %d rows, want the stub's capped 1", len(capped))
	}

	all, err := b.ReadyAll()
	if err != nil {
		t.Fatalf("ReadyAll() error = %v", err)
	}
	if len(all) != 2 || all[1].ID != "gt-spike" {
		t.Fatalf("ReadyAll() = %#v, want gt-epic and gt-spike", all)
	}
}
