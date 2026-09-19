package cmd

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/steveyegge/gastown/internal/beads"
	"github.com/steveyegge/gastown/internal/scheduler/capacity"
)

// Exercise the real Beads subprocess wrapper, not injected assignment results.
// The only bd on PATH is a stub: these tests cannot connect to any Dolt server.
func TestMQResumeBeadsPinsRigAuthorityForUnprefixedOperations(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("uses a POSIX shell bd fixture")
	}
	for _, layout := range []string{"rig-local", "rig-redirect"} {
		t.Run(layout, func(t *testing.T) {
			town, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			rig := filepath.Join(town, "hisn")
			work := filepath.Join(rig, "mayor", "rig")
			target := filepath.Join(rig, ".beads")
			bin := filepath.Join(town, "bin")
			for _, dir := range []string{work, target, bin} {
				if err := os.MkdirAll(dir, 0700); err != nil {
					t.Fatal(err)
				}
			}
			if layout == "rig-redirect" {
				if err := os.WriteFile(filepath.Join(target, "redirect"), []byte("mayor/rig/.beads"), 0600); err != nil {
					t.Fatal(err)
				}
				target = filepath.Join(work, ".beads")
				if err := os.MkdirAll(target, 0700); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.WriteFile(filepath.Join(target, "metadata.json"), []byte(`{"dolt_database":"hisn_fixture","dolt_server_host":"127.0.0.1","dolt_server_port":45454}`), 0600); err != nil {
				t.Fatal(err)
			}
			log := filepath.Join(town, "calls.log")
			script := `#!/bin/sh
if [ "${BEADS_DIR-}" != "$HISN_MQ_TEST_BEADS" ]; then
  echo 'Error: no beads database found (wrong explicit target)' >&2
  exit 93
fi
[ "$PWD" = "$HISN_MQ_TEST_WORK" ] || exit 94
[ "${BEADS_DOLT_SERVER_DATABASE-}" = 'hisn_fixture' ] || exit 95
[ "${BEADS_DB-}" = '' ] || exit 96
printf '%s\n' "$*" >> "$HISN_MQ_TEST_LOG"
case "$1" in
  --allow-stale) echo 'unknown flag: --allow-stale'; exit 1 ;;
  list) printf '[{"id":"hisn-test.1","status":"in_progress","assignee":"hisn/polecats/quartz"}]\n' ;;
  query) printf '[]\n' ;;
  create) printf '{"id":"hisn-wisp-queued","status":"open"}\n' ;;
  dep) printf '{}\n' ;;
  *) echo 'unexpected fake bd operation' >&2; exit 97 ;;
esac
`
			if err := os.WriteFile(filepath.Join(bin, "bd"), []byte(script), 0700); err != nil {
				t.Fatal(err)
			}
			t.Setenv("PATH", bin)
			t.Setenv("HISN_MQ_TEST_BEADS", target)
			t.Setenv("HISN_MQ_TEST_WORK", work)
			t.Setenv("HISN_MQ_TEST_LOG", log)
			t.Setenv("BEADS_DIR", filepath.Join(town, "foreign", ".beads"))
			t.Setenv("BEADS_DOLT_SERVER_DATABASE", "foreign_fixture")
			t.Setenv("BEADS_DB", filepath.Join(town, "foreign.db"))
			t.Setenv("GT_DOLT_HOST", "127.0.0.1")
			t.Setenv("GT_DOLT_PORT", "45454")
			beads.ResetBdAllowStaleCacheForTest()
			t.Cleanup(beads.ResetBdAllowStaleCacheForTest)
			b := mqResumeBeads(town)
			assigned, err := b.List(beads.ListOptions{Assignee: "hisn/polecats/quartz", Status: "all", Priority: -1})
			if err != nil || len(assigned) != 1 || assigned[0].ID != "hisn-test.1" {
				t.Fatalf("assignment lookup = %v, %v", assigned, err)
			}
			if _, err := b.ListOpenSlingContexts(); err != nil {
				t.Fatalf("context lookup: %v", err)
			}
			created, err := b.CreateSlingContext("fixture", "hisn-test.1", &capacity.SlingContextFields{Version: 1, WorkBeadID: "hisn-test.1", TargetRig: "hisn"})
			if err != nil || created.ID != "hisn-wisp-queued" {
				t.Fatalf("context creation = %v, %v", created, err)
			}
			calls, err := os.ReadFile(log)
			if err != nil {
				t.Fatal(err)
			}
			for _, want := range []string{"list --json --status=all --assignee=hisn/polecats/quartz --limit=0 --flat", "query --json ephemeral=true", "create --json --ephemeral", "dep add hisn-wisp-queued hisn-test.1 --type=tracks"} {
				if !strings.Contains(string(calls), want) {
					t.Fatalf("missing %q in subprocess evidence: %s", want, calls)
				}
			}
		})
	}
}
