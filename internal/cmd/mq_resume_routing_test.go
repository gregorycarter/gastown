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
  update|close) printf '{}\n' ;;
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
			// The real scheduler carries the discovered directory on pending work
			// and context records. Cover failure bookkeeping, success-close and
			// operator-clear/cleanup routes, not just queue creation.
			fields := &capacity.SlingContextFields{Version: 1, WorkBeadID: "hisn-test.1", TargetRig: "hisn", ResumeMR: "hisn-wisp-original", DispatchFailures: 1}
			pending := capacity.PendingBead{ID: created.ID, Context: fields, ContextWorkDir: work, ContextBeadsDir: target}
			if err := beadsForPendingContext(town, pending).UpdateSlingContextFields(created.ID, fields); err != nil {
				t.Fatalf("context failure bookkeeping: %v", err)
			}
			if err := beadsForPendingContext(town, pending).CloseSlingContext(created.ID, "dispatched"); err != nil {
				t.Fatalf("context success-close: %v", err)
			}
			if err := beadsForContextRecord(slingContextRecord{issue: created, workDir: work, beadsDir: target}).CloseSlingContext(created.ID, "cleared"); err != nil {
				t.Fatalf("context record clear: %v", err)
			}
			calls, err := os.ReadFile(log)
			if err != nil {
				t.Fatal(err)
			}
			for _, want := range []string{"list --json --status=all --assignee=hisn/polecats/quartz --limit=0 --flat", "query --json ephemeral=true", "create --json --ephemeral", "dep add hisn-wisp-queued hisn-test.1 --type=tracks", "update hisn-wisp-queued", "close hisn-wisp-queued"} {
				if !strings.Contains(string(calls), want) {
					t.Fatalf("missing %q in subprocess evidence: %s", want, calls)
				}
			}
		})
	}
}

// Exercise the actual CAS-hook command used in BeforeLaunch. Assignment/context
// reads succeeding must not conceal a separate mutation pin to mayor/rig/.beads.
func TestMQResumeHookPinsRigAuthority(t *testing.T) {
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
			if err := os.WriteFile(filepath.Join(target, "metadata.json"), []byte(`{"dolt_database":"hisn_hook_fixture","dolt_server_host":"127.0.0.1","dolt_server_port":45454}`), 0600); err != nil {
				t.Fatal(err)
			}
			script := `#!/bin/sh
[ "${BEADS_DIR-}" = "$HISN_MQ_TEST_BEADS" ] || { echo 'Error: no beads database found (wrong hook target)' >&2; exit 93; }
[ "$PWD" = "$HISN_MQ_TEST_WORK" ] || exit 94
[ "${BEADS_DOLT_SERVER_DATABASE-}" = 'hisn_hook_fixture' ] || exit 95
[ "${BEADS_DOLT_SERVER_PORT-}" = '45454' ] || exit 96
[ "${BEADS_DB-}" = '' ] || exit 97
[ "${BD_READONLY-}" = '' ] || exit 98
[ "${BD_DOLT_AUTO_COMMIT-}" = 'on' ] || exit 99
[ "$*" = 'update hisn-test.1 --if-status=in_progress --status=hooked --assignee=hisn/polecats/quartz' ] || exit 100
printf 'fixture mutation accepted\n'
`
			if err := os.WriteFile(filepath.Join(bin, "bd"), []byte(script), 0700); err != nil {
				t.Fatal(err)
			}
			// PATH contains no real bd; the only subprocess is this shell stub.
			t.Setenv("PATH", bin)
			t.Setenv("GT_TOWN_ROOT", town)
			t.Setenv("HISN_MQ_TEST_BEADS", target)
			t.Setenv("HISN_MQ_TEST_WORK", work)
			t.Setenv("BEADS_DIR", filepath.Join(town, "foreign", ".beads"))
			t.Setenv("BEADS_DOLT_SERVER_DATABASE", "foreign_fixture")
			t.Setenv("BEADS_DOLT_SERVER_PORT", "45555")
			t.Setenv("BEADS_DB", filepath.Join(town, "foreign.db"))
			t.Setenv("GT_DOLT_HOST", "127.0.0.1")
			t.Setenv("GT_DOLT_PORT", "45454")
			t.Setenv("BD_READONLY", "true")
			t.Setenv("BD_DOLT_AUTO_COMMIT", "off")
			source := &beads.Issue{ID: "hisn-test.1", Status: "in_progress", Assignee: "hisn/polecats/quartz"}
			fields := &capacity.SlingContextFields{WorkBeadID: source.ID, TargetRig: "hisn", ResumeWorker: "quartz"}
			if err := mqResumeHookCommand(town, &mqResumeState{Source: source}, fields).Run(); err != nil {
				t.Fatalf("CAS-hook mutation routing: %v", err)
			}
			if source.Status != "in_progress" || source.Assignee != "hisn/polecats/quartz" {
				t.Fatal("command construction rewrote source")
			}
		})
	}
}
