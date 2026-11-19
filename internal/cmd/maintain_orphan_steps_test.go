package cmd

import (
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/steveyegge/gastown/internal/atomicfile"
	"github.com/steveyegge/gastown/internal/daemon"
	"github.com/steveyegge/gastown/internal/formula"
)

func TestOrphanStepSourceRefs(t *testing.T) {
	cases := []struct {
		name, desc string
		want       []string
	}{
		{"bd show", "gt hook\nbd show kasb-g0io4           # Validates Dolt connectivity", []string{"kasb-g0io4"}},
		{"issue line", "  MR ID: gt-xxxxx\n  Source: polecat/<name>\n  Issue: kasb-g0io4\n", []string{"kasb-g0io4"}},
		{"dotted id and trailing period", "Run bd show kasb-kmv.8. Then Issue: kasb-kmv.8", []string{"kasb-kmv.8"}},
		{"unrendered template", "bd show {{issue}} and Issue: <id>", nil},
		{"two sources", "bd show bt-1\nIssue: kasb-2", []string{"bt-1", "kasb-2"}},
		{"no prefix", "bd show something", nil},
	}
	for _, c := range cases {
		if got := orphanStepSourceRefs(c.desc); !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: refs = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestComponentRefs_StepsInheritSourceFromSiblings(t *testing.T) {
	// One orphaned molecule: load -> branch -> build, chained by blocks.
	// Only load names the source; build-check names nothing. A previous run
	// already closed "submit" (not a candidate) which also names it.
	cands := []orphanStepCandidate{
		{ID: "w-load", Refs: []string{"kasb-1"}},
		{ID: "w-branch"},
		{ID: "w-build"},
		{ID: "w-lonely"}, // another molecule, no source anywhere
		{ID: "w-solo-b"}, // molecule whose only source is on a non-candidate sibling
	}
	neighbors := map[string][]string{"w-submit-closed": {"kasb-9"}}
	edges := [][2]string{
		{"w-branch", "w-load"},
		{"w-build", "w-branch"},
		{"w-solo-b", "w-submit-closed"},
	}
	got := componentRefs(cands, neighbors, edges)
	for _, id := range []string{"w-load", "w-branch", "w-build"} {
		if !reflect.DeepEqual(got[id], []string{"kasb-1"}) {
			t.Errorf("%s refs = %v, want [kasb-1]", id, got[id])
		}
	}
	if len(got["w-lonely"]) != 0 {
		t.Errorf("w-lonely refs = %v, want none", got["w-lonely"])
	}
	if !reflect.DeepEqual(got["w-solo-b"], []string{"kasb-9"}) {
		t.Errorf("w-solo-b refs = %v, want [kasb-9]", got["w-solo-b"])
	}
	if _, ok := got["w-submit-closed"]; ok {
		t.Error("non-candidate neighbour must not be returned")
	}
}

func TestPlanOrphanStepReap_Selection(t *testing.T) {
	cands := []orphanStepCandidate{
		{ID: "a"}, {ID: "b"}, {ID: "c"}, {ID: "d"}, {ID: "e"}, {ID: "f"}, {ID: "g"},
	}
	refs := map[string][]string{
		"a": {"kasb-closed"},
		"b": nil,                              // no source
		"c": {"zz-elsewhere"},                 // not an issue in this DB
		"d": {"kasb-closed", "kasb-closed-2"}, // ambiguous
		"e": {"kasb-open"},                    // source still open
		"f": {"kasb-closed", "zz-elsewhere"},  // one existing source: eligible
		"g": {"kasb-closed-2"},
	}
	status := map[string]string{
		"kasb-closed":   "closed",
		"kasb-closed-2": "closed",
		"kasb-open":     "in_progress",
	}
	plan := planOrphanStepReap(cands, refs, status, 2)
	want := orphanStepPlan{
		Candidates: 7, EligibleTotal: 3,
		Picks:    []orphanStepPick{{ID: "a", Source: "kasb-closed"}, {ID: "f", Source: "kasb-closed"}},
		NoSource: 1, SourceUnknown: 1, Ambiguous: 1, SourceOpen: 1,
	}
	if !reflect.DeepEqual(plan, want) {
		t.Errorf("plan = %+v\nwant  %+v", plan, want)
	}
}

func TestResolveOrphanStepSettings(t *testing.T) {
	def, err := resolveOrphanStepSettings(nil, "", 0)
	if err != nil || def.Mode != orphanStepModeDryRun || def.Cap != 2000 || def.MinAge != 48*time.Hour {
		t.Fatalf("defaults = %+v, %v", def, err)
	}

	cfg := &daemon.DaemonPatrolConfig{Patrols: &daemon.PatrolsConfig{
		ScheduledMaintenance: &daemon.ScheduledMaintenanceConfig{
			OrphanStepReap: &daemon.OrphanStepReapConfig{Mode: "close", Cap: 500, MinAge: "72h"},
		},
	}}
	got, err := resolveOrphanStepSettings(cfg, "", 0)
	if err != nil || got.Mode != "close" || got.Cap != 500 || got.MinAge != 72*time.Hour {
		t.Fatalf("config = %+v, %v", got, err)
	}
	got, err = resolveOrphanStepSettings(cfg, "off", 10)
	if err != nil || got.Mode != "off" || got.Cap != 10 {
		t.Fatalf("flag override = %+v, %v", got, err)
	}
	if _, err := resolveOrphanStepSettings(nil, "delete", 0); err == nil {
		t.Error("unknown mode must be rejected")
	}
	cfg.Patrols.ScheduledMaintenance.OrphanStepReap.MinAge = "soon"
	if _, err := resolveOrphanStepSettings(cfg, "", 0); err == nil {
		t.Error("invalid min_age must be rejected")
	}
	if maintainDryRunOrphanMode("close") != "dry-run" || maintainDryRunOrphanMode("off") != "off" {
		t.Error("--dry-run must downgrade close to dry-run and keep off")
	}
}

func TestOrphanStepCandidateQuery_Shape(t *testing.T) {
	q := orphanStepCandidateQuery(3)
	if got := strings.Count(q, "?"); got != 1+3+4 {
		t.Errorf("placeholders = %d, want 8 (cutoff, 3 titles, keyset x3, limit)", got)
	}
	for _, want := range []string{"w.status = 'open'", "issue_type <> 'molecule'", "d.type = 'parent-child'", "ORDER BY w.created_at, w.id"} {
		if !strings.Contains(q, want) {
			t.Errorf("query missing %q", want)
		}
	}
	if regexp.MustCompile(`(?i)\b(delete|update|insert)\s`).MatchString(q) {
		t.Error("selection query must be read-only")
	}
}

func TestStepTitles_EmbeddedAndTown(t *testing.T) {
	town := t.TempDir()
	dir := filepath.Join(town, ".beads", "formulas")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	custom := "formula = \"mol-x\"\nversion = 1\n[[steps]]\nid = \"a\"\ntitle = \"Custom town step\"\n[[steps]]\nid = \"b\"\ntitle = \"Templated {{issue}}\"\n"
	if err := os.WriteFile(filepath.Join(dir, "mol-x.formula.toml"), []byte(custom), 0o644); err != nil {
		t.Fatal(err)
	}
	titles := formula.StepTitles(town)
	has := map[string]bool{}
	for _, tt := range titles {
		has[tt] = true
	}
	for _, want := range []string{"Load context and verify assignment", "Implement the solution", "Pre-merge rebase verification", "Custom town step"} {
		if !has[want] {
			t.Errorf("titles missing %q", want)
		}
	}
	for tt := range has {
		if strings.Contains(tt, "{") {
			t.Errorf("templated title %q included", tt)
		}
	}
}

func TestMaintainMetricsLine(t *testing.T) {
	town := t.TempDir()
	line := maintainMetricsLine(town, map[string]int{"hq": 5900, "kasb_core": 7821, "beads": -1}, []string{"hq", "kasb_core", "beads"})
	if line != "maintain metrics: open wisps hq=5900 kasb_core=7821; convoy checks last hour=unknown" {
		t.Errorf("line = %q", line)
	}
	snap := daemon.ConvoyMetricsSnapshot{WrittenAt: time.Date(2026, 11, 19, 2, 55, 0, 0, time.UTC), ConvoyChecks: 312}
	if err := atomicfile.EnsureDirAndWriteJSON(daemon.ConvoyMetricsFile(town), snap); err != nil {
		t.Fatal(err)
	}
	line = maintainMetricsLine(town, map[string]int{"hq": 1}, []string{"hq"})
	if !strings.HasSuffix(line, "convoy checks last hour=312 (window ending 02:55Z)") {
		t.Errorf("line = %q", line)
	}
}

func TestFormatOrphanStepReport(t *testing.T) {
	r := orphanStepReport{DB: "kasb_core", Plan: orphanStepPlan{
		Candidates: 10, EligibleTotal: 4,
		Picks: []orphanStepPick{{"w1", "kasb-1"}, {"w2", "kasb-1"}, {"w3", "kasb-2"}, {"w4", "kasb-2"}},
	}}
	s := orphanStepSettings{Mode: orphanStepModeDryRun, Cap: 2000, MinAge: 48 * time.Hour}
	line := formatOrphanStepReport(r, s)
	for _, want := range []string{"orphan-steps kasb_core [dry-run]: would close 4 of 4 eligible", "scanned 10", "e.g. w1<-kasb-1, w2<-kasb-1, w3<-kasb-2"} {
		if !strings.Contains(line, want) {
			t.Errorf("line %q missing %q", line, want)
		}
	}
	r.Closed = 3
	r.Errors = []string{"bd close: boom\nmore"}
	s.Mode = orphanStepModeClose
	line = formatOrphanStepReport(r, s)
	if !strings.Contains(line, "closed 3 of 4 eligible") || !strings.Contains(line, "first: bd close: boom") {
		t.Errorf("close line = %q", line)
	}
}
