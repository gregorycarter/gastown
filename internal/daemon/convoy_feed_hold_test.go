package daemon

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	beadsdk "github.com/steveyegge/beads"
)

// newHoldFeedManager builds a ConvoyManager whose `gt sling` is a mock that
// records its arguments, and whose issue reads come from issues.
func newHoldFeedManager(t *testing.T, issues map[string]*beadsdk.Issue, settingsJSON string) (*ConvoyManager, string, *[]string) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("skipping on Windows")
	}
	binDir := t.TempDir()
	townRoot := t.TempDir()
	if err := os.MkdirAll(filepath.Join(townRoot, ".beads"), 0755); err != nil {
		t.Fatal(err)
	}
	routes := `{"prefix":"gt-","path":"gt/.beads"}` + "\n"
	if err := os.WriteFile(filepath.Join(townRoot, ".beads", "routes.jsonl"), []byte(routes), 0644); err != nil {
		t.Fatal(err)
	}
	if settingsJSON != "" {
		if err := os.MkdirAll(filepath.Join(townRoot, "settings"), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(townRoot, "settings", "config.json"), []byte(settingsJSON), 0644); err != nil {
			t.Fatal(err)
		}
	}
	slingLog := filepath.Join(binDir, "sling.log")
	script := "#!/bin/sh\nif [ \"$1\" = \"sling\" ]; then echo \"$@\" >> \"" + slingLog + "\"; fi\nexit 0\n"
	gtPath := filepath.Join(binDir, "gt")
	if err := os.WriteFile(gtPath, []byte(script), 0755); err != nil {
		t.Fatal(err)
	}
	var logged []string
	logger := func(format string, args ...interface{}) {
		logged = append(logged, fmt.Sprintf(format, args...))
	}
	m := NewConvoyManager(townRoot, logger, gtPath, 10*time.Minute, nil, nil, nil)
	m.lookupFeedIssue = func(_ string, id string) *beadsdk.Issue { return issues[id] }
	return m, slingLog, &logged
}

func readSlingLog(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	return string(data)
}

// hq-mw7902: kasb-mfyhh (demo, dispatch:hold, live-validation) was fed by a
// convoy. Held beads are skipped; the first unlabelled bead still feeds.
func TestFeedFirstReady_SkipsHeldBeadsAndFeedsUnlabelled(t *testing.T) {
	issues := map[string]*beadsdk.Issue{
		"gt-hold": {ID: "gt-hold", Status: "open", Labels: []string{"demo", "dispatch:hold"}},
		"gt-live": {ID: "gt-live", Status: "open", Labels: []string{"live-validation"}},
		"gt-free": {ID: "gt-free", Status: "open", Labels: []string{"demo"}},
	}
	m, slingLog, logged := newHoldFeedManager(t, issues, "")

	m.feedFirstReady(strandedConvoyInfo{ID: "hq-cv1", ReadyIssues: []string{"gt-hold", "gt-live", "gt-free"}})

	got := readSlingLog(t, slingLog)
	if strings.Contains(got, "gt-hold") || strings.Contains(got, "gt-live") {
		t.Fatalf("held bead was slung: %q", got)
	}
	if !strings.Contains(got, "sling gt-free gt") {
		t.Fatalf("unlabelled bead was not fed: %q", got)
	}
	joined := strings.Join(*logged, "\n")
	for _, want := range []string{"gt-hold, held: dispatch:hold", "gt-live, held: live-validation"} {
		if !strings.Contains(joined, want) {
			t.Errorf("missing log %q in:\n%s", want, joined)
		}
	}
	if m.feedBackoffActive("hq-cv1") {
		t.Error("a successful feed must not leave a backoff")
	}
}

// A convoy whose ready issues are all held is waiting, not failing: no sling,
// no backoff, and each hold is logged once across scans.
func TestFeedFirstReady_AllHeld_NoFailureAndLoggedOnce(t *testing.T) {
	issues := map[string]*beadsdk.Issue{
		"gt-hold": {ID: "gt-hold", Status: "open", Labels: []string{"dispatch:hold"}},
		"gt-live": {ID: "gt-live", Status: "open", Labels: []string{"live-validation"}},
	}
	m, slingLog, logged := newHoldFeedManager(t, issues, "")
	c := strandedConvoyInfo{ID: "hq-cv2", ReadyIssues: []string{"gt-hold", "gt-live"}}

	m.feedFirstReady(c)
	m.feedFirstReady(c)

	if got := readSlingLog(t, slingLog); got != "" {
		t.Fatalf("held beads were slung: %q", got)
	}
	if m.feedBackoffActive("hq-cv2") {
		t.Error("held beads must not count as a feed failure (backoff)")
	}
	held := 0
	for _, line := range *logged {
		if strings.Contains(line, "held:") {
			held++
		}
		if strings.Contains(line, "backing off") || strings.Contains(line, "failed") {
			t.Errorf("unexpected failure log: %q", line)
		}
	}
	if held != 2 {
		t.Errorf("logged %d held lines over two scans, want 2 (once per bead): %v", held, *logged)
	}

	// Released: the bead feeds on the next scan.
	issues["gt-hold"].Labels = nil
	m.feedFirstReady(c)
	if got := readSlingLog(t, slingLog); !strings.Contains(got, "sling gt-hold gt") {
		t.Fatalf("released bead was not fed: %q", got)
	}
}

// The configured scheduler.autofeed_exclude_labels is honoured, and
// dispatch:hold holds even when the configured list omits it.
func TestFeedFirstReady_UsesConfiguredExcludeLabels(t *testing.T) {
	issues := map[string]*beadsdk.Issue{
		"gt-watch": {ID: "gt-watch", Status: "open", Labels: []string{"watchdog"}},
		"gt-hold":  {ID: "gt-hold", Status: "open", Labels: []string{"dispatch:hold"}},
		"gt-live":  {ID: "gt-live", Status: "open", Labels: []string{"live-validation"}},
	}
	settings := `{"type":"town-settings","version":1,"scheduler":{"max_polecats":7,"autofeed_exclude_labels":["watchdog"]}}`
	m, slingLog, _ := newHoldFeedManager(t, issues, settings)

	m.feedFirstReady(strandedConvoyInfo{ID: "hq-cv3", ReadyIssues: []string{"gt-watch", "gt-hold", "gt-live"}})

	got := readSlingLog(t, slingLog)
	if strings.Contains(got, "gt-watch") || strings.Contains(got, "gt-hold") {
		t.Fatalf("held bead was slung: %q", got)
	}
	// live-validation is not in this town's configured list, so it feeds.
	if !strings.Contains(got, "sling gt-live gt") {
		t.Fatalf("bead outside the configured list was not fed: %q", got)
	}
}
