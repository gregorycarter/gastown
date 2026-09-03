package tmux

import (
	"os"
	"strconv"
	"testing"
)

func TestFindDescendantPIDWithNames(t *testing.T) {
	// 100 (pane shell) -> 200 (wrapper) -> 300 (codex)
	//                  -> 201 (unrelated)
	ps := []byte("100 1 zsh\n200 100 sh\n300 200 codex\n201 100 grep\n")
	snapshot := parseProcessSnapshot(ps)

	if got := findDescendantPIDWithNames("100", []string{"codex"}, snapshot); got != "300" {
		t.Errorf("codex pid = %q, want 300", got)
	}
	if got := findDescendantPIDWithNames("100", []string{"claude"}, snapshot); got != "" {
		t.Errorf("missing agent should return empty, got %q", got)
	}
	if got := findDescendantPIDWithNames("100", nil, snapshot); got != "" {
		t.Errorf("no names should return empty, got %q", got)
	}
	if got := findDescendantPIDWithNames("100", []string{"grep", "codex"}, snapshot); got != "201" {
		t.Errorf("breadth-first should find the shallow match first, got %q", got)
	}
}

func TestFindDescendantPIDWithNamesFullPathComm(t *testing.T) {
	ps := []byte("10 1 bash\n11 10 /usr/local/bin/claude\n")
	snapshot := parseProcessSnapshot(ps)
	if got := findDescendantPIDWithNames("10", []string{"claude"}, snapshot); got != "11" {
		t.Errorf("full-path comm should match by basename, got %q", got)
	}
}

func TestFindDescendantPIDWithNamesCycleSafe(t *testing.T) {
	// Self-parenting entry must not loop forever.
	ps := []byte("10 10 bash\n")
	snapshot := parseProcessSnapshot(ps)
	if got := findDescendantPIDWithNames("10", []string{"codex"}, snapshot); got != "" {
		t.Errorf("cycle should terminate with empty result, got %q", got)
	}
}

func TestProcessRSSMBInvalidPID(t *testing.T) {
	if _, err := ProcessRSSMB("not-a-pid"); err == nil {
		t.Error("expected error for invalid pid")
	}
}

func TestProcessRSSMBSelf(t *testing.T) {
	// The test binary itself always has a resident set.
	mb, err := ProcessRSSMB(currentPIDString())
	if err != nil {
		t.Skipf("ps unavailable: %v", err)
	}
	if mb < 0 {
		t.Errorf("rss = %d, want >= 0", mb)
	}
}

func currentPIDString() string {
	return strconv.Itoa(os.Getpid())
}
