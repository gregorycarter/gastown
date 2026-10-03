package witness

import (
	"errors"
	"strings"
	"testing"
)

// Steps of an orphaned mol-polecat-work are chained with blocks deps; bd
// refuses a plain close of a blocked step and refuses the root while steps
// are open. The witness cleanup must force-close both.
func TestCloseMoleculeWithDescendantsForcesBlockedSteps(t *testing.T) {
	var closes []string
	bd := &BdCli{
		Exec: func(_ string, args ...string) (string, error) {
			if len(args) > 1 && args[0] == "list" && args[1] == "--parent=mol-1" {
				return `[{"id":"step-1","status":"open"},{"id":"step-2","status":"open"}]`, nil
			}
			return `[]`, nil
		},
		Run: func(_ string, args ...string) error {
			if args[0] != "close" {
				return nil
			}
			force := false
			for _, a := range args {
				if a == "--force" {
					force = true
				}
			}
			if !force {
				return errors.New("cannot close blocked issue (use --force to override)")
			}
			closes = append(closes, strings.Join(args, " "))
			return nil
		},
	}

	closed, err := closeMoleculeWithDescendants(bd, "/tmp", "mol-1")
	if err != nil {
		t.Fatalf("closeMoleculeWithDescendants: %v (closes=%v)", err, closes)
	}
	if closed != 3 {
		t.Fatalf("closed = %d, want 3 (2 steps + root); closes=%v", closed, closes)
	}
	if len(closes) != 2 || !strings.Contains(closes[0], "step-1") || !strings.Contains(closes[1], "mol-1") {
		t.Fatalf("unexpected close order: %v", closes)
	}
}
