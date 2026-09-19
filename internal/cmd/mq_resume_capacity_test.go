package cmd

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/steveyegge/gastown/internal/config"
	"github.com/steveyegge/gastown/internal/wisp"
)

// Both external executables are replaced. This harness cannot contact live
// Dolt or tmux; fixtures intentionally contain no real worker directories.
func mqResumeCapacityTown(t *testing.T, townMax int, names string) string {
	t.Helper()
	town := setupPolecatCapacityTestTown(t, townMax)
	if err := config.SaveRigsConfig(filepath.Join(town, "mayor", "rigs.json"), &config.RigsConfig{Version: config.CurrentRigsVersion, Rigs: map[string]config.RigEntry{"hisn": {BeadsConfig: &config.BeadsConfig{Prefix: "hisn"}}, "bridge_town_core": {BeadsConfig: &config.BeadsConfig{Prefix: "bt"}}}}); err != nil {
		t.Fatal(err)
	}
	writeJSONFile(t, wisp.NewConfig(town, "hisn").ConfigPath(), wisp.ConfigFile{Rig: "hisn", Values: map[string]any{"max_polecats": 4}})
	bin := t.TempDir()
	for name, script := range map[string]string{"bd": "#!/bin/sh\necho 'unexpected bd access in isolated capacity test' >&2\nexit 91\n", "tmux": "#!/bin/sh\ncase \"$*\" in *list-sessions*) printf '%s' \"$HISN_TEST_SESSION_NAMES\";; *) echo 'unexpected tmux mutation' >&2;exit 92;; esac\n"} {
		if err := os.WriteFile(filepath.Join(bin, name), []byte(script), 0755); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("HISN_TEST_SESSION_NAMES", names)
	return town
}

func TestMQResumeRigCapIndependentOfLargerTownCap(t *testing.T) {
	for _, townMax := range []int{12, -1} {
		t.Run(fmt.Sprint(townMax), func(t *testing.T) {
			town := mqResumeCapacityTown(t, townMax, "hisn-a\nhisn-b\nhisn-c\nhisn-witness\nhisn-refinery\nbt-one\nbt-two\n")
			first, _, err := acquirePolecatAdmission(town, "hisn", "hisn-test", "same-mr-resume")
			if err != nil {
				t.Fatal(err)
			}
			defer first.Release()
			if first.disabled {
				t.Fatal("explicit rig cap disabled by town direct mode")
			}
			second, _, err := acquirePolecatAdmission(town, "hisn", "hisn-test2", "test")
			if second != nil {
				second.Release()
			}
			var denied *polecatCapacityAdmissionError
			if !errors.As(err, &denied) || !strings.Contains(err.Error(), "rig hisn max_polecats capacity is full (4/4)") {
				t.Fatalf("cap not enforced: %v", err)
			}
			first.Release()
			third, _, err := acquirePolecatAdmission(town, "hisn", "hisn-test3", "test")
			if err != nil {
				t.Fatal(err)
			}
			third.Release()
		})
	}
}

func TestMQResumeRigCapConcurrentReservations(t *testing.T) {
	town := mqResumeCapacityTown(t, 12, "hisn-a\nhisn-b\nhisn-c\n")
	var wg sync.WaitGroup
	var mu sync.Mutex
	var handles []*polecatAdmissionHandle
	start := make(chan struct{})
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			h, _, err := acquirePolecatAdmission(town, "hisn", "hisn-race", "same-mr-resume")
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				handles = append(handles, h)
			} else {
				var denied *polecatCapacityAdmissionError
				if !errors.As(err, &denied) && !strings.Contains(err.Error(), "admission is busy") {
					t.Errorf("unexpected failure %v", err)
				}
			}
		}()
	}
	close(start)
	wg.Wait()
	defer func() {
		for _, h := range handles {
			h.Release()
		}
	}()
	if len(handles) != 1 {
		t.Fatalf("%d additional slots admitted with three workers and rig cap four", len(handles))
	}
}

func TestMQResumeRigCapDoesNotAlterUnconfiguredRig(t *testing.T) {
	town := mqResumeCapacityTown(t, 12, "hisn-a\nhisn-b\nhisn-c\nhisn-d\n")
	h, _, err := acquirePolecatAdmission(town, "bridge_town_core", "bt-test", "test")
	if err != nil {
		t.Fatal(err)
	}
	h.Release()
}

func TestMQResumeRigCapMalformedConfigFailsClosed(t *testing.T) {
	for _, value := range []any{0, -1, 4.5, "4", nil} {
		t.Run(fmt.Sprint(value), func(t *testing.T) {
			town := mqResumeCapacityTown(t, 12, "")
			writeJSONFile(t, wisp.NewConfig(town, "hisn").ConfigPath(), wisp.ConfigFile{Rig: "hisn", Values: map[string]any{"max_polecats": value}})
			if h, _, err := acquirePolecatAdmission(town, "hisn", "hisn-test", "test"); err == nil {
				h.Release()
				t.Fatal("invalid rig cap accepted")
			}
		})
	}
}
