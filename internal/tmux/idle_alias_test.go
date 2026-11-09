package tmux

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Real Kasb sessions use GT_AGENT=codex-isolated, not the built-in preset name.
// Exercise steady-state idle detection and wait-idle delivery against that session.
func TestIdleCustomCodexSession(t *testing.T) {
	for _, tc := range []struct {
		name, agent, processes, pane string
		idle                         bool
	}{
		{"alias completed", "codex-isolated", "codex", "Worked for 50s\n› Ask Codex to do anything\nGPT-6.1 Terra medium", true},
		{"alias active model", "codex-isolated", "codex", "• Working (1m • esc to interrupt)\n› Ask Codex to do anything", false},
		{"alias active test", "codex-isolated", "codex", "• Waiting for background terminal (3m • esc to interrupt)\n› Ask Codex to do anything", false},
		{"unknown runtime", "custom", "custom-bin", "› Ask Codex to do anything", false},
		{"ambiguous processes", "custom", "codex,custom-bin", "› Ask Codex to do anything", false},
		{"built in Claude", "claude", "node,claude", "❯ Ask anything", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			script := `#!/bin/sh
case " $* " in
 *" capture-pane "*) printf '%s\n' "$GT_TEST_IDLE_PANE" ;;
 *" show-environment "*" GT_AGENT "*) printf 'GT_AGENT=%s\n' "$GT_TEST_IDLE_AGENT" ;;
 *" show-environment "*" GT_PROCESS_NAMES "*) printf 'GT_PROCESS_NAMES=%s\n' "$GT_TEST_IDLE_PROCESSES" ;;
 *) exit 90 ;;
esac
`
			if err := os.WriteFile(filepath.Join(dir, "tmux"), []byte(script), 0700); err != nil {
				t.Fatal(err)
			}
			t.Setenv("PATH", dir)
			t.Setenv("GT_TEST_IDLE_AGENT", tc.agent)
			t.Setenv("GT_TEST_IDLE_PROCESSES", tc.processes)
			t.Setenv("GT_TEST_IDLE_PANE", tc.pane)
			tm := NewTmuxWithSocket("hermetic-idle-test")
			if got := tm.IsIdle("worker"); got != tc.idle {
				t.Fatalf("IsIdle=%v, want %v", got, tc.idle)
			}
			err := tm.WaitForIdle("worker", 450*time.Millisecond)
			if (err == nil) != tc.idle {
				t.Fatalf("WaitForIdle error=%v, idle=%v", err, tc.idle)
			}
		})
	}
}
