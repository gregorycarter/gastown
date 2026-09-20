package cmd

import (
	"fmt"
	"testing"
	"time"
)

func TestCompletedWorkerTurnRequiresSettledNativeCompletion(t *testing.T) {
	now := time.Date(2026, 9, 20, 15, 0, 0, 0, time.UTC)
	launch := now.Add(-time.Hour)
	row := func(kind string, at time.Time) string {
		return fmt.Sprintf("{\"timestamp\":%q,\"type\":\"event_msg\",\"payload\":{\"type\":%q}}\n", at.Format(time.RFC3339), kind)
	}
	done := row("task_complete", now.Add(-5*time.Minute))
	for _, tc := range []struct {
		name, data string
		ok         bool
	}{
		{"complete", done, true},
		{"usage after final", done + row("token_count", now.Add(-4*time.Minute)), true},
		{"new turn", done + row("task_started", now.Add(-3*time.Minute)), false},
		{"new user", done + row("user_message", now.Add(-3*time.Minute)), false},
		{"new tool", done + row("item_completed", now.Add(-3*time.Minute)), false},
		{"quiet active", row("task_started", launch.Add(time.Minute)), false},
		{"old launch", row("task_complete", launch.Add(-time.Minute)), false},
		{"grace", row("task_complete", now.Add(-time.Minute)), false},
		{"corrupt", done + "{broken}\n", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := completedWorkerTurn([]byte(tc.data), launch, now)
			if (err == nil) != tc.ok {
				t.Fatalf("err=%v expected valid=%v", err, tc.ok)
			}
		})
	}
}

func TestWaitingWorkerProtectsLiveSubprocesses(t *testing.T) {
	runtime := "10 1 /opt/bin/codex\n11 10 /opt/vendor/codex\n"
	for _, tc := range []struct {
		name, extra string
		ok          bool
	}{
		{"idle", "", true}, {"shell", "12 11 /bin/zsh\n", false}, {"test", "12 11 node\n", false},
		{"compiler", "12 11 python3\n", false}, {"unrelated", "42 1 node\n", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := onlyWorkerRuntime([]byte(runtime+tc.extra), "10"); got != tc.ok {
				t.Fatalf("got %v", got)
			}
		})
	}
	if onlyWorkerRuntime([]byte("10 1 zsh\n"), "10") {
		t.Fatal("interactive shell accepted")
	}
	if onlyWorkerRuntime([]byte(runtime), "99") {
		t.Fatal("missing PID accepted")
	}
}
