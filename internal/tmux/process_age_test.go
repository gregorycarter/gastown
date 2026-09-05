package tmux

import (
	"os"
	"strconv"
	"testing"
	"time"
)

func TestParseProcessElapsed(t *testing.T) {
	for value, want := range map[string]time.Duration{"01:30": 90 * time.Second, "02:03:04": 7384 * time.Second, "2-01:02:03": 176523 * time.Second} {
		got, err := parseElapsedTime(value)
		if err != nil || got != want {
			t.Fatalf("%s: got %v,%v want %v", value, got, err, want)
		}
	}
	for _, value := range []string{"", "no", "-1:00", "1-"} {
		if _, err := parseElapsedTime(value); err == nil {
			t.Fatalf("accepted %q", value)
		}
	}
}

func TestProcessAgeUsesProcess(t *testing.T) {
	age, err := ProcessAge(strconv.Itoa(os.Getpid()))
	if err != nil || age < 0 || age > time.Hour {
		t.Fatalf("age=%v err=%v", age, err)
	}
}
