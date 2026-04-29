package cmd

import (
	"testing"
	"time"
)

func TestFormatNotifyDuration(t *testing.T) {
	tests := []struct {
		name string
		d    time.Duration
		want string
	}{
		{"seconds", 30 * time.Second, "30s"},
		{"minutes", 5 * time.Minute, "5m"},
		{"hours", 3 * time.Hour, "3h"},
		{"one day", 24 * time.Hour, "1d"},
		{"days only", 5 * 24 * time.Hour, "5d"},
		{"days and hours", 5*24*time.Hour + 3*time.Hour, "5d3h"},
		{"zero", 0, "0s"},
		{"mixed", 2*time.Hour + 30*time.Minute, "2h"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := formatNotifyDuration(tt.d)
			if got != tt.want {
				t.Errorf("formatNotifyDuration(%v) = %q, want %q", tt.d, got, tt.want)
			}
		})
	}
}
