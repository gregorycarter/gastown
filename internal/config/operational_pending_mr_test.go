package config

import (
	"testing"
	"time"
)

func TestPolecatIdleSessionTimeoutPendingMRD(t *testing.T) {
	var nilCfg *DaemonThresholds
	if got := nilCfg.PolecatIdleSessionTimeoutPendingMRD(); got != 60*time.Minute {
		t.Errorf("nil default = %v, want 60m", got)
	}
	empty := &DaemonThresholds{}
	if got := empty.PolecatIdleSessionTimeoutPendingMRD(); got != 60*time.Minute {
		t.Errorf("unset default = %v, want 60m", got)
	}
	set := &DaemonThresholds{PolecatIdleSessionTimeoutPendingMR: "90m"}
	if got := set.PolecatIdleSessionTimeoutPendingMRD(); got != 90*time.Minute {
		t.Errorf("configured = %v, want 90m", got)
	}
	bad := &DaemonThresholds{PolecatIdleSessionTimeoutPendingMR: "not-a-duration"}
	if got := bad.PolecatIdleSessionTimeoutPendingMRD(); got != 60*time.Minute {
		t.Errorf("invalid value = %v, want the 60m default", got)
	}
}
