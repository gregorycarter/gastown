package daemon

import (
	"testing"
)

func TestIsAgentSession(t *testing.T) {
	tests := []struct {
		name string
		want bool
	}{
		{"hq-mayor", true},
		{"rig-witness", true},
		{"rig-refinery", true},
		{"rig-polecat-abc", true},
		{"hq-deacon", true},
		{"hq-boot", true},
		{"rig-dog-fido", true},
		{"my-personal-session", false},
		{"", false},
	}
	for _, tt := range tests {
		if got := isAgentSession(tt.name); got != tt.want {
			t.Errorf("isAgentSession(%q) = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestLoadAverage5_DoesNotPanic(t *testing.T) {
	load := loadAverage5()
	if load < 0 {
		t.Errorf("load average should be >= 0, got %f", load)
	}
}

func TestAvailableMemoryGB_DoesNotPanic(t *testing.T) {
	mem := availableMemoryGB()
	if mem < 0 {
		t.Errorf("available memory should be >= 0, got %f", mem)
	}
}

func TestCPUPressureDeferredHysteresis(t *testing.T) {
	const threshold = 2.0
	const exitRatio = 0.8 // clears below 1.6

	tests := []struct {
		name        string
		wasDeferred bool
		loadPerCore float64
		want        bool
	}{
		{"calm host stays open", false, 1.0, false},
		{"at threshold does not engage", false, 2.0, false},
		{"above threshold engages", false, 2.1, true},
		{"engaged stays engaged in the band", true, 1.8, true},
		{"engaged stays engaged at exit boundary", true, 1.6, true},
		{"engaged clears below exit", true, 1.59, false},
		{"engaged clears on a calm host", true, 0.4, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := cpuPressureDeferred(tc.wasDeferred, tc.loadPerCore, threshold, exitRatio); got != tc.want {
				t.Errorf("cpuPressureDeferred(%v, %.2f) = %v, want %v",
					tc.wasDeferred, tc.loadPerCore, got, tc.want)
			}
		})
	}
}

func TestCPUPressureDeferredDisabledThreshold(t *testing.T) {
	if cpuPressureDeferred(true, 99, 0, 0.8) {
		t.Error("threshold 0 must disable the CPU tier entirely")
	}
}

func TestCPUPressureDeferredInvalidExitRatio(t *testing.T) {
	// A nonsensical ratio must not make the deferral unclearable or instant:
	// it degrades to "clear at the threshold itself".
	if got := cpuPressureDeferred(true, 1.5, 2.0, 0); got {
		t.Error("exitRatio 0 should degrade to the threshold, clearing at 1.5 < 2.0")
	}
	if got := cpuPressureDeferred(true, 2.5, 2.0, 5); !got {
		t.Error("exitRatio > 1 should degrade to the threshold, staying engaged at 2.5")
	}
}

func TestCPUExemptKinds(t *testing.T) {
	if !cpuExemptKinds["refinery"] || !cpuExemptKinds["witness"] {
		t.Error("refinery and witness must be exempt from the CPU tier")
	}
	if cpuExemptKinds["polecat"] || cpuExemptKinds["dog"] {
		t.Error("polecats and dogs must remain CPU-gated")
	}
}
