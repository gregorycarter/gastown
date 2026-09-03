//go:build !darwin && !linux && !windows

package daemon

// loadAverage5Sysctl is a no-op on unsupported platforms.
func loadAverage5Sysctl() float64 {
	return 0
}

// availableMemoryGB is a no-op on unsupported platforms.
func availableMemoryGB() float64 {
	return 0
}
