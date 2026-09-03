package daemon

// loadAverage5Sysctl is a no-op on Windows — load average is not a standard metric.
func loadAverage5Sysctl() float64 {
	return 0
}

// availableMemoryGB is a no-op on Windows — pressure checks are not supported.
func availableMemoryGB() float64 {
	return 0
}
