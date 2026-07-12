//go:build !linux && !darwin

package artifact

func isMountPoint(_ string) (bool, error) {
	return false, nil
}
