//go:build windows

package polecat

import (
	"fmt"
	"os"
	"path/filepath"
)

func inspectRetirementCache(parent string) (string, string, error) {
	if _, err := os.Lstat(filepath.Join(parent, ".cache")); os.IsNotExist(err) {
		return "", "", nil
	}
	return "", "", fmt.Errorf("dependency cache retirement requires Unix ownership and open-file inventory")
}
func removeRetirementCache(parent, identity string) error {
	return fmt.Errorf("dependency cache retirement unsupported")
}
func retirementHostFree(path string) *int64 { return nil }
