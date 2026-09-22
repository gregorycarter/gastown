//go:build !windows

package polecat

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// Only the generated dependency cache layout is disposable. This is called
// under assignment/lifecycle locks after source and merge proof checks, twice
// before worktree removal and once again immediately before cache removal.
func inspectRetirementCache(parent string) (string, string, error) {
	cache := filepath.Join(parent, ".cache")
	info, err := os.Lstat(cache)
	if os.IsNotExist(err) {
		return "", "", nil
	}
	if err != nil {
		return "", "", err
	}
	root, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || root.Uid != uint32(os.Getuid()) {
		return "", "", fmt.Errorf("dependency cache is not an owned directory")
	}
	entries, err := os.ReadDir(cache)
	if err != nil {
		return "", "", err
	}
	if len(entries) != 1 || entries[0].Name() != "polecat-deps" {
		return "", "", fmt.Errorf("unrecognized dependency cache layout")
	}
	deps := filepath.Join(cache, "polecat-deps")
	allowed := map[string]bool{"npm-cache": true, "pip": true, "pnpm-store": true, "pypoetry": true, "venvs": true}
	err = filepath.WalkDir(cache, func(p string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		stat, err := entry.Info()
		if err != nil {
			return err
		}
		s, ok := stat.Sys().(*syscall.Stat_t)
		if !ok || s.Uid != root.Uid || s.Dev != root.Dev {
			return fmt.Errorf("foreign owner or filesystem in dependency cache")
		}
		if p == deps && (!stat.IsDir() || stat.Mode()&os.ModeSymlink != 0) {
			return fmt.Errorf("redirected dependency cache")
		}
		if filepath.Dir(p) == deps && !allowed[entry.Name()] {
			return fmt.Errorf("unknown dependency cache entry: %s", entry.Name())
		}
		if !stat.IsDir() && !stat.Mode().IsRegular() && stat.Mode()&os.ModeSymlink == 0 {
			return fmt.Errorf("special file in dependency cache")
		}
		return nil // WalkDir never follows venv executable/package symlinks.
	})
	if err != nil {
		return "", "", err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "lsof", "-nP", "-a", "-u", strconv.Itoa(os.Getuid()), "-Fn")
	out, err := cmd.Output()
	if err != nil {
		return "", "", fmt.Errorf("dependency cache open-file inventory unavailable: %w", err)
	}
	for _, line := range strings.Split(string(out), "\n") {
		if strings.HasPrefix(line, "n") {
			p := strings.TrimPrefix(line, "n")
			if p == parent || strings.HasPrefix(p, parent+string(os.PathSeparator)) {
				return "", "", fmt.Errorf("worker directory has open files")
			}
		}
	}
	return cache, fmt.Sprintf("%d:%d:%d", root.Dev, root.Ino, info.ModTime().UnixNano()), nil
}

func removeRetirementCache(parent, identity string) error {
	cache, fresh, err := inspectRetirementCache(parent)
	if err != nil {
		return err
	}
	if cache == "" || fresh != identity {
		return fmt.Errorf("dependency cache identity changed; retained")
	}
	// Immutable environments have read-only directories. Change directory
	// permissions only; never follow symlinks or chmod a linked external file.
	if err := filepath.WalkDir(cache, func(p string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			info, err := entry.Info()
			if err != nil {
				return err
			}
			return os.Chmod(p, info.Mode().Perm()|0700)
		}
		return nil
	}); err != nil {
		return err
	}
	return os.RemoveAll(cache)
}

// Physical filesystem free space, never logical du size or APFS purgeable
// estimates. The signed observation can include concurrent activity elsewhere.
func retirementHostFree(path string) *int64 {
	var stat syscall.Statfs_t
	if syscall.Statfs(path, &stat) != nil {
		return nil
	}
	bytes := int64(stat.Bavail) * int64(stat.Bsize)
	return &bytes
}
