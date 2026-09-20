package checkpoint

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// Only the exact untracked checkpoint written for this preserved source may
// accompany recovery. Unknown, tracked, malformed or foreign files remain holds.
func ValidateRecoveryStatus(root, status, branch, head, issue string) ([]byte, error) {
	if status == "" {
		return nil, nil
	}
	if status != "?? "+Filename {
		return nil, fmt.Errorf("uncommitted source changes must remain preserved")
	}
	info, err := os.Lstat(Path(root))
	if err != nil || !info.Mode().IsRegular() || info.Size() > 64*1024 {
		return nil, fmt.Errorf("checkpoint is missing, redirected or oversized")
	}
	data, err := os.ReadFile(Path(root))
	if err != nil {
		return nil, err
	}
	var cp Checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil, fmt.Errorf("invalid recovery checkpoint: %w", err)
	}
	if cp.LastCommit != head || cp.Branch != branch || cp.HookedBead != issue || cp.Timestamp.IsZero() || cp.Timestamp.After(time.Now().Add(time.Minute)) {
		return nil, fmt.Errorf("checkpoint does not describe this preserved source")
	}
	for _, file := range cp.ModifiedFiles {
		if file != Filename {
			return nil, fmt.Errorf("checkpoint records uncommitted source: %s", file)
		}
	}
	return data, nil
}

// Move the verified checkpoint out of the repository before the worker starts.
// Content-addressed archives are retained alongside the worktree, never GC'd here.
func ArchiveRecovery(root, branch, head, issue string) error {
	cmd := exec.Command("git", "status", "--porcelain")
	cmd.Dir = root
	status, err := cmd.Output()
	if err != nil {
		return err
	}
	data, err := ValidateRecoveryStatus(root, strings.TrimSpace(string(status)), branch, head, issue)
	if err != nil {
		return err
	}
	// A tracked or ignored checkpoint is not our untracked recovery artifact.
	if data == nil {
		return nil
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(root))
	if err != nil {
		return err
	}
	dir := filepath.Join(parent, ".runtime", "recovery-checkpoints")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	if real, err := filepath.EvalSymlinks(dir); err != nil || real != dir {
		return fmt.Errorf("checkpoint archive redirected")
	}
	archive := filepath.Join(dir, fmt.Sprintf("%x.json", sha256.Sum256(data)))
	f, err := os.OpenFile(archive, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err == nil {
		_, err = f.Write(data)
		closeErr := f.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
	} else if !os.IsExist(err) {
		return err
	}
	info, err := os.Lstat(archive)
	if err != nil || !info.Mode().IsRegular() {
		return fmt.Errorf("checkpoint archive is not a regular file")
	}
	stored, err := os.ReadFile(archive)
	if err != nil || !bytes.Equal(stored, data) {
		return fmt.Errorf("checkpoint archive not verified; original retained")
	}
	fresh, err := os.ReadFile(Path(root))
	if err != nil || !bytes.Equal(fresh, data) {
		return fmt.Errorf("checkpoint changed during archival; original retained")
	}
	return os.Remove(Path(root))
}
