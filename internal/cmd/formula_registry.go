package cmd

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/steveyegge/gastown/internal/beads"
	"github.com/steveyegge/gastown/internal/config"
)

// bd cook accepts paths, but bd wisp/bond resolve only registry names. A
// content-addressed alias bridges that API without replacing a shared formula,
// changing the canonical formula title, or persisting prototype/step beads.
func registeredPinnedFormula(town, rig, workDir, name string) (string, error) {
	pinned, err := config.PinnedFormulaFile(town, rig, name)
	if err != nil || pinned == "" {
		return name, err
	}
	return registerPinnedFormulaFile(pinned, workDir)
}

func registerPinnedFormulaFile(pinned, workDir string) (string, error) {
	data, err := BdCmd("cook", pinned, "--search-path", filepath.Dir(pinned)).Dir(workDir).Output()
	if err != nil {
		return "", fmt.Errorf("parse pinned formula: %w", err)
	}
	return publishFormulaAlias(filepath.Join(beads.ResolveBeadsDir(workDir), "formulas"), data)
}

func publishFormulaAlias(dir string, data []byte) (string, error) {
	var f struct {
		Formula string `json:"formula"`
	}
	if err := json.Unmarshal(data, &f); err != nil || f.Formula == "" {
		return "", fmt.Errorf("pinned cook returned no formula")
	}
	name := fmt.Sprintf("mol-gt-pin-%x", sha256.Sum256(data))
	if err := os.MkdirAll(dir, 0700); err != nil {
		return "", err
	}
	if info, err := os.Lstat(dir); err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("unsafe formula registry")
	}
	target := filepath.Join(dir, name+".formula.json")
	if info, err := os.Lstat(target); err == nil {
		if !info.Mode().IsRegular() {
			return "", fmt.Errorf("unsafe existing formula alias")
		}
		old, err := os.ReadFile(target)
		if err != nil || !bytes.Equal(old, data) {
			return "", fmt.Errorf("formula alias content mismatch")
		}
		return name, nil
	} else if !os.IsNotExist(err) {
		return "", err
	}
	// Publish complete bytes atomically without overwriting a concurrent writer.
	tmp, err := os.CreateTemp(dir, ".gt-formula-")
	if err != nil {
		return "", err
	}
	defer os.Remove(tmp.Name()) // exact temporary file owned by this invocation
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return "", err
	}
	if err := tmp.Close(); err != nil {
		return "", err
	}
	if err := os.Link(tmp.Name(), target); err != nil {
		if !os.IsExist(err) {
			return "", err
		}
		return publishFormulaAlias(dir, data)
	}
	return name, nil
}
