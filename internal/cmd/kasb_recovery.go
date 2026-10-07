package cmd

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/steveyegge/gastown/internal/beads"
)

// Kasb's mechanical lander records blocked/needs-rebase, but historically did
// not emit Hisn's recovery receipt. Derive it only from freshly validated native
// MR/source/worker state. Never replace an existing (possibly stale) receipt.
func kasbRecoveryCandidate(mr *beads.Issue, base string, show func(string) (*beads.Issue, error)) (*mqResumeState, error) {
	if mr == nil {
		return nil, fmt.Errorf("missing MR")
	}
	metadata := map[string]json.RawMessage{}
	if len(mr.Metadata) != 0 && json.Unmarshal(mr.Metadata, &metadata) != nil {
		return nil, fmt.Errorf("invalid MR metadata")
	}
	if _, exists := metadata["delivery_recovery"]; exists {
		return nil, fmt.Errorf("existing recovery receipt requires disposition")
	}
	f := beads.ParseMRFields(mr)
	if f == nil || f.Rig != "kasb_core" || !mqResumeSHA.MatchString(base) {
		return nil, fmt.Errorf("invalid Kasb MR/base")
	}
	record := mqResumeRecord{SchemaVersion: 1, Kind: "same-mr-rebase", Rig: f.Rig,
		MR: mr.ID, Source: f.SourceIssue, Worker: f.Worker, Branch: f.Branch, Submitted: f.CommitSHA,
		Base: base, CreatedAt: time.Now().UTC().Format(time.RFC3339Nano), DescriptionSHA256: mqResumeHash(mr.Description)}
	record.Instructions = recoveryInstructions(record)
	raw, _ := json.Marshal(record)
	metadata["delivery_recovery"] = raw
	candidate := *mr
	candidate.Metadata, _ = json.Marshal(metadata)
	return validateMQResumeRecords("kasb_core", mr.ID, func(id string) (*beads.Issue, error) {
		if id == mr.ID {
			return &candidate, nil
		}
		return show(id)
	})
}

func prepareKasbRecovery(town, mrID string, dry bool) (*mqResumeState, error) {
	if parked, reason := IsRigParkedOrDocked(town, "kasb_core"); parked {
		return nil, fmt.Errorf("rig %s", reason)
	}
	b := mqResumeBeads(town, "kasb_core")
	mr, err := b.Show(mrID)
	if err != nil {
		return nil, err
	}
	f := beads.ParseMRFields(mr)
	if f == nil {
		return nil, fmt.Errorf("missing MR mapping")
	}
	unlock, err := tryAcquireSlingBeadLock(town, f.SourceIssue)
	if err != nil {
		return nil, err
	}
	defer unlock()
	mr, err = b.Show(mrID)
	if err != nil {
		return nil, err
	}
	root := filepath.Join(town, "kasb_core", "mayor", "rig")
	// Verify the stable repository authority before trusting its remote main.
	origin, err := mqResumeGit(root, "remote", "get-url", "origin")
	spec, _ := recoveryRig("kasb_core")
	if err != nil || strings.TrimSuffix(origin, ".git") != spec.Origin {
		return nil, fmt.Errorf("foreign recovery origin")
	}
	remote, err := mqResumeGit(root, "ls-remote", "--refs", "origin", "refs/heads/main")
	fields := strings.Fields(remote)
	if err != nil || len(fields) != 2 || fields[1] != "refs/heads/main" {
		return nil, fmt.Errorf("current main unavailable")
	}
	state, err := kasbRecoveryCandidate(mr, fields[0], b.Show)
	if err != nil {
		return nil, err
	}
	if err = validateMQResumeWorker(town, state, b); err != nil {
		return nil, err
	}
	if dry {
		return state, nil
	}
	fresh, err := b.Show(mrID)
	if err != nil || fresh.Status != mr.Status || fresh.Description != mr.Description || string(fresh.Metadata) != string(mr.Metadata) {
		return nil, fmt.Errorf("MR changed during recovery preparation")
	}
	raw, _ := json.Marshal(state.Record)
	if err = BdCmd("update", mrID, "--if-status=blocked", "--set-metadata=delivery_recovery="+string(raw)).
		Dir(root).WithBeadsDir(beads.ResolveBeadsDir(filepath.Join(town, "kasb_core"))).WithAutoCommit().Run(); err != nil {
		return nil, err
	}
	return loadMQResumeState(town, "kasb_core", mrID)
}
