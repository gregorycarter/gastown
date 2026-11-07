package cmd

// Kasb's existing recovery tick may continue one proven idle, unfinished Codex
// turn. It never stops a process or creates/reassigns work.
import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/steveyegge/gastown/internal/atomicfile"
	"github.com/steveyegge/gastown/internal/beads"
)

func continuationSourceAllowed(source *beads.Issue) bool {
	if source == nil || !strings.HasPrefix(source.ID, "kasb-") ||
		(source.Status != "hooked" && source.Status != "in_progress") {
		return false
	}
	for _, label := range source.Labels {
		switch label {
		case "needs-operator", "needs-operator-rollout", "dispatch:hold", "needs-review", "live-validation":
			return false
		}
		if strings.HasPrefix(label, "awaiting-merge:") {
			return false
		}
	}
	// Unknown metadata may represent a handoff, run wait or real approval hold.
	// Leave it for operator disposition rather than interpreting an expiry.
	meta := strings.TrimSpace(string(source.Metadata))
	return meta == "" || meta == "null" || meta == "{}"
}

func continuationMRPending(source string, mrs []*beads.Issue) bool {
	for _, mr := range mrs {
		if mr.Status == "closed" {
			continue
		}
		f := beads.ParseMRFields(mr)
		if f != nil && f.SourceIssue == source {
			return true
		}
	}
	return false
}

func validateWorkerContinuation(town string, source *beads.Issue, evidence *waitingEvidence, b *beads.Beads) error {
	if !continuationSourceAllowed(source) || source.Assignee != "kasb_core/polecats/"+evidence.Worker {
		return fmt.Errorf("source held or custody changed")
	}
	if err := validateMQResumeDependencies(source, b.Show, map[string]bool{}, true); err != nil {
		return err
	}
	mrs, err := b.ListMergeRequests(beads.ListOptions{Status: "all", Label: "gt:merge-request", Rig: "kasb_core", Limit: 0, Priority: -1})
	if err != nil {
		return err
	}
	if continuationMRPending(source.ID, mrs) {
		return fmt.Errorf("source has pending MR")
	}
	id := beads.PolecatBeadIDWithPrefix("kasb", "kasb_core", evidence.Worker)
	_, fields, err := b.GetAgentBead(id)
	if err != nil || fields == nil || fields.HookBead != source.ID || fields.AgentState != "working" {
		return fmt.Errorf("native hook or agent state changed")
	}
	return nil
}

type workerContinuation struct {
	Evidence waitingEvidence
	Status   string
	Error    string
}

// Caller owns the assignee lock. At most one attempt per worker launch/source,
// even if the worker returns another analysis-only final. A failed delivery is
// durable evidence for an operator, never permission for an automatic loop.
func continueWorkerOnce(path string, evidence waitingEvidence, deliver func() error) error {
	if evidence.Launched.IsZero() || !evidence.Continuation {
		return fmt.Errorf("missing native continuation evidence")
	}
	raw, err := os.ReadFile(path)
	if err == nil {
		var old workerContinuation
		if err := json.Unmarshal(raw, &old); err != nil {
			return err
		}
		if old.Evidence.Source == evidence.Source && old.Evidence.Launched.Equal(evidence.Launched) {
			return fmt.Errorf("continuation already attempted; inspect %s", path)
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	record := workerContinuation{Evidence: evidence, Status: "attempting"}
	if err := atomicfile.EnsureDirAndWriteJSONWithPerm(path, record, 0600); err != nil {
		return err
	}
	err = deliver()
	record.Status = "delivered"
	if err != nil {
		record.Status, record.Error = "failed", err.Error()
	}
	if writeErr := atomicfile.WriteJSONWithPerm(path, record, 0600); writeErr != nil {
		return writeErr
	}
	return err
}
