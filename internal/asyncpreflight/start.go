package asyncpreflight

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
)

// StartRequest describes the run gt done wants to start.
type StartRequest struct {
	RigPath  string
	Rig      string
	Source   string
	Branch   string
	Head     string
	Worktree string
	Prefix   []string // preflight argv prefix from rig settings
	Args     []string // the polecat's preflight arguments
	Now      time.Time
}

// gt owns these: they bind the receipt to the submitted head.
var forbiddenArgs = []string{"--receipt", "--head", "--source-bead-id", "--red-command", "--red-expect"}

// ValidateArgs refuses preflight arguments that would let the receipt prove
// something other than the submitted head, or that never admit a submission.
func ValidateArgs(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("--async-preflight needs the preflight arguments after --, e.g. gt done --async-preflight -- --requirement R --acceptance-test A")
	}
	for i, a := range args {
		name, value, hasValue := strings.Cut(a, "=")
		for _, f := range forbiddenArgs {
			if name == f {
				return fmt.Errorf("%s is set by gt done --async-preflight; remove it", f)
			}
		}
		if name == "--stage" {
			if !hasValue && i+1 < len(args) {
				value = args[i+1]
			}
			if value != "broad" {
				return fmt.Errorf("--stage %s never admits a submission; async preflight runs the broad stage", value)
			}
		}
		if i == 0 && !strings.HasPrefix(a, "-") {
			return fmt.Errorf("preflight subcommand %q is not a preflight run", a)
		}
	}
	return nil
}

// Prepare returns the marker for req. When a live or passed marker already
// exists for the same source and head it is returned with reused=true (an
// idempotent gt done retry). A failed run for the same head is refused: the
// head needs a fix. An errored or lost run is replaced.
func Prepare(req StartRequest) (m *Marker, path string, reused bool, err error) {
	if err := ValidateArgs(req.Args); err != nil {
		return nil, "", false, err
	}
	if len(req.Prefix) == 0 {
		return nil, "", false, fmt.Errorf("empty async preflight command")
	}
	path, err = MarkerPath(req.RigPath, req.Source, req.Head)
	if err != nil {
		return nil, "", false, err
	}
	if existing, loadErr := Load(path); loadErr == nil {
		switch {
		case existing.Status == StatusPassed || Live(existing, req.Now):
			return existing, path, true, nil
		case existing.Status == StatusFailed:
			return nil, "", false, fmt.Errorf("the async preflight for %s already failed (%s; log %s); fix it, commit and run gt done again", short(req.Head), existing.Reason, existing.Log)
		default:
			prev := fmt.Sprintf("%s.%d.prev", path, req.Now.Unix())
			if err := os.Rename(path, prev); err != nil {
				return nil, "", false, fmt.Errorf("cannot retire earlier async preflight marker: %w", err)
			}
		}
	} else if !errors.Is(loadErr, os.ErrNotExist) {
		return nil, "", false, loadErr
	}
	receipt := ReceiptRelPath(req.Head)
	command := append(append([]string(nil), req.Prefix...), req.Args...)
	command = append(command, "--head", req.Head, "--source-bead-id", req.Source, "--receipt", receipt)
	m = &Marker{
		SchemaVersion: SchemaVersion,
		Rig:           req.Rig,
		Source:        req.Source,
		Branch:        req.Branch,
		Head:          req.Head,
		Worktree:      req.Worktree,
		Command:       command,
		ReceiptPath:   receipt,
		ReceiptCopy:   ReceiptCopyPath(path),
		Log:           LogPath(path),
		Status:        StatusStarting,
		CreatedAt:     req.Now.UTC().Format(time.RFC3339Nano),
	}
	if err := Create(path, m); err != nil {
		return nil, "", false, err
	}
	return m, path, false, nil
}
