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
	Prefix   []string      // preflight argv prefix from rig settings
	Args     []string      // the polecat's preflight arguments
	Timeout  time.Duration // hard timeout of the run (0: DefaultTimeout)
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
// idempotent gt done retry). A failed run for the same head and the same
// command is refused: the head needs a fix. An errored or lost run, or a
// failed run whose preflight arguments differ (kasb-xeon8: an argument-only
// failure must not force a new commit), is retired and replaced.
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
	receipt := ReceiptRelPath(req.Head)
	command := append(append([]string(nil), req.Prefix...), req.Args...)
	command = append(command, "--head", req.Head, "--source-bead-id", req.Source, "--receipt", receipt)
	if existing, loadErr := Load(path); loadErr == nil {
		switch {
		case existing.Status == StatusPassed || Live(existing, req.Now):
			return existing, path, true, nil
		case existing.Status == StatusFailed && sameCommand(existing.Command, command):
			return nil, "", false, fmt.Errorf("the async preflight for %s already failed with these arguments (%s; log %s); fix it, commit and run gt done again", short(req.Head), existing.Reason, existing.Log)
		default:
			if _, err := Retire(path, req.Now); err != nil {
				return nil, "", false, fmt.Errorf("cannot retire earlier async preflight marker: %w", err)
			}
		}
	} else if !errors.Is(loadErr, os.ErrNotExist) {
		return nil, "", false, loadErr
	}
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
		TimeoutSecs:   int(req.Timeout / time.Second),
	}
	if err := Create(path, m); err != nil {
		return nil, "", false, err
	}
	return m, path, false, nil
}

func sameCommand(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Retire moves a finished run that does not pass (failed, error, lost or an
// unreadable marker) aside: the MR sidecar, receipt copy, log and finally the
// marker are renamed to <file>.<unixnano>.prev, which Prune deletes after
// StaleAfter. It reports whether anything was retired. A missing, passed or
// live run is left alone; a run whose runner process still exists is asked to
// stop and reported as an error.
//
// kasb-xeon8: Evaluate falls back to the marker for the MR's exact
// source@head and Create refuses an existing marker, so an un-retired failure
// blocked its head for good, even after a passing synchronous preflight.
func Retire(markerPath string, now time.Time) (bool, error) {
	m, err := Load(markerPath)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err == nil && (m.Status == StatusPassed || Live(m, now)) {
		return false, nil
	}
	// A runner that is still alive (for example, a stale heartbeat) would Save
	// the marker back after a rename. Ask it to stop and retire on a later try.
	if err == nil && runnerPresent(m) {
		Stop(m)
		return false, fmt.Errorf("async preflight runner pid %d for %s@%s is still alive; asked it to stop, retry once it has exited", m.PID, m.Source, short(m.Head))
	}
	stamp := now.UnixNano()
	for _, p := range []string{MRSidecarPath(markerPath), ReceiptCopyPath(markerPath), LogPath(markerPath), markerPath} {
		if _, statErr := os.Lstat(p); statErr != nil {
			continue
		}
		if err := os.Rename(p, fmt.Sprintf("%s.%d.prev", p, stamp)); err != nil {
			return false, err
		}
	}
	return true, nil
}
