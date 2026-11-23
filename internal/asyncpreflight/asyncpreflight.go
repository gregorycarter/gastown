// Package asyncpreflight runs a polecat's preflight after `gt done` has opened
// its MR (kasb-eel8t.6: submit while preflight runs).
//
// Flow:
//
//  1. `gt done --async-preflight -- <preflight args>` pushes the head, writes a
//     marker <rig>/.runtime/async-preflight/<source>--<head>.json with status
//     "starting", launches a detached runner, and opens the MR with
//     `preflight_receipt: pending` and `preflight_run: <marker path>`.
//  2. The runner (`gt preflight-async run`) re-checks the worktree is clean at
//     the head, runs the preflight in the polecat's own worktree, heartbeats
//     the marker, and finally records passed/failed/error together with a copy
//     of the receipt next to the marker. The polecat's sandbox is not reused
//     while its MR is pending (active_mr blocks slot reuse) and, as a second
//     interlock, slot reuse also refuses a sandbox with a running marker.
//  3. The lander asks `gt mq preflight-gate <rig> <mr>`: pending -> skip the
//     MR this cycle; passed (receipt for the exact head, or carried over to a
//     rebased head whose change has the same patch-id, kasb-7huzb.41) -> land; failed, lost
//     or invalid -> MR blocked + needs-repair with a correction reason, which
//     the existing Kasb same-MR repair sweep re-dispatches to the original
//     worker.
package asyncpreflight

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"
)

// SchemaVersion of the marker file.
const SchemaVersion = 1

// Marker states.
const (
	StatusStarting = "starting" // written by gt done before the runner starts
	StatusRunning  = "running"  // the runner owns the marker and heartbeats it
	StatusPassed   = "passed"   // a passing receipt for the exact head exists
	StatusFailed   = "failed"   // the preflight produced a non-passing receipt
	StatusError    = "error"    // no usable receipt (crash, worktree changed, ...)
)

// Gate states reported to the lander.
const (
	GateNone    = "none"    // the MR was not submitted with an async preflight
	GatePending = "pending" // still running; skip, do not block the queue
	GatePassed  = "passed"  // landable
	GateFailed  = "failed"  // needs-fix
	GateLost    = "lost"    // runner vanished without a result; needs-fix
	GateInvalid = "invalid" // marker missing/mismatched/outside the run dir; needs-fix
	GateMoved   = "moved"   // branch head differs from the proven head; needs-fix
)

// HeartbeatInterval is how often the runner refreshes heartbeat_at.
const HeartbeatInterval = 30 * time.Second

// HeartbeatFresh is the maximum heartbeat age of a live run. A run is live
// only with a fresh heartbeat AND its runner process (PID plus kernel start
// time, so a reused PID never counts) still present.
const HeartbeatFresh = 3 * time.Minute

// StartGrace is how long a "starting" marker may wait for its runner.
const StartGrace = 3 * time.Minute

// LostAfter is kept for callers: a run older than this without proof of life
// is lost. It equals HeartbeatFresh.
const LostAfter = HeartbeatFresh

// DefaultTimeout bounds one preflight run; on expiry the runner kills the
// preflight and records a failed (timed_out) result.
const DefaultTimeout = 2 * time.Hour

// StaleAfter is the age after which finished or dead markers are pruned.
const StaleAfter = 7 * 24 * time.Hour

// Marker is the durable record of one async preflight run.
type Marker struct {
	SchemaVersion int      `json:"schema_version"`
	Rig           string   `json:"rig"`
	Source        string   `json:"source_bead_id"`
	Branch        string   `json:"branch"`
	Head          string   `json:"head_sha"`
	Worktree      string   `json:"worktree"`
	Command       []string `json:"command"`
	ReceiptPath   string   `json:"receipt_path"`
	ReceiptCopy   string   `json:"receipt_copy"`
	Log           string   `json:"log"`
	Status        string   `json:"status"`
	PID           int      `json:"pid,omitempty"`
	PIDStart      string   `json:"pid_start,omitempty"` // kernel start time of PID
	TimeoutSecs   int      `json:"timeout_seconds,omitempty"`
	CreatedAt     string   `json:"created_at"`
	StartedAt     string   `json:"started_at,omitempty"`
	HeartbeatAt   string   `json:"heartbeat_at,omitempty"`
	FinishedAt    string   `json:"finished_at,omitempty"`
	ExitCode      *int     `json:"exit_code,omitempty"`
	ReceiptStatus string   `json:"receipt_status,omitempty"`
	Reason        string   `json:"reason,omitempty"`
	// RecheckedEnvironment is the infrastructure death that made the runner
	// rerun this head once with --recheck-environment (hq-y1q7ew).
	RecheckedEnvironment string `json:"rechecked_environment,omitempty"`
}

var (
	shaRe    = regexp.MustCompile(`^[0-9a-f]{40}$`)
	sourceRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)
)

// Dir is the run directory of a rig.
func Dir(rigPath string) string { return filepath.Join(rigPath, ".runtime", "async-preflight") }

// MarkerPath returns the marker path for (source, head).
func MarkerPath(rigPath, source, head string) (string, error) {
	if !sourceRe.MatchString(source) || !shaRe.MatchString(head) {
		return "", fmt.Errorf("invalid source %q or head %q", source, head)
	}
	return filepath.Join(Dir(rigPath), source+"--"+head+".json"), nil
}

func sibling(markerPath, suffix string) string {
	return strings.TrimSuffix(markerPath, ".json") + suffix
}

// LogPath is the runner log next to the marker.
func LogPath(markerPath string) string { return sibling(markerPath, ".log") }

// ReceiptCopyPath is where the runner keeps a copy of the final receipt.
func ReceiptCopyPath(markerPath string) string { return sibling(markerPath, ".receipt.json") }

// ReceiptRelPath is the receipt the preflight writes inside the worktree.
func ReceiptRelPath(head string) string {
	return filepath.Join(".delivery", "preflight-receipts", "async-"+head[:12]+".json")
}

// Load reads a marker.
func Load(path string) (*Marker, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var m Marker
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("marker %s: %w", path, err)
	}
	if m.SchemaVersion != SchemaVersion {
		return nil, fmt.Errorf("marker %s: unsupported schema %d", path, m.SchemaVersion)
	}
	return &m, nil
}

// Save writes a marker atomically.
func Save(path string, m *Marker) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".marker-*.tmp")
	if err != nil {
		return err
	}
	if _, err := tmp.Write(append(raw, '\n')); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmp.Name())
		return err
	}
	return os.Rename(tmp.Name(), path)
}

// Create writes a new marker, refusing to replace an existing one.
func Create(path string, m *Marker) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	_ = f.Close()
	return Save(path, m)
}

// Receipt is the subset of the kasb preflight receipt the gate checks.
type Receipt struct {
	Head   string `json:"head_sha"`
	Source string `json:"source_bead_id"`
	Status string `json:"status"`
	Stage  string `json:"stage"`
}

// Passing reports whether a receipt status admits a submission. passed_light
// (the pre-staging stage) never does.
func Passing(status string) bool { return status == "passed" || status == "passed_non_behavioral" }

// ReadReceipt parses a receipt file.
func ReadReceipt(path string) (*Receipt, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var r Receipt
	if err := json.Unmarshal(raw, &r); err != nil {
		return nil, fmt.Errorf("receipt %s: %w", path, err)
	}
	return &r, nil
}

// ProcessStartTime returns the kernel start time of pid ("" if absent).
// Replaced in tests.
var ProcessStartTime = processStartTime

// Timeout returns the run's hard timeout.
func (m *Marker) Timeout() time.Duration {
	if m == nil || m.TimeoutSecs <= 0 {
		return DefaultTimeout
	}
	return time.Duration(m.TimeoutSecs) * time.Second
}

func age(stamp string, now time.Time) (time.Duration, bool) {
	t, err := time.Parse(time.RFC3339Nano, stamp)
	if err != nil {
		return 0, false
	}
	return now.Sub(t), true
}

// runnerPresent reports whether the marker's runner process still exists:
// same PID and same kernel start time (a reused PID has another).
func runnerPresent(m *Marker) bool {
	if m == nil || m.PID <= 0 || m.PIDStart == "" {
		return false
	}
	return ProcessStartTime(m.PID) == m.PIDStart
}

// Live reports whether a marker still represents a run in progress:
//   - starting: created less than StartGrace ago (runner not yet claimed it);
//   - running: heartbeat younger than HeartbeatFresh, the runner process
//     present with its recorded start time, and not past its hard timeout
//     (plus StartGrace for the runner to record the expiry).
func Live(m *Marker, now time.Time) bool {
	if m == nil {
		return false
	}
	switch m.Status {
	case StatusStarting:
		a, ok := age(m.CreatedAt, now)
		return ok && a < StartGrace
	case StatusRunning:
		hb, ok := age(m.HeartbeatAt, now)
		if !ok || hb >= HeartbeatFresh || !runnerPresent(m) {
			return false
		}
		if started, ok := age(m.StartedAt, now); ok && started > m.Timeout()+StartGrace {
			return false
		}
		return true
	}
	return false
}

// Stop sends SIGTERM to a run's runner when it is still the recorded process
// (PID and start time match). The runner then kills the preflight's process
// group and records an error. Returns whether a signal was sent.
func Stop(markerPath string, m *Marker) bool {
	if m == nil || (m.Status != StatusRunning && m.Status != StatusStarting) || !runnerPresent(m) {
		return false
	}
	// hq-y1q7ew: mark the stop as gt's own, so the runner does not rerun.
	_ = os.WriteFile(StopRequestPath(markerPath), []byte(time.Now().UTC().Format(time.RFC3339Nano)+"\n"), 0o644)
	return killProcess(m.PID, syscall.SIGTERM) == nil
}

// killProcess is syscall.Kill; tests replace it.
var killProcess = syscall.Kill

// GateResult is the lander-facing verdict.
type GateResult struct {
	State  string `json:"state"`
	Reason string `json:"reason"`
	Marker string `json:"marker,omitempty"`
	Log    string `json:"log,omitempty"`
	// Carried is set when a passed receipt was carried over to an identical
	// rebased change (kasb-7huzb.41).
	Carried *CarryOver `json:"carried_over,omitempty"`
	// Run is the loaded marker, when there is one (not serialized).
	Run *Marker `json:"-"`
}

// Landable reports whether the lander may land the MR now.
func (g GateResult) Landable() bool { return g.State == GateNone || g.State == GatePassed }

// NeedsFix reports whether the MR must go back to its worker.
func (g GateResult) NeedsFix() bool {
	switch g.State {
	case GateFailed, GateLost, GateInvalid, GateMoved:
		return true
	}
	return false
}

// MRFields are the MR description fields the gate needs.
type MRFields struct {
	Source     string
	CommitSHA  string
	RunPath    string // preflight_run
	ReceiptTag string // preflight_receipt
	Target     string // target (default main)
}

// ParseMRFields extracts the async preflight fields from an MR description.
func ParseMRFields(description string) MRFields {
	var f MRFields
	for _, line := range strings.Split(description, "\n") {
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		value = strings.TrimSpace(value)
		switch strings.ReplaceAll(strings.ToLower(strings.TrimSpace(key)), "-", "_") {
		case "source_issue":
			f.Source = value
		case "commit_sha":
			f.CommitSHA = value
		case "preflight_run":
			f.RunPath = value
		case "preflight_receipt":
			f.ReceiptTag = value
		case "target":
			f.Target = value
		}
	}
	return f
}

// DescriptionFields returns the MR description lines for a marker.
func DescriptionFields(markerPath string) string {
	return "\npreflight_receipt: pending\npreflight_run: " + markerPath
}

// WithDescriptionFields returns description with exactly one pair of async
// preflight fields for markerPath (replacing any earlier pair).
func WithDescriptionFields(description, markerPath string) string {
	var kept []string
	for _, line := range strings.Split(description, "\n") {
		key, _, ok := strings.Cut(line, ":")
		k := strings.ReplaceAll(strings.ToLower(strings.TrimSpace(key)), "-", "_")
		if ok && (k == "preflight_run" || k == "preflight_receipt") {
			continue
		}
		kept = append(kept, line)
	}
	return strings.TrimRight(strings.Join(kept, "\n"), "\n") + DescriptionFields(markerPath)
}

// within reports whether path resolves inside dir (symlinks resolved).
func within(dir, path string) bool {
	if !filepath.IsAbs(path) {
		return false
	}
	rd, err := filepath.EvalSymlinks(dir)
	if err != nil {
		return false
	}
	rp, err := filepath.EvalSymlinks(path)
	if err != nil {
		// A missing file: resolve its directory instead.
		parent, perr := filepath.EvalSymlinks(filepath.Dir(path))
		if perr != nil {
			return false
		}
		rp = filepath.Join(parent, filepath.Base(path))
	}
	rel, err := filepath.Rel(rd, rp)
	return err == nil && rel != "." && !strings.HasPrefix(rel, "..") && !strings.Contains(rel, string(filepath.Separator))
}

// Evaluate decides the gate state of an MR. branchHead, when non-empty, is the
// current remote branch head the lander would land.
//
// An MR without a preflight_run line is still gated when a marker exists for
// its exact source and head (the binding may have failed to reach the MR).
func Evaluate(rigPath string, f MRFields, branchHead string, now time.Time) GateResult {
	if f.RunPath == "" {
		if p, err := MarkerPath(rigPath, f.Source, f.CommitSHA); err == nil {
			if _, statErr := os.Stat(p); statErr == nil {
				f.RunPath = p
			}
		}
	}
	if f.RunPath == "" {
		return GateResult{State: GateNone, Reason: "not an async-preflight submission"}
	}
	res := GateResult{Marker: f.RunPath}
	if !within(Dir(rigPath), f.RunPath) {
		res.State, res.Reason = GateInvalid, fmt.Sprintf("preflight_run %s is outside %s", f.RunPath, Dir(rigPath))
		return res
	}
	m, err := Load(f.RunPath)
	if err != nil {
		res.State, res.Reason = GateInvalid, fmt.Sprintf("async preflight marker unreadable: %v", err)
		return res
	}
	res.Log, res.Run = m.Log, m
	if m.Source != f.Source || m.Head != f.CommitSHA || !shaRe.MatchString(m.Head) {
		res.State, res.Reason = GateInvalid, fmt.Sprintf("marker binds %s@%s but the MR is %s@%s", m.Source, short(m.Head), f.Source, short(f.CommitSHA))
		if m.Source == f.Source && shaRe.MatchString(m.Head) && shaRe.MatchString(f.CommitSHA) {
			co, r, why := carryOver(rigPath, f, m, f.CommitSHA, branchHead)
			if co != nil {
				return carriedResult(res, co, r, m)
			}
			res.Reason += "; receipt not carried over: " + why
		}
		return res
	}
	if branchHead != "" && branchHead != m.Head {
		res.State, res.Reason = GateMoved, fmt.Sprintf("branch head %s is not the async-preflighted head %s; push no new commits after gt done --async-preflight, or rerun the preflight on the new head and resubmit the same MR", short(branchHead), short(m.Head))
		if shaRe.MatchString(branchHead) {
			co, r, why := carryOver(rigPath, f, m, branchHead, branchHead)
			if co != nil {
				return carriedResult(res, co, r, m)
			}
			res.Reason += "; receipt not carried over: " + why
		}
		return res
	}
	switch m.Status {
	case StatusStarting, StatusRunning:
		if Live(m, now) {
			res.State, res.Reason = GatePending, fmt.Sprintf("preflight running for %s since %s (pid %d)", short(m.Head), firstNonEmpty(m.StartedAt, m.CreatedAt), m.PID)
			return res
		}
		res.State, res.Reason = GateLost, fmt.Sprintf("async preflight for %s has no live runner (pid %d gone, reused or hung; last heartbeat %s; timeout %s); log %s", short(m.Head), m.PID, firstNonEmpty(m.HeartbeatAt, m.CreatedAt), m.Timeout(), m.Log)
		return res
	case StatusPassed:
		r, why := passedReceipt(m)
		if r == nil {
			res.State, res.Reason = GateInvalid, why
			return res
		}
		res.State, res.Reason = GatePassed, fmt.Sprintf("preflight %s for %s (receipt %s)", r.Status, short(m.Head), m.ReceiptCopy)
		return res
	case StatusFailed, StatusError:
		res.State = GateFailed
		res.Reason = fmt.Sprintf("async preflight %s for %s: %s; log %s", firstNonEmpty(m.ReceiptStatus, m.Status), short(m.Head), firstNonEmpty(m.Reason, "see log"), m.Log)
		return res
	default:
		res.State, res.Reason = GateInvalid, fmt.Sprintf("unknown marker status %q", m.Status)
		return res
	}
}

// RunningForWorktree returns a non-empty reason when a live async preflight
// is using worktree. Used to refuse sandbox reuse while it runs.
func RunningForWorktree(rigPath, worktree string, now time.Time) string {
	files, _ := filepath.Glob(filepath.Join(Dir(rigPath), "*.json"))
	want := filepath.Clean(worktree)
	for _, p := range files {
		if strings.HasSuffix(p, ".receipt.json") {
			continue
		}
		m, err := Load(p)
		if err != nil || filepath.Clean(m.Worktree) != want {
			continue
		}
		if Live(m, now) {
			return fmt.Sprintf("async-preflight running for %s@%s (pid %d)", m.Source, short(m.Head), m.PID)
		}
	}
	return ""
}

func short(sha string) string {
	if len(sha) > 9 {
		return sha[:9]
	}
	return sha
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}
