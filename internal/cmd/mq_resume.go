package cmd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/steveyegge/gastown/internal/beads"
	"github.com/steveyegge/gastown/internal/polecat"
	"github.com/steveyegge/gastown/internal/scheduler/capacity"
	"github.com/steveyegge/gastown/internal/session"
	"github.com/steveyegge/gastown/internal/tmux"
	"github.com/steveyegge/gastown/internal/workspace"
)

var mqResumeJSON, mqResumeDryRun bool

func init() {
	c := &cobra.Command{Use: "resume <rig> <mr-id>", Short: "Queue admission-aware recovery on the original worker and MR",
		Long: "Queue recovery of an existing blocked Hisn MR. No worker starts here: the normal daemon pressure/capacity tick admits the original worker. No new source, molecule, branch or MR is created.",
		Args: cobra.ExactArgs(2), RunE: runMQResume}
	c.Flags().BoolVar(&mqResumeJSON, "json", false, "Output the queued identity as JSON")
	c.Flags().BoolVar(&mqResumeDryRun, "dry-run", false, "Validate without creating scheduler context")
	mqCmd.AddCommand(c)
}

type mqResumeRecord struct {
	SchemaVersion     int    `json:"schemaVersion"`
	Kind              string `json:"kind"`
	Rig               string `json:"rig"`
	MR                string `json:"mr"`
	Source            string `json:"source"`
	Worker            string `json:"worker"`
	Branch            string `json:"branch"`
	Submitted         string `json:"submitted"`
	Base              string `json:"base"`
	CreatedAt         string `json:"createdAt"`
	DescriptionSHA256 string `json:"descriptionSha256"`
	Instructions      string `json:"instructions"`
}

type mqResumeState struct {
	MR          *beads.Issue
	Source      *beads.Issue
	Record      mqResumeRecord
	ReceiptHash string
	WorkDir     string
}

type mqResumeResult struct {
	Status  string `json:"status"`
	Context string `json:"context,omitempty"`
	MR      string `json:"mr"`
	Source  string `json:"source"`
	Rig     string `json:"rig"`
	Worker  string `json:"worker"`
	Branch  string `json:"branch"`
	Head    string `json:"head"`
}

var mqResumeSHA = regexp.MustCompile(`^[a-f0-9]{40}$`)
var mqResumeMR = regexp.MustCompile(`^hisn-wisp-[a-z0-9]+$`)
var mqResumeBranch = regexp.MustCompile(`^polecat/([a-z0-9_-]+)/(hisn-[a-z0-9]+(?:\.[1-9][0-9]*)*)(?:[+@][a-z0-9_-]+)?$`)

func mqResumeHash(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

func mqResumeHeld(issue *beads.Issue) bool {
	return beads.HasLabel(issue, "needs-operator") || beads.HasLabel(issue, "needs-operator-rollout")
}

// This is deliberately an opt-in contract for Hisn's durable recovery receipt.
// It does not reinterpret Bridge Town's reject-and-resling workflow.
func validateMQResumeRecords(rigName, mrID string, show func(string) (*beads.Issue, error)) (*mqResumeState, error) {
	if rigName != "hisn" || !mqResumeMR.MatchString(mrID) {
		return nil, fmt.Errorf("unsupported rig or recovery MR")
	}
	mr, err := show(mrID)
	if err != nil {
		return nil, err
	}
	if mr == nil || mr.ID != mrID || mr.Status != "blocked" || !beads.HasLabel(mr, "needs-rebase") || !beads.HasLabel(mr, "gt:merge-request") || mqResumeHeld(mr) {
		return nil, fmt.Errorf("MR is not an unheld, blocked needs-rebase merge request")
	}
	f := beads.ParseMRFields(mr)
	if f == nil || f.Rig != rigName || f.Target != "main" || !mqResumeSHA.MatchString(f.CommitSHA) {
		return nil, fmt.Errorf("invalid MR mapping")
	}
	match := mqResumeBranch.FindStringSubmatch(f.Branch)
	if match == nil || match[1] != f.Worker || match[2] != f.SourceIssue {
		return nil, fmt.Errorf("MR worker/branch/source mismatch")
	}
	// Reject duplicate structured fields rather than relying on last-one-wins.
	for _, key := range []string{"rig", "target", "branch", "worker", "source_issue", "commit_sha"} {
		count := 0
		for _, line := range strings.Split(mr.Description, "\n") {
			k, _, ok := strings.Cut(line, ":")
			canonical := strings.ReplaceAll(strings.ToLower(strings.TrimSpace(k)), "-", "_")
			if canonical == "sourceissue" {
				canonical = "source_issue"
			}
			if canonical == "commitsha" {
				canonical = "commit_sha"
			}
			if ok && canonical == key {
				count++
			}
		}
		if count != 1 {
			return nil, fmt.Errorf("ambiguous MR field %s", key)
		}
	}
	var meta map[string]json.RawMessage
	if err := json.Unmarshal(mr.Metadata, &meta); err != nil {
		return nil, fmt.Errorf("missing recovery metadata")
	}
	raw := meta["delivery_recovery"]
	var encoded string
	if json.Unmarshal(raw, &encoded) == nil {
		raw = json.RawMessage(encoded)
	}
	var record mqResumeRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		return nil, fmt.Errorf("invalid recovery receipt: %w", err)
	}
	if record.SchemaVersion != 1 || record.Kind != "same-mr-rebase" || record.Rig != rigName || record.MR != mr.ID || record.Source != f.SourceIssue || record.Worker != f.Worker || record.Branch != f.Branch || record.Submitted != f.CommitSHA || !mqResumeSHA.MatchString(record.Base) || record.DescriptionSHA256 != mqResumeHash(mr.Description) {
		return nil, fmt.Errorf("stale or mismatched recovery receipt")
	}
	if _, err := time.Parse(time.RFC3339Nano, record.CreatedAt); err != nil {
		return nil, fmt.Errorf("invalid recovery time")
	}
	wantInstructions := fmt.Sprintf("Original worker hisn/%s: recover source %s on %s; resolve/rebase onto %s, commit as Greg Carter <email@gregorycarter.net>, run and publish renewed exact-HEAD preflight, push only this worker branch with an explicit expected-old-head lease, run node tools/delivery/resubmit.mjs %s --apply, then gt done --pre-verified. Preserve this MR and source; do not create conflict/verifier tasks or close the source.", record.Worker, record.Source, record.Branch, record.Base, record.MR)
	if record.Instructions != wantInstructions {
		return nil, fmt.Errorf("recovery instruction contract changed")
	}
	source, err := show(f.SourceIssue)
	if err != nil {
		return nil, err
	}
	if source == nil || source.ID != f.SourceIssue || mqResumeHeld(source) || (source.Status != "open" && source.Status != "hooked" && source.Status != "in_progress") {
		return nil, fmt.Errorf("source closed, unavailable or held")
	}
	actor := rigName + "/polecats/" + f.Worker
	if source.Assignee != "" && source.Assignee != actor {
		return nil, fmt.Errorf("source reassigned")
	}
	for _, label := range source.Labels {
		if strings.HasPrefix(label, "awaiting-merge:") && label != "awaiting-merge:"+mrID {
			return nil, fmt.Errorf("source awaiting another MR")
		}
	}
	if err := validateMQResumeDependencies(source, show, map[string]bool{}, true); err != nil {
		return nil, err
	}
	return &mqResumeState{MR: mr, Source: source, Record: record, ReceiptHash: mqResumeHash(string(raw))}, nil
}

func validateMQResumeDependencies(issue *beads.Issue, show func(string) (*beads.Issue, error), seen map[string]bool, root bool) error {
	if seen[issue.ID] {
		return fmt.Errorf("dependency parent cycle")
	}
	seen[issue.ID] = true
	defer delete(seen, issue.ID)
	attachment := beads.ParseAttachmentFields(issue)
	for _, dep := range issue.Dependencies {
		switch dep.DependencyType {
		case "tracks", "related", "discovered-from", "thread":
			continue
		case "blocks", "conditional-blocks", "waits-for", "merge-blocks", "parent-child":
		default:
			return fmt.Errorf("unknown dependency relation for %s", dep.ID)
		}
		// Only the source's own recorded workflow bond may remain open.
		if root && dep.DependencyType == "blocks" && attachment != nil && attachment.AttachedMolecule != "" && dep.ID == attachment.AttachedMolecule {
			continue
		}
		d, err := show(dep.ID)
		if err != nil || d == nil || d.ID != dep.ID {
			return fmt.Errorf("dependency %s unavailable", dep.ID)
		}
		if dep.DependencyType != "parent-child" && d.Status != "closed" {
			return fmt.Errorf("open prerequisite %s", dep.ID)
		}
		if dep.DependencyType == "parent-child" {
			if mqResumeHeld(d) {
				return fmt.Errorf("parent %s requires operator", d.ID)
			}
			if err := validateMQResumeDependencies(d, show, seen, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func mqResumeBeads(townRoot string) *beads.Beads {
	root := filepath.Join(townRoot, "hisn", "mayor", "rig")
	// Keep the stable worktree cwd, but pin the rig's actual database. The
	// resolver follows redirects; it does not search upward from mayor/rig.
	// Prefix-routed Show can mask a bad pin that unprefixed List/Create cannot.
	return beads.NewWithBeadsDir(root, beads.ResolveBeadsDir(filepath.Join(townRoot, "hisn")))
}

func mqResumeGit(root string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c := exec.CommandContext(ctx, "git", args...)
	c.Dir = root
	out, err := c.Output()
	return strings.TrimSpace(string(out)), err
}

type mqResumeWorkerOps struct {
	git         func(string, ...string) (string, error)
	assignments func(string) ([]*beads.Issue, error)
	running     func(string) (bool, error)
}

func validateMQResumeWorker(townRoot string, state *mqResumeState, b *beads.Beads) error {
	return validateMQResumeWorkerWith(townRoot, state, mqResumeWorkerOps{
		git: mqResumeGit,
		assignments: func(actor string) ([]*beads.Issue, error) {
			return b.List(beads.ListOptions{Assignee: actor, Status: "all", Limit: 0, Priority: -1})
		},
		running: tmux.NewTmux().HasSession,
	})
}

func validateMQResumeWorkerWith(townRoot string, state *mqResumeState, ops mqResumeWorkerOps) error {
	r := state.Record
	root := filepath.Join(townRoot, r.Rig, "polecats", r.Worker)
	if stat, err := os.Stat(filepath.Join(root, r.Rig)); err == nil && stat.IsDir() {
		root = filepath.Join(root, r.Rig)
	}
	real, err := filepath.EvalSymlinks(root)
	if err != nil || real != root {
		return fmt.Errorf("original worker worktree missing or redirected")
	}
	checks := []struct {
		args     []string
		expected string
	}{
		{[]string{"rev-parse", "--show-toplevel"}, root},
		{[]string{"branch", "--show-current"}, r.Branch},
		{[]string{"rev-parse", "HEAD"}, r.Submitted},
		{[]string{"status", "--porcelain"}, ""},
	}
	for _, check := range checks {
		value, err := ops.git(root, check.args...)
		if err != nil || value != check.expected {
			return fmt.Errorf("original worker Git state changed (%s)", strings.Join(check.args, " "))
		}
	}
	origin, err := ops.git(root, "remote", "get-url", "origin")
	if err != nil || strings.TrimSuffix(origin, ".git") != "https://github.com/gregorycarter/hisn-core" {
		return fmt.Errorf("foreign worker origin")
	}
	if r.Kind != "dependency-resume" {
		remote, err := ops.git(root, "ls-remote", "--refs", "origin", "refs/heads/"+r.Branch)
		if err != nil || remote != r.Submitted+"\trefs/heads/"+r.Branch {
			return fmt.Errorf("original pushed head changed or unavailable")
		}
	}
	issues, err := ops.assignments(r.Rig + "/polecats/" + r.Worker)
	if err != nil {
		return fmt.Errorf("worker assignments unavailable: %w", err)
	}
	for _, issue := range issues {
		if issue.ID != r.Source && (issue.Status == "hooked" || issue.Status == "in_progress") {
			return fmt.Errorf("original worker has other active work")
		}
	}
	running, err := ops.running(session.PolecatSessionName("hisn", r.Worker))
	if err != nil {
		return fmt.Errorf("worker session state unavailable: %w", err)
	}
	if running {
		return fmt.Errorf("original worker session already exists; use live-worker recovery")
	}
	state.WorkDir = root
	return nil
}

func loadMQResumeState(townRoot, rigName, mrID string) (*mqResumeState, error) {
	if rigName != "hisn" {
		return nil, fmt.Errorf("same-MR recovery is opt-in for hisn only")
	}
	if blocked, reason := IsRigParkedOrDocked(townRoot, rigName); blocked {
		return nil, fmt.Errorf("rig %s", reason)
	}
	b := mqResumeBeads(townRoot)
	state, err := validateMQResumeRecords(rigName, mrID, b.Show)
	if err != nil {
		return nil, err
	}
	if err := validateMQResumeWorker(townRoot, state, b); err != nil {
		return nil, err
	}
	return state, nil
}

func validateMQResumeContext(townRoot string, fields *capacity.SlingContextFields) (*mqResumeState, error) {
	var state *mqResumeState
	var err error
	if fields.ResumeDependency {
		if fields.TargetRig != "hisn" || fields.ResumeMR != "" {
			return nil, fmt.Errorf("invalid dependency recovery context")
		}
		state, err = loadDependencyResume(townRoot, fields.WorkBeadID)
	} else {
		state, err = loadMQResumeState(townRoot, fields.TargetRig, fields.ResumeMR)
	}
	if err != nil {
		return nil, err
	}
	r := state.Record
	expected := &capacity.SlingContextFields{Version: 1, WorkBeadID: r.Source, TargetRig: r.Rig, ResumeDependency: r.Kind == "dependency-resume", ResumeMR: r.MR, ResumeWorker: r.Worker, ResumeBranch: r.Branch, ResumeHead: r.Submitted, ResumeReceipt: state.ReceiptHash}
	if !sameMQResumeContext(fields, expected) {
		return nil, fmt.Errorf("queued recovery identity changed")
	}
	return state, nil
}

func runMQResume(cmd *cobra.Command, args []string) error {
	if err := validateMQResumeCaller(os.Getenv("GT_ROLE"), os.Getenv("GT_RIG"), os.Getenv("BD_ACTOR")); err != nil {
		return err
	}
	townRoot, err := workspace.FindFromCwdOrError()
	if err != nil {
		return err
	}
	max, err := configuredSchedulerMaxPolecats(townRoot)
	if err != nil {
		return err
	}
	if max <= 0 {
		return fmt.Errorf("same-MR recovery requires enabled deferred scheduler")
	}
	state, err := loadMQResumeState(townRoot, args[0], args[1])
	if err != nil {
		return err
	}
	result := mqResumeOutput(state, "ready-to-queue", "")
	if !mqResumeDryRun {
		b := mqResumeBeads(townRoot)
		result, err = queueMQResume(townRoot, state, func() (*mqResumeState, error) { return loadMQResumeState(townRoot, args[0], args[1]) }, b.ListOpenSlingContexts, b.CreateSlingContext)
		if err != nil {
			return err
		}
	}
	if mqResumeJSON {
		return json.NewEncoder(cmd.OutOrStdout()).Encode(result)
	}
	fmt.Fprintf(cmd.OutOrStdout(), "%s: %s on %s/%s (same source %s, context %s; daemon admission required)\n", result.Status, result.MR, result.Rig, result.Worker, result.Source, result.Context)
	return nil
}

func validateMQResumeCaller(role, rigName, actor string) error {
	// Neutral operators remain neutral; don't infer an agent from the cwd.
	if role == "" {
		return nil
	}
	if role == "hisn/refinery" && rigName == "hisn" && actor == "hisn/refinery" {
		return nil
	}
	if role == "mayor" && (actor == "mayor" || actor == "mayor/") {
		return nil
	}
	return fmt.Errorf("same-MR resume requires the Hisn Refinery, Mayor or a neutral operator")
}

func mqResumeOutput(state *mqResumeState, status, contextID string) mqResumeResult {
	r := state.Record
	return mqResumeResult{Status: status, Context: contextID, MR: r.MR, Source: r.Source, Rig: r.Rig, Worker: r.Worker, Branch: r.Branch, Head: r.Submitted}
}

func queueMQResume(townRoot string, initial *mqResumeState, load func() (*mqResumeState, error), list func() ([]*beads.Issue, error), create func(string, string, *capacity.SlingContextFields) (*beads.Issue, error), refresh ...func(string, *capacity.SlingContextFields) error) (mqResumeResult, error) {
	empty := mqResumeResult{}
	release, err := tryAcquireSlingBeadLock(townRoot, initial.Source.ID)
	if err != nil {
		return empty, err
	}
	defer release()
	state, err := load()
	if err != nil {
		return empty, err
	}
	if initial.ReceiptHash != state.ReceiptHash || initial.Record != state.Record {
		return empty, fmt.Errorf("recovery changed during enqueue")
	}
	fields := &capacity.SlingContextFields{Version: 1, WorkBeadID: state.Record.Source, TargetRig: state.Record.Rig, EnqueuedAt: time.Now().UTC().Format(time.RFC3339Nano), ResumeDependency: state.Record.Kind == "dependency-resume", ResumeMR: state.Record.MR, ResumeWorker: state.Record.Worker, ResumeBranch: state.Record.Branch, ResumeHead: state.Record.Submitted, ResumeReceipt: state.ReceiptHash}
	contexts, err := list()
	if err != nil {
		return empty, err
	}
	result := mqResumeOutput(state, "queued", "")
	var matched *beads.Issue
	for _, ctx := range contexts {
		existing := beads.ParseSlingContextFields(ctx.Description)
		if existing == nil || existing.WorkBeadID != fields.WorkBeadID {
			continue
		}
		if matched != nil {
			return empty, fmt.Errorf("duplicate recovery contexts; preserve and inspect")
		}
		matched = ctx
	}
	if matched != nil {
		existing := beads.ParseSlingContextFields(matched.Description)
		if sameMQResumeContext(existing, fields) {
			return mqResumeOutput(state, "already-queued", matched.ID), nil
		}
		// Only refresh a dependency receipt for the exact same preserved work.
		// Never reinterpret an ordinary context, changed HEAD, worker or branch.
		updated := *existing
		updated.ResumeReceipt = fields.ResumeReceipt
		if !existing.ResumeDependency || !fields.ResumeDependency || existing.ResumeMR != "" || len(refresh) != 1 || !sameMQResumeContext(&updated, fields) {
			return empty, fmt.Errorf("existing scheduler context conflicts with exact recovery; preserve and inspect")
		}
		if err := refresh[0](matched.ID, &updated); err != nil {
			return empty, err
		}
		confirmed, err := list()
		if err != nil {
			return empty, err
		}
		for _, ctx := range confirmed {
			if ctx.ID == matched.ID && sameMQResumeContext(beads.ParseSlingContextFields(ctx.Description), &updated) {
				return mqResumeOutput(state, "refreshed", matched.ID), nil
			}
		}
		return empty, fmt.Errorf("recovery refresh unconfirmed")
	}
	created, err := create(state.Source.Title, state.Source.ID, fields)
	if err != nil {
		return empty, err
	}
	if created == nil || created.ID == "" {
		return empty, fmt.Errorf("scheduler context creation unconfirmed")
	}
	// Confirm publication. A failed read leaves the context for inspection,
	// not deletion; retries discover it under this same source lock.
	confirmed, err := list()
	if err != nil {
		return empty, err
	}
	for _, ctx := range confirmed {
		if ctx.ID == created.ID && sameMQResumeContext(beads.ParseSlingContextFields(ctx.Description), fields) {
			result.Context = created.ID
			return result, nil
		}
	}
	return empty, fmt.Errorf("scheduler context publication unconfirmed")
}

func sameMQResumeContext(a, b *capacity.SlingContextFields) bool {
	if a == nil || b == nil {
		return false
	}
	// Only scheduler-owned retry/timing fields may change. Ordinary sling
	// flags must not be smuggled into a recovery context for later fallback.
	actual, expected := *a, *b
	actual.EnqueuedAt, expected.EnqueuedAt = "", ""
	actual.DispatchFailures, expected.DispatchFailures = 0, 0
	actual.LastFailure, expected.LastFailure = "", ""
	actual.LastFailureAt, expected.LastFailureAt = "", ""
	return actual == expected
}

type mqResumeDispatchOps struct {
	load  func(string, *capacity.SlingContextFields) (*mqResumeState, error)
	admit func(string, string, string, string) (*polecatAdmissionHandle, polecatCapacitySnapshot, error)
	start func(string, polecat.SessionStartOptions) error
	hook  func(*mqResumeState) error
}

func dispatchMQResume(townRoot string, fields *capacity.SlingContextFields) (*SlingResult, error) {
	return dispatchMQResumeWith(townRoot, fields, mqResumeDispatchOps{
		load: validateMQResumeContext, admit: acquirePolecatAdmission,
		start: func(worker string, opts polecat.SessionStartOptions) error {
			manager, _, err := getSessionManager(fields.TargetRig)
			if err != nil {
				return err
			}
			return manager.Start(worker, opts)
		},
		hook: func(state *mqResumeState) error {
			actor := fields.TargetRig + "/polecats/" + fields.ResumeWorker
			if err := mqResumeHookCommand(townRoot, state, fields).Run(); err != nil {
				return fmt.Errorf("hook failed: %w", err)
			}
			hooked, err := mqResumeBeads(townRoot).Show(fields.WorkBeadID)
			if err != nil {
				return err
			}
			return verifyMQResumeHook(state.Source, hooked, actor)
		},
	})
}

func mqResumeHookCommand(townRoot string, state *mqResumeState, fields *capacity.SlingContextFields) *bdCmd {
	root := filepath.Join(townRoot, "hisn", "mayor", "rig")
	actor := fields.TargetRig + "/polecats/" + fields.ResumeWorker
	// Dir controls cwd but its implicit pin does not search upward. Match the
	// read/context wrapper's rig authority, including rig .beads redirects.
	return BdCmd("update", fields.WorkBeadID, "--if-status="+state.Source.Status, "--status=hooked", "--assignee="+actor).
		Dir(root).WithBeadsDir(beads.ResolveBeadsDir(filepath.Join(townRoot, "hisn"))).WithAutoCommit()
}

func verifyMQResumeHook(source, hooked *beads.Issue, actor string) error {
	if hooked == nil || hooked.ID != source.ID || hooked.Status != "hooked" || hooked.Assignee != actor || hooked.Description != source.Description || string(hooked.Metadata) != string(source.Metadata) || strings.Join(hooked.Labels, "\x00") != strings.Join(source.Labels, "\x00") {
		return fmt.Errorf("hook write not verified; worker not started")
	}
	return nil
}

func dispatchMQResumeWith(townRoot string, fields *capacity.SlingContextFields, ops mqResumeDispatchOps) (*SlingResult, error) {
	// GT_DAEMON marks the existing daemon pressure-admitted scheduler call.
	// No new CLI escape hatch: an ad-hoc scheduler run cannot wake recoveries.
	if !isDaemonDispatch() {
		return nil, fmt.Errorf("same-MR resume waits for normal daemon pressure admission")
	}
	release, err := tryAcquireSlingBeadLock(townRoot, fields.WorkBeadID)
	if err != nil {
		return nil, err
	}
	defer release()
	actor := fields.TargetRig + "/polecats/" + fields.ResumeWorker
	unlock, err := tryAcquireSlingAssigneeLock(townRoot, actor)
	if err != nil {
		return nil, err
	}
	defer unlock()
	scheduler, err := capacity.LoadState(townRoot)
	if err != nil {
		return nil, err
	}
	if scheduler.Paused {
		return nil, fmt.Errorf("scheduler paused")
	}
	state, err := ops.load(townRoot, fields)
	if err != nil {
		return nil, err
	}
	admission, _, err := ops.admit(townRoot, fields.TargetRig, fields.WorkBeadID, "same-mr-resume")
	if err != nil {
		return nil, err
	}
	defer admission.Release()
	opts := polecat.SessionStartOptions{WorkDir: state.WorkDir, Issue: fields.WorkBeadID, PreserveBranch: fields.ResumeBranch, PreserveHead: fields.ResumeHead, StartupInstructions: state.Record.Instructions}
	opts.BeforeLaunch = func() error {
		freshScheduler, err := capacity.LoadState(townRoot)
		if err != nil {
			return err
		}
		if freshScheduler.Paused {
			return fmt.Errorf("scheduler paused before launch")
		}
		fresh, err := ops.load(townRoot, fields)
		if err != nil {
			return err
		}
		if fresh.Source.Description != state.Source.Description || string(fresh.Source.Metadata) != string(state.Source.Metadata) {
			return fmt.Errorf("source workflow changed before launch")
		}
		return ops.hook(fresh)
	}
	if err := ops.start(fields.ResumeWorker, opts); err != nil {
		return nil, err
	}
	return &SlingResult{BeadID: fields.WorkBeadID, PolecatName: fields.ResumeWorker, Success: true}, nil
}
