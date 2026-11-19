package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/steveyegge/gastown/internal/asyncpreflight"
	"github.com/steveyegge/gastown/internal/beads"
	"github.com/steveyegge/gastown/internal/config"
	"github.com/steveyegge/gastown/internal/style"
	"github.com/steveyegge/gastown/internal/workspace"
)

// kasb-eel8t.6: submit while the preflight runs. See package asyncpreflight
// for the flow. Everything here is inert unless a rig sets
// delivery.submit_while_preflight=true and a polecat passes --async-preflight.

// Exit codes of `gt mq preflight-gate`.
const (
	preflightGateExitPending  = 10
	preflightGateExitNeedsFix = 11
)

var (
	asyncPreflightMarker string
	asyncPreflightDetach bool

	preflightGateApply      bool
	preflightGateBranchHead string
	preflightGateJSON       bool
)

var preflightAsyncCmd = &cobra.Command{
	Use:    "preflight-async",
	Short:  "Detached preflight runner for gt done --async-preflight (internal)",
	Hidden: true,
}

var preflightAsyncRunCmd = &cobra.Command{
	Use:   "run --marker <path>",
	Short: "Run the preflight recorded in an async-preflight marker",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		if asyncPreflightMarker == "" {
			return fmt.Errorf("--marker is required")
		}
		if asyncPreflightDetach {
			return spawnDetachedAsyncPreflight(asyncPreflightMarker)
		}
		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
		defer stop()
		return asyncpreflight.Run(ctx, asyncPreflightMarker, asyncpreflight.DefaultRunDeps())
	},
}

var mqPreflightGateCmd = &cobra.Command{
	Use:   "preflight-gate <rig> <mr-id>",
	Short: "Report whether an async-preflight MR may land (exit 0 land, 10 pending, 11 needs fix)",
	Long: `Report the async preflight state of a merge request (kasb-eel8t.6).

  none     the MR was submitted without --async-preflight: land as before (exit 0)
  passed   a passing receipt for the exact submitted head exists (exit 0)
  pending  the preflight is still running: skip this MR, do not block (exit 10)
  failed   / lost / invalid / moved: not landable (exit 11); with --apply the MR
           is set blocked + needs-repair with a correction_reason, which the
           Kasb same-MR repair sweep returns to the original worker.

Pass --branch-head with the remote branch head the lander would land; a head
other than the preflighted one is "moved".`,
	Args: cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		town, err := workspace.FindFromCwdOrError()
		if err != nil {
			return err
		}
		rigName, mrID := args[0], args[1]
		b := mqResumeBeads(town, rigName)
		root := filepath.Join(town, rigName, "mayor", "rig")
		beadsDir := beads.ResolveBeadsDir(filepath.Join(town, rigName))
		deps := preflightGateDeps{
			show: b.Show,
			block: func(id, reason string) error {
				return BdCmd("update", id, "--if-status=open", "--status=blocked", "--add-label=needs-repair",
					"--set-metadata=correction_reason="+reason).Dir(root).WithBeadsDir(beadsDir).WithAutoCommit().Run()
			},
			comment: b.AddComment,
			now:     time.Now,
		}
		res, applied, err := runPreflightGate(filepath.Join(town, rigName), mrID, preflightGateBranchHead, preflightGateApply, deps)
		if err != nil {
			return err
		}
		if preflightGateJSON {
			out, _ := json.Marshal(struct {
				asyncpreflight.GateResult
				Applied bool `json:"applied"`
			}{res, applied})
			fmt.Fprintln(cmd.OutOrStdout(), string(out))
		} else {
			fmt.Fprintf(cmd.OutOrStdout(), "%s\t%s\n", res.State, res.Reason)
		}
		switch {
		case res.Landable():
			return nil
		case res.State == asyncpreflight.GatePending:
			return NewSilentExit(preflightGateExitPending)
		default:
			return NewSilentExit(preflightGateExitNeedsFix)
		}
	},
}

func init() {
	preflightAsyncRunCmd.Flags().StringVar(&asyncPreflightMarker, "marker", "", "async preflight marker path")
	preflightAsyncRunCmd.Flags().BoolVar(&asyncPreflightDetach, "detach", false, "start the runner in its own session and return")
	preflightAsyncCmd.AddCommand(preflightAsyncRunCmd)
	rootCmd.AddCommand(preflightAsyncCmd)

	mqPreflightGateCmd.Flags().BoolVar(&preflightGateApply, "apply", false, "on failed/lost/invalid/moved: set the open MR blocked + needs-repair with a correction reason")
	mqPreflightGateCmd.Flags().StringVar(&preflightGateBranchHead, "branch-head", "", "remote branch head the lander would land")
	mqPreflightGateCmd.Flags().BoolVar(&preflightGateJSON, "json", false, "JSON output")
	mqCmd.AddCommand(mqPreflightGateCmd)
}

type preflightGateDeps struct {
	show    func(id string) (*beads.Issue, error)
	block   func(id, reason string) error
	comment func(id, text string) error
	now     func() time.Time
}

// runPreflightGate evaluates an MR and, with apply, returns a not-landable
// open MR to its worker as a same-MR repair. Returns whether it applied.
func runPreflightGate(rigPath, mrID, branchHead string, apply bool, d preflightGateDeps) (asyncpreflight.GateResult, bool, error) {
	mr, err := d.show(mrID)
	if err != nil {
		return asyncpreflight.GateResult{}, false, err
	}
	if mr == nil || mr.ID != mrID || !beads.HasLabel(mr, "gt:merge-request") {
		return asyncpreflight.GateResult{}, false, fmt.Errorf("%s is not a merge request", mrID)
	}
	f := asyncpreflight.ParseMRFields(mr.Description)
	res := asyncpreflight.Evaluate(rigPath, f, branchHead, d.now())
	if !apply || !res.NeedsFix() || mr.Status != "open" {
		return res, false, nil
	}
	reason := truncateGateReason("async preflight "+res.State+": "+res.Reason+". Fix it on the SAME branch, run the preflight on the fixed head, push, then gt mq resubmit-recovery and gt done --pre-verified.", 3500)
	if err := d.block(mrID, reason); err != nil {
		return res, false, fmt.Errorf("blocking %s: %w", mrID, err)
	}
	stamp := d.now().UTC().Format(time.RFC3339)
	_ = d.comment(mrID, fmt.Sprintf("preflight-gate %s: %s -> blocked + needs-repair (kasb-eel8t.6). %s", stamp, res.State, res.Reason))
	if f.Source != "" {
		_ = d.comment(f.Source, fmt.Sprintf("preflight-gate %s: MR %s not landable (%s): %s. Same MR, same branch; the repair sweep returns it to the original worker.", stamp, mrID, res.State, res.Reason))
	}
	return res, true, nil
}

func truncateGateReason(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// spawnDetachedAsyncPreflight starts the runner in a new session and returns
// at once. gt done waits only for this intermediate process, so the runner is
// reparented to init: it is not a descendant of the polecat pane (session
// retirement kills descendants) and not in its process group (SIGHUP).
func spawnDetachedAsyncPreflight(markerPath string) error {
	m, err := asyncpreflight.Load(markerPath)
	if err != nil {
		return err
	}
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	logFile, err := os.OpenFile(m.Log, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer logFile.Close()
	devNull, err := os.Open(os.DevNull)
	if err != nil {
		return err
	}
	defer devNull.Close()
	c := exec.Command(exe, "preflight-async", "run", "--marker", markerPath)
	c.Dir = m.Worktree
	c.Stdin, c.Stdout, c.Stderr = devNull, logFile, logFile
	c.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := c.Start(); err != nil {
		return err
	}
	return c.Process.Release()
}

// launchAsyncPreflight is replaced in tests.
var launchAsyncPreflight = func(markerPath string) error {
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	c := exec.Command(exe, "preflight-async", "run", "--marker", markerPath, "--detach")
	out, err := c.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%v: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

// asyncPreflightSubmission is what gt done records on the MR.
type asyncPreflightSubmission struct {
	MarkerPath string
	Reused     bool
}

// asyncPreflightGit is the subset of git gt done needs here.
type asyncPreflightGit interface {
	Rev(ref string) (string, error)
}

// gitOutput runs git in dir and returns trimmed stdout.
func gitOutput(dir string, args ...string) (string, error) {
	c := exec.Command("git", args...)
	c.Dir = dir
	out, err := c.Output()
	return strings.TrimSpace(string(out)), err
}

// remoteBranchHead returns the SHA of origin/<branch> as the remote reports it.
func remoteBranchHead(dir, branch string) (string, error) {
	out, err := gitOutput(dir, "ls-remote", "--refs", "origin", "refs/heads/"+branch)
	if err != nil {
		return "", err
	}
	fields := strings.Fields(out)
	if len(fields) != 2 || fields[1] != "refs/heads/"+branch {
		return "", fmt.Errorf("origin has no single refs/heads/%s", branch)
	}
	return fields[0], nil
}

// startAsyncPreflightForDone validates and starts the detached preflight for
// the pushed head. It runs before any MR exists: an error leaves no MR, and
// the polecat runs the preflight synchronously instead.
func startAsyncPreflightForDone(settings *config.RigSettings, rigPath, rigName, issueID, branch, worktree, autoSavedHead string, g asyncPreflightGit, remoteHead func() (string, error), trackedDirty func() (string, error), preflightArgs []string, now time.Time) (*asyncPreflightSubmission, error) {
	if !settings.SubmitWhilePreflightEnabled() {
		return nil, fmt.Errorf("--async-preflight is disabled for rig %s (settings delivery.submit_while_preflight); run the preflight, then gt done", rigName)
	}
	if autoSavedHead != "" {
		return nil, fmt.Errorf("--async-preflight refuses an auto-saved head %s; commit your work deliberately, push, and run gt done again", shortDoneSHA(autoSavedHead))
	}
	if issueID == "" {
		return nil, fmt.Errorf("--async-preflight needs the source issue (branch or --issue)")
	}
	head, err := g.Rev("HEAD")
	if err != nil {
		return nil, fmt.Errorf("--async-preflight: cannot read HEAD: %w", err)
	}
	if pushed, err := remoteHead(); err != nil || pushed != head {
		return nil, fmt.Errorf("--async-preflight: HEAD %s is not the head of origin/%s (%s %v); push first", shortDoneSHA(head), branch, shortDoneSHA(pushed), err)
	}
	if dirty, err := trackedDirty(); err != nil || strings.TrimSpace(dirty) != "" {
		return nil, fmt.Errorf("--async-preflight needs a clean worktree (tracked changes: %s %v)", strings.TrimSpace(dirty), err)
	}
	m, path, reused, err := asyncpreflight.Prepare(asyncpreflight.StartRequest{
		RigPath: rigPath, Rig: rigName, Source: issueID, Branch: branch, Head: head,
		Worktree: worktree, Prefix: settings.AsyncPreflightCommandOrDefault(), Args: preflightArgs, Now: now,
	})
	if err != nil {
		return nil, err
	}
	if !reused {
		if err := launchAsyncPreflight(path); err != nil {
			_ = os.Remove(path)
			return nil, fmt.Errorf("could not start the detached preflight: %w; run the preflight, then gt done", err)
		}
		fmt.Printf("%s Preflight for %s running detached (pid in %s, log %s)\n", style.Bold.Render("✓"), shortDoneSHA(head), path, m.Log)
	} else {
		fmt.Printf("%s Reusing async preflight %s (%s)\n", style.Bold.Render("✓"), path, m.Status)
	}
	return &asyncPreflightSubmission{MarkerPath: path, Reused: reused}, nil
}

// ensureAsyncPreflightMRFields makes an existing MR (idempotent retry,
// checkpoint, conflict resume) carry the pending preflight fields.
func ensureAsyncPreflightMRFields(show func(string) (*beads.Issue, error), update func(id string, description string) error, mrID, markerPath string) error {
	mr, err := show(mrID)
	if err != nil || mr == nil {
		return fmt.Errorf("reading MR %s: %v", mrID, err)
	}
	if asyncpreflight.ParseMRFields(mr.Description).RunPath == markerPath {
		return nil
	}
	return update(mrID, asyncpreflight.WithDescriptionFields(mr.Description, markerPath))
}
