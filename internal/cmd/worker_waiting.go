package cmd

// Reconcile durable completed-turn evidence, never quiet terminal output. This
// opt-in recovery path preserves source and ownership; it does not submit or reset it.
import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/steveyegge/gastown/internal/beads"
	"github.com/steveyegge/gastown/internal/session"
	"github.com/steveyegge/gastown/internal/tmux"
	"github.com/steveyegge/gastown/internal/workspace"
)

type waitingEvidence struct {
	Worker, Source, Branch, Head, Rollout, Preflight string
	Completed, Launched                              time.Time
	Continuation                                     bool
}

func completedWorkerTurn(data []byte, launched, now time.Time) (time.Time, error) {
	var completed time.Time
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 4096), 4*1024*1024)
	for scanner.Scan() {
		var row struct {
			Timestamp time.Time
			Type      string
			Payload   struct{ Type string }
		}
		if err := json.Unmarshal(scanner.Bytes(), &row); err != nil {
			return time.Time{}, err
		}
		if row.Type == "event_msg" {
			switch row.Payload.Type {
			case "task_complete":
				completed = row.Timestamp
			case "token_count":
			default:
				completed = time.Time{}
			}
		} else if row.Type == "response_item" || row.Type == "turn_context" {
			completed = time.Time{}
		}
	}
	if scanner.Err() != nil {
		return time.Time{}, scanner.Err()
	}
	if completed.Before(launched) || now.Sub(completed) < 2*time.Minute {
		return time.Time{}, fmt.Errorf("no settled completed turn")
	}
	return completed, nil
}

func waitingTail(file string) ([]byte, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	offset := info.Size() - 8*1024*1024
	if offset > 0 {
		if _, err = f.Seek(offset, io.SeekStart); err != nil {
			return nil, err
		}
	}
	data, err := io.ReadAll(io.LimitReader(f, 8*1024*1024+1))
	if err != nil {
		return nil, err
	}
	if offset > 0 {
		i := bytes.IndexByte(data, '\n')
		if i < 0 {
			return nil, fmt.Errorf("unbounded rollout row")
		}
		data = data[i+1:]
	}
	return data, nil
}

func onlyWorkerRuntime(processes []byte, root string) bool {
	children := map[string][]string{}
	names := map[string]string{}
	for _, line := range strings.Split(string(processes), "\n") {
		f := strings.Fields(line)
		if len(f) >= 3 {
			children[f[1]] = append(children[f[1]], f[0])
			names[f[0]] = filepath.Base(f[2])
		}
	}
	if names[root] != "codex" {
		return false
	}
	queue := []string{root}
	seen := map[string]bool{}
	for len(queue) > 0 {
		p := queue[0]
		queue = queue[1:]
		if seen[p] {
			return false
		}
		seen[p] = true
		for _, child := range children[p] {
			if names[child] != "codex" {
				return false
			}
			queue = append(queue, child)
		}
	}
	return true
}

func inspectWaitingWorker(town string, source *beads.Issue) (*waitingEvidence, error) {
	spec, specErr := recoveryRigForSource(source.ID)
	if specErr != nil {
		return nil, specErr
	}
	rigName := spec.Name
	worker := strings.TrimPrefix(source.Assignee, rigName+"/polecats/")
	if worker == source.Assignee || !workerNamePattern.MatchString(worker) || (source.Status != "in_progress" && source.Status != "hooked") {
		return nil, fmt.Errorf("not active worker ownership")
	}
	root := filepath.Join(town, rigName, "polecats", worker, rigName)
	home := filepath.Join(town, ".runtime", "codex-homes", rigName, "polecats", worker)
	var launch struct {
		Timestamp        time.Time
		PID              int
		Actor, Role, Cwd string
	}
	raw, err := os.ReadFile(filepath.Join(home, "launch.json"))
	if err != nil {
		return nil, err
	}
	if err = json.Unmarshal(raw, &launch); err != nil {
		return nil, err
	}
	if launch.Actor != source.Assignee || launch.Role != "polecat" || launch.Cwd != root {
		return nil, fmt.Errorf("runtime ownership mismatch")
	}
	t := tmux.NewTmux()
	sid := session.PolecatSessionName(spec.Prefix, worker)
	info, err := t.GetSessionInfo(sid)
	if err != nil {
		return nil, err
	}
	if info.Attached {
		return nil, fmt.Errorf("attached operator session")
	}
	pid, err := t.GetPanePID(sid)
	if err != nil || strings.TrimSpace(pid) != strconv.Itoa(launch.PID) {
		return nil, fmt.Errorf("launch process changed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	processes, err := exec.CommandContext(ctx, "ps", "-axo", "pid=,ppid=,comm=").Output()
	if err != nil || !onlyWorkerRuntime(processes, strings.TrimSpace(pid)) {
		return nil, fmt.Errorf("active or unknown child work")
	}
	// Docker jobs can outlive their host caller. Protect all local Jest fixtures
	// while any is active, as well as any other container mounting this checkout.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	containers, err := exec.CommandContext(ctx2, "docker", "ps", "--format", "{{.Names}} {{.Mounts}}").Output()
	if err != nil {
		return nil, err
	}
	for _, line := range strings.Split(string(containers), "\n") {
		if strings.HasPrefix(line, spec.Prefix+"-jest-") || strings.HasPrefix(line, spec.Prefix+"-unit-pg-") || strings.Contains(line, root) {
			return nil, fmt.Errorf("test or checkout container active")
		}
	}
	branch, err := mqResumeGit(root, "branch", "--show-current")
	if err != nil {
		return nil, err
	}
	match := mqResumeBranch.FindStringSubmatch(branch)
	if match == nil || match[1] != worker || match[2] != source.ID {
		return nil, fmt.Errorf("branch ownership changed")
	}
	head, err := mqResumeGit(root, "rev-parse", "HEAD")
	if err != nil {
		return nil, err
	}
	var rollout string
	var modified time.Time
	err = filepath.WalkDir(filepath.Join(home, "sessions"), func(p string, d os.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if !d.IsDir() && strings.HasSuffix(p, ".jsonl") {
			i, e := d.Info()
			if e != nil {
				return e
			}
			if i.ModTime().After(modified) {
				modified = i.ModTime()
				rollout = p
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	data, err := waitingTail(rollout)
	if err != nil {
		return nil, err
	}
	completed, err := completedWorkerTurn(data, launch.Timestamp, time.Now())
	if err != nil {
		return nil, err
	}
	files, _ := filepath.Glob(filepath.Join(root, ".delivery", "preflight", head+"-*.json"))
	if rigName == "kasb_core" {
		files, _ = filepath.Glob(filepath.Join(root, ".delivery", "preflight-receipts", "*.json"))
	}
	var receipt string
	var latest time.Time
	var passed, exactReceipt, unreadableReceipt bool
	for _, p := range files {
		var r struct {
			Head, Bead, Status string
			HeadSHA            string    `json:"head_sha"`
			SourceBead         string    `json:"source_bead_id"`
			FinishedAt         time.Time `json:"finished_at"`
			Commands           []struct {
				FinishedAt time.Time `json:"finished_at"`
			} `json:"commands"`
			CompletedAt time.Time
		}
		raw, e := os.ReadFile(p)
		if e != nil {
			unreadableReceipt = true
			continue
		}
		if json.Unmarshal(raw, &r) != nil {
			unreadableReceipt = true
			continue
		}
		if rigName == "kasb_core" {
			// Unknown/incomplete exact-head receipts also protect existing work.
			if r.HeadSHA == head && r.SourceBead == source.ID {
				exactReceipt = true
			}
			if r.Status != "failed" && r.Status != "failed_red_probe" && r.Status != "passed" && r.Status != "passed_non_behavioral" {
				continue
			}
			r.Head, r.Bead, r.CompletedAt = r.HeadSHA, r.SourceBead, r.FinishedAt
			for _, command := range r.Commands {
				if command.FinishedAt.After(r.CompletedAt) {
					r.CompletedAt = command.FinishedAt
				}
			}
		}
		if r.Head == head && r.Bead == source.ID && r.CompletedAt.After(latest) {
			latest = r.CompletedAt
			receipt = p
			passed = r.Status == "passed" || r.Status == "passed_non_behavioral"
		}
	}
	if rigName == "kasb_core" && !exactReceipt && !unreadableReceipt {
		return &waitingEvidence{Worker: worker, Source: source.ID, Branch: branch, Head: head, Rollout: rollout, Completed: completed, Launched: launch.Timestamp, Continuation: true}, nil
	}
	if receipt == "" || passed || latest.After(completed) || latest.Before(launch.Timestamp) {
		return nil, fmt.Errorf("no failed exact-head proof in completed turn")
	}
	return &waitingEvidence{Worker: worker, Source: source.ID, Branch: branch, Head: head, Rollout: rollout, Preflight: receipt, Completed: completed}, nil
}

func reconcileWaitingWorkers(town string, dry bool, rigs ...string) ([]waitingEvidence, error) {
	rigName := recoveryName(rigs)
	if _, err := recoveryRig(rigName); err != nil {
		return nil, err
	}
	result := []waitingEvidence{}
	if parked, _ := IsRigParkedOrDocked(town, rigName); parked {
		return result, nil
	}
	b := mqResumeBeads(town, rigName)
	for _, status := range []string{"in_progress", "hooked"} {
		sources, err := b.List(beads.ListOptions{Status: status, Limit: 0, Priority: -1})
		if err != nil {
			return result, err
		}
		for _, source := range sources {
			evidence, err := inspectWaitingWorker(town, source)
			if err != nil {
				continue
			}
			if evidence.Continuation {
				if err := validateWorkerContinuation(town, source, evidence, b); err != nil {
					continue
				}
			}
			if dry {
				result = append(result, *evidence)
				continue
			}
			unlock, err := tryAcquireSlingAssigneeLock(town, source.Assignee)
			if err != nil {
				continue
			}
			func() {
				defer unlock()
				fresh, err := b.Show(source.ID)
				if err != nil {
					return
				}
				again, err := inspectWaitingWorker(town, fresh)
				if err != nil || *again != *evidence {
					return
				}
				if evidence.Continuation {
					if err := validateWorkerContinuation(town, fresh, again, b); err != nil {
						return
					}
					stamp := filepath.Join(town, ".runtime", "worker-continuations", rigName, evidence.Worker+".json")
					err := continueWorkerOnce(stamp, *again, func() error {
						current, err := b.Show(source.ID)
						if err != nil || validateWorkerContinuation(town, current, again, b) != nil {
							return fmt.Errorf("custody changed before continuation")
						}
						last, err := inspectWaitingWorker(town, current)
						if err != nil || *last != *again {
							return fmt.Errorf("worker changed before continuation")
						}
						message := fmt.Sprintf("Continue your unfinished hooked work %s on the SAME branch %s. The last native turn completed without an exact-head preflight receipt or submitted MR. Check current custody, then continue implementation and normal delivery once. Preserve existing tests and proof; do not restart or duplicate a preflight, smoke, workflow, branch or MR. If a real prerequisite blocks progress, record its exact disposition on the source.", source.ID, evidence.Branch)
						sid := session.PolecatSessionName("kasb", evidence.Worker)
						return tmux.NewTmux().NudgeSessionWithOpts(sid, message, tmux.NudgeOpts{SkipEscape: true, TownRoot: town})
					})
					if err != nil {
						fmt.Fprintf(os.Stderr, "Worker continuation %s: %v\n", source.ID, err)
						return
					}
					result = append(result, *evidence)
					return
				}
				root := filepath.Join(town, rigName, "polecats", evidence.Worker, rigName)
				// A failed preflight is an explicit hold, not permission to resling the
				// same completed prerequisites. Retain exact evidence for disposition.
				note := fmt.Sprintf("Completed worker turn waiting after failed preflight. Preserved branch %s at %s. Proof: %s; runtime: %s. Resolve failure and remove needs-review before recovery.", evidence.Branch, evidence.Head, evidence.Preflight, evidence.Rollout)
				if err = BdCmd("update", source.ID, "--if-status="+fresh.Status, "--if-assignee="+fresh.Assignee, "--add-label=needs-review", "--append-notes="+note).Dir(root).WithBeadsDir(beads.ResolveBeadsDir(filepath.Join(town, rigName))).WithAutoCommit().Run(); err != nil {
					return
				}
				if err = checkpointBlockedDone(town, root, fresh.Assignee, source.ID, evidence.Branch, ExitDeferred); err != nil {
					return
				}
				// Recheck native runtime and subprocess evidence after durable writes.
				last, err := inspectWaitingWorker(town, fresh)
				if err != nil || *last != *evidence {
					return
				}
				manager, _, err := getSessionManager(rigName)
				if err != nil {
					return
				}
				if err = manager.Stop(evidence.Worker, false); err != nil {
					return
				}
				result = append(result, *evidence)
				fmt.Fprintf(os.Stderr, "Preserved waiting worker %s: %s failed; execution released\n", evidence.Worker, source.ID)
			}()
		}
	}
	return result, nil
}

func init() {
	var dry bool
	c := &cobra.Command{Use: "reconcile-waiting <rig>", Short: "Recover settled unfinished Kasb turns or preserve failed-preflight workers", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		if _, err := recoveryRig(args[0]); err != nil {
			return err
		}
		if err := validateMQResumeCaller(os.Getenv("GT_ROLE"), os.Getenv("GT_RIG"), os.Getenv("BD_ACTOR")); err != nil {
			return err
		}
		town, err := workspace.FindFromCwdOrError()
		if err != nil {
			return err
		}
		rows, err := reconcileWaitingWorkers(town, dry, args[0])
		if err != nil {
			return err
		}
		return json.NewEncoder(cmd.OutOrStdout()).Encode(rows)
	}}
	c.Flags().BoolVar(&dry, "dry-run", false, "Inspect without changing source or sessions")
	mqCmd.AddCommand(c)
}
