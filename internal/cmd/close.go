package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"

	beadsdk "github.com/steveyegge/beads"
	"github.com/steveyegge/gastown/internal/beads"
	"github.com/steveyegge/gastown/internal/convoy"
	"github.com/steveyegge/gastown/internal/workspace"

	"github.com/spf13/cobra"
)

var closeCmd = &cobra.Command{
	Use:     "close [bead-id...]",
	GroupID: GroupWork,
	Short:   "Close one or more beads",
	Long: `Close one or more beads (wrapper for 'bd close').

This is a convenience command that passes through to 'bd close' with
all arguments and flags preserved.

When an issue is closed, any convoys tracking it are checked for
completion. If all tracked issues in a convoy are closed, the convoy
is auto-closed.

Examples:
  gt close gt-abc              # Close bead gt-abc
  gt close gt-abc gt-def       # Close multiple beads
  gt close --reason "Done"     # Close with reason
  gt close --comment "Done"    # Same as --reason (alias)
  gt close --force             # Force close pinned beads
  gt close gt-abc --cascade    # Close gt-abc and all its children`,
	DisableFlagParsing: true, // Pass all flags through to bd close
	RunE:               runClose,
}

func init() {
	rootCmd.AddCommand(closeCmd)
}

func runClose(cmd *cobra.Command, args []string) error {
	// Handle --help since DisableFlagParsing bypasses Cobra's help handling
	if helped, err := checkHelpFlag(cmd, args); helped || err != nil {
		return err
	}

	// Extract --cascade flag before passing to bd (gt-only flag)
	cascade, filteredArgs := extractCascadeFlag(args)

	// Convert --comment to --reason (alias support)
	convertedArgs := make([]string, len(filteredArgs))
	for i, arg := range filteredArgs {
		if arg == "--comment" {
			convertedArgs[i] = "--reason"
		} else if strings.HasPrefix(arg, "--comment=") {
			convertedArgs[i] = "--reason=" + strings.TrimPrefix(arg, "--comment=")
		} else {
			convertedArgs[i] = arg
		}
	}

	// If cascade, close children first (depth-first)
	if cascade {
		beadIDs := extractBeadIDs(filteredArgs)
		visited := make(map[string]bool)
		for _, id := range beadIDs {
			if err := closeChildren(id, visited, 0); err != nil {
				return fmt.Errorf("cascade close failed for children of %s: %w", id, err)
			}
		}
	}

	// Build bd close command with all args passed through.
	// Route to the correct rig database via prefix resolution — bd no longer
	// does cross-rig routing internally (removed in beads v0.62). We resolve
	// the bead's prefix to the owning rig's directory and strip BEADS_DIR so
	// bd discovers the database from the working directory.
	bdArgs := append([]string{"close"}, convertedArgs...)
	bdCmd := exec.Command("bd", bdArgs...)
	bdCmd.Stdin = os.Stdin
	bdCmd.Stdout = os.Stdout
	bdCmd.Stderr = os.Stderr
	if beadIDs := extractBeadIDs(convertedArgs); len(beadIDs) > 0 {
		if dir := resolveBeadDir(beadIDs[0]); dir != "" && dir != "." {
			bdCmd.Dir = dir
			bdCmd.Env = filterEnvKey(os.Environ(), "BEADS_DIR")
		}
	}
	if err := bdCmd.Run(); err != nil {
		return err
	}

	// After successful close, check convoy completion for each closed issue.
	// This is best-effort: failures are silently ignored since the daemon's
	// event polling and deacon patrol serve as backup mechanisms.
	beadIDs := extractBeadIDs(filteredArgs)
	if len(beadIDs) > 0 {
		checkConvoyCompletion(beadIDs)
		checkCrossRigDependents(beadIDs)
	}

	return nil
}

// extractCascadeFlag removes --cascade from args and returns whether it was present.
func extractCascadeFlag(args []string) (bool, []string) {
	cascade := false
	var filtered []string
	for _, arg := range args {
		if arg == "--cascade" {
			cascade = true
		} else {
			filtered = append(filtered, arg)
		}
	}
	return cascade, filtered
}

// childBead represents a child bead from bd children --json output.
type childBead struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

// maxCascadeDepth is the maximum recursion depth for cascade close.
// Prevents runaway recursion from dependency cycles or deeply nested hierarchies.
const maxCascadeDepth = 50

// closeChildren recursively closes all open children of a bead (depth-first).
// visited tracks already-processed IDs to prevent cycles. depth guards against
// excessively nested hierarchies.
func closeChildren(parentID string, visited map[string]bool, depth int) error {
	if depth > maxCascadeDepth {
		return fmt.Errorf("cascade depth limit (%d) exceeded at %s — possible cycle", maxCascadeDepth, parentID)
	}
	if visited[parentID] {
		return nil // already processed — cycle detected, skip silently
	}
	visited[parentID] = true

	// Query children via bd children --json.
	// Route to the correct rig database via prefix resolution.
	childCmd := exec.Command("bd", "children", parentID, "--json")
	if dir := resolveBeadDir(parentID); dir != "" && dir != "." {
		childCmd.Dir = dir
		childCmd.Env = filterEnvKey(os.Environ(), "BEADS_DIR")
	}
	out, err := childCmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() != 0 {
			fmt.Fprintf(os.Stderr, "Warning: bd children %s failed: %v\n", parentID, err)
		}
		return nil
	}

	var children []childBead
	if err := json.Unmarshal(out, &children); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to parse children of %s: %v\n", parentID, err)
		return nil
	}

	if len(children) == 0 {
		return nil
	}

	// Collect open children and recursively close their children first (depth-first)
	var childIDs []string
	for _, child := range children {
		if child.Status == "closed" {
			continue
		}
		if err := closeChildren(child.ID, visited, depth+1); err != nil {
			return err
		}
		childIDs = append(childIDs, child.ID)
	}

	if len(childIDs) == 0 {
		return nil
	}

	reason := fmt.Sprintf("Parent %s closed (cascade)", parentID)

	closeArgs := []string{"close"}
	closeArgs = append(closeArgs, childIDs...)
	closeArgs = append(closeArgs, "--reason", reason, "--force")

	fmt.Fprintf(os.Stderr, "Cascade: closing %d children of %s\n", len(childIDs), parentID)

	closeBd := exec.Command("bd", closeArgs...)
	closeBd.Stdout = os.Stdout
	closeBd.Stderr = os.Stderr
	if dir := resolveBeadDir(parentID); dir != "" && dir != "." {
		closeBd.Dir = dir
		closeBd.Env = filterEnvKey(os.Environ(), "BEADS_DIR")
	}
	return closeBd.Run()
}

// extractBeadIDs extracts bead IDs from raw args, skipping flags and flag values.
// Since DisableFlagParsing is true, we get unparsed args and must identify flags manually.
func extractBeadIDs(args []string) []string {
	// Flags that consume a following argument (value flags without = form)
	valueFlags := map[string]bool{
		"--reason": true, "-r": true,
		"--session": true,
		"--actor": true,
		"--db": true,
		"--dolt-auto-commit": true,
		// Also handle the --comment alias (before conversion)
		"--comment": true,
	}

	var ids []string
	skipNext := false
	for _, arg := range args {
		if skipNext {
			skipNext = false
			continue
		}
		if strings.HasPrefix(arg, "-") {
			// Check for --flag=value (consumed in one token)
			if strings.Contains(arg, "=") {
				continue
			}
			// Check if this flag takes a value argument
			if valueFlags[arg] {
				skipNext = true
			}
			continue
		}
		ids = append(ids, arg)
	}
	return ids
}

// depResult holds a dependent issue ID and optional title.
type depResult struct {
	id    string
	title string
}

// checkConvoyCompletion checks if any closed issues are tracked by convoys
// and triggers convoy completion checks. This implements the ZFC principle:
// the closure event propagates at the source (bd close) rather than relying
// solely on daemon event polling.
//
// This is best-effort. If the workspace or hq store is unavailable, the
// daemon's event polling and deacon patrol serve as backup mechanisms.
func checkConvoyCompletion(beadIDs []string) {
	townRoot, err := workspace.FindFromCwd()
	if err != nil || townRoot == "" {
		return
	}

	hqBeadsDir := filepath.Join(townRoot, ".beads")
	ctx := context.Background()

	store, err := beadsdk.Open(ctx, hqBeadsDir)
	if err != nil {
		return
	}
	defer func() { _ = store.Close() }()

	gtPath, err := os.Executable()
	if err != nil {
		gtPath, _ = exec.LookPath("gt")
		if gtPath == "" {
			return
		}
	}

	for _, beadID := range beadIDs {
		convoy.CheckConvoysForIssue(ctx, store, townRoot, beadID, "Close", nil, gtPath, nil)
	}
}

// checkCrossRigDependents checks if any closed issues were blocking issues in
// other rigs, and notifies the affected rig's witness. This is best-effort:
// failures are silently ignored.
func checkCrossRigDependents(beadIDs []string) {
	townRoot, err := workspace.FindFromCwd()
	if err != nil || townRoot == "" {
		return
	}

	// Discover all database paths (town + rigs) from routes.
	var dbPaths []string
	hqBeadsDir := filepath.Join(townRoot, ".beads")
	if routes, err := beads.LoadRoutes(hqBeadsDir); err == nil {
		for _, r := range routes {
			if r.Path == "." {
				dbPaths = append(dbPaths, townRoot)
			} else {
				dbPaths = append(dbPaths, filepath.Join(townRoot, r.Path))
			}
		}
	}
	if len(dbPaths) == 0 {
		// Fallback: at least try town root
		dbPaths = append(dbPaths, townRoot)
	}

	for _, closedID := range beadIDs {
		closedPrefix := beads.ExtractPrefix(closedID)

		// Query all databases in parallel for issues blocked by closedID.
		resultChan := make(chan []depResult, len(dbPaths))
		var wg sync.WaitGroup

		for _, dbPath := range dbPaths {
			wg.Add(1)
			go func(path string) {
				defer wg.Done()
				deps := queryBlockedDependents(path, closedID)
				resultChan <- deps
			}(dbPath)
		}

		go func() {
			wg.Wait()
			close(resultChan)
		}()

		// Collect cross-rig dependents.
		crossRig := make(map[string][]depResult) // rig -> dependents
		for batch := range resultChan {
			for _, d := range batch {
				depPrefix := beads.ExtractPrefix(d.id)
				if depPrefix == "" || depPrefix == closedPrefix {
					continue
				}
				rig := beads.GetRigNameForPrefix(townRoot, depPrefix)
				if rig == "" {
					continue
				}
				crossRig[rig] = append(crossRig[rig], d)
			}
		}

		if len(crossRig) == 0 {
			continue
		}

		// Fetch fresh titles for any missing ones.
		var allDepIDs []string
		for _, deps := range crossRig {
			for _, d := range deps {
				allDepIDs = append(allDepIDs, d.id)
			}
		}
		detailsMap := getIssueDetailsBatch(allDepIDs)

		// Notify each affected rig once, listing all unblocked issues.
		for rig, deps := range crossRig {
			var lines []string
			for _, d := range deps {
				title := d.title
				if title == "" {
					if det, ok := detailsMap[d.id]; ok {
						title = det.Title
					}
				}
				if title != "" {
					lines = append(lines, fmt.Sprintf("  %s (%s)", d.id, title))
				} else {
					lines = append(lines, fmt.Sprintf("  %s", d.id))
				}
			}

			addr := rig + "/witness"
			subject := fmt.Sprintf("Dependency resolved: %s", closedID)
			body := fmt.Sprintf("External dependency %s has closed.\nUnblocked:\n%s\nThis issue may now proceed.", closedID, strings.Join(lines, "\n"))

			mailCmd := exec.Command("gt", "mail", "send", addr, "-s", subject, "-m", body)
			_ = mailCmd.Run() // best-effort
		}
	}
}

// queryBlockedDependents runs `bd dep list` in the given database path to find
// issues that are blocked by the given closed issue. Returns a slice of
// depResult values; title may be empty if bd dep list doesn't include it.
func queryBlockedDependents(dbPath, closedID string) []depResult {
	var result []depResult

	cmd := exec.Command("bd", "dep", "list", closedID, "--direction=up", "--type=blocks", "--json")
	cmd.Dir = dbPath
	cmd.Env = filterEnvKey(os.Environ(), "BEADS_DIR")
	var stdout bytes.Buffer
	cmd.Stdout = &stdout

	if err := cmd.Run(); err != nil {
		return result
	}

	var deps []struct {
		ID    string `json:"id"`
		Title string `json:"title"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &deps); err != nil {
		return result
	}

	for _, d := range deps {
		id := beads.ExtractIssueID(d.ID)
		if id != "" {
			result = append(result, depResult{id: id, title: d.Title})
		}
	}
	return result
}
