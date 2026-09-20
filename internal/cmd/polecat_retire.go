package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/spf13/cobra"
	"github.com/steveyegge/gastown/internal/polecat"
	"github.com/steveyegge/gastown/internal/workspace"
)

func init() {
	var dry, all, jsonOutput bool
	var limit int
	command := &cobra.Command{Use: "retire [rig]", Short: "Retire dormant merged sandboxes and empty completed directories", Args: cobra.MaximumNArgs(1),
		Long: "Release completed polecat directories after checking source closure, target merge proof, sessions, assignments, MRs, dirty files and stashes. Preserve agent identity, branches and test receipts. Never runs wisp GC. Parked rigs are skipped.",
		RunE: func(cmd *cobra.Command, args []string) error {
			if all == (len(args) == 1) {
				return fmt.Errorf("provide one rig or --all")
			}
			if limit < 1 || limit > 100 {
				return fmt.Errorf("limit must be 1..100")
			}
			town, err := workspace.FindFromCwdOrError()
			if err != nil {
				return err
			}
			names := args
			if all {
				rigs, err := getAllRigs()
				if err != nil {
					return err
				}
				for _, r := range rigs {
					names = append(names, r.Name)
				}
			}
			sort.Strings(names)
			results := []polecat.Retirement{}
			retired := 0
			for _, rigName := range names {
				if blocked, reason := IsRigParkedOrDocked(town, rigName); blocked {
					results = append(results, polecat.Retirement{Rig: rigName, Status: "retained", Reason: "rig " + reason})
					continue
				}
				mgr, r, err := getPolecatManager(rigName)
				if err != nil {
					return err
				}
				entries, err := os.ReadDir(filepath.Join(r.Path, "polecats"))
				if os.IsNotExist(err) {
					continue
				}
				if err != nil {
					return err
				}
				for _, entry := range entries {
					if retired >= limit {
						break
					}
					if !entry.IsDir() || strings.HasPrefix(entry.Name(), ".") {
						continue
					}
					row := retirePolecat(town, rigName, entry.Name(), mgr, dry)
					results = append(results, row)
					if row.Status == "retired" || row.Status == "eligible" {
						retired++
					}
				}
			}
			if jsonOutput {
				return json.NewEncoder(cmd.OutOrStdout()).Encode(results)
			}
			for _, r := range results {
				fmt.Fprintf(cmd.OutOrStdout(), "%s/%s: %s %s\n", r.Rig, r.Worker, r.Status, r.Reason)
			}
			return nil
		}}
	command.Flags().BoolVar(&dry, "dry-run", false, "Inspect without deleting or resetting")
	command.Flags().BoolVar(&all, "all", false, "Inspect all active rigs")
	command.Flags().BoolVar(&jsonOutput, "json", false, "Emit retirement receipts as JSON")
	command.Flags().IntVar(&limit, "limit", 2, "Maximum eligible sandboxes per invocation")
	polecatCmd.AddCommand(command)
}

func retirePolecat(town, rigName, name string, mgr *polecat.Manager, dry bool) polecat.Retirement {
	row := polecat.Retirement{Rig: rigName, Worker: name, Status: "retained"}
	unlock, err := tryAcquireSlingAssigneeLock(town, rigName+"/polecats/"+name)
	if err != nil {
		row.Reason = err.Error()
		return row
	}
	defer unlock()
	reservations, err := readPolecatAdmissionReservations(town)
	if err != nil {
		row.Reason = "admission state unavailable: " + err.Error()
		return row
	}
	for _, reservation := range reservations {
		if reservation.Rig == rigName {
			row.Reason = "rig startup reservation in progress"
			return row
		}
	}
	result, err := mgr.RetireMerged(name, dry)
	if err != nil {
		result.Rig = rigName
		result.Worker = name
		result.Status = "retained"
		result.Reason = err.Error()
	}
	return result
}
