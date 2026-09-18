package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	"github.com/steveyegge/gastown/internal/beads"
	"github.com/steveyegge/gastown/internal/config"
	"github.com/steveyegge/gastown/internal/rig"
	"github.com/steveyegge/gastown/internal/workspace"
	"io"
	"path/filepath"
)

func init() {
	rigCmd.AddCommand(&cobra.Command{
		Use: "repair-agents <rig>", Short: "Create missing registered-rig lifecycle identities without patrol or cleanup",
		Long: "Repair only missing Witness/Refinery identities for one registered rig in HQ. Existing identities are validated, never reset or reopened. Does not start/stop sessions, run doctor, GC, or modify another rig.",
		Args: cobra.ExactArgs(1), RunE: runRigRepairAgents,
	})
}

func runRigRepairAgents(cmd *cobra.Command, args []string) error {
	town, err := workspace.FindFromCwdOrError()
	if err != nil {
		return err
	}
	name := args[0]
	registry, err := config.LoadRigsConfig(filepath.Join(town, "mayor", "rigs.json"))
	if err != nil {
		return err
	}
	if _, ok := registry.Rigs[name]; !ok {
		return fmt.Errorf("rig %q is not registered", name)
	}
	if filepath.Base(name) != name || name == "." || name == ".." {
		return fmt.Errorf("invalid rig name")
	}
	cfg, err := rig.LoadRigConfig(filepath.Join(town, name))
	if err != nil {
		return err
	}
	if cfg.Beads == nil || cfg.Beads.Prefix == "" {
		return fmt.Errorf("registered rig has no beads prefix")
	}
	db := beads.New(town).ForAgentBead()
	return repairRigLifecycleIdentities(db, cfg.Beads.Prefix, name, cmd.OutOrStdout())
}

type rigIdentityStore interface {
	GetAgentBead(string) (*beads.Issue, *beads.AgentFields, error)
	CreateAgentBead(string, string, *beads.AgentFields) (*beads.Issue, error)
}

func repairRigLifecycleIdentities(db rigIdentityStore, prefix, name string, out io.Writer) error {
	for _, item := range []struct{ id, role string }{
		{beads.WitnessBeadIDWithPrefix(prefix, name), "witness"},
		{beads.RefineryBeadIDWithPrefix(prefix, name), "refinery"},
	} {
		issue, fields, getErr := db.GetAgentBead(item.id)
		if getErr != nil {
			return fmt.Errorf("inspect %s: %w", item.id, getErr)
		}
		// GetAgentBead represents a missing identity as (nil, nil, nil).
		// An error is an unavailable/invalid store, never permission to create.
		if issue != nil {
			if fields == nil || fields.RoleType != item.role || fields.Rig != name || issue.Status != "open" {
				return fmt.Errorf("existing identity %s is not the expected open role; refusing overwrite", item.id)
			}
			fmt.Fprintf(out, "unchanged %s\n", item.id)
			continue
		}
		if _, err := db.CreateAgentBead(item.id, fmt.Sprintf("%s for %s", item.role, name), &beads.AgentFields{RoleType: item.role, Rig: name, AgentState: "idle"}); err != nil {
			return fmt.Errorf("repair %s: %w", item.id, err)
		}
		_, verifiedFields, verifyErr := db.GetAgentBead(item.id)
		if verifyErr != nil || verifiedFields == nil || verifiedFields.RoleType != item.role || verifiedFields.Rig != name {
			return fmt.Errorf("created identity %s could not be verified", item.id)
		}
		fmt.Fprintf(out, "created %s in HQ\n", item.id)
	}
	return nil
}
