package doctor

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"github.com/steveyegge/gastown/internal/beads"
	"github.com/steveyegge/gastown/internal/config"
)

// AgentBeadsTownResolvableCheck verifies that every agent bead resolves through
// the town-routed path from a rig working directory.
//
// AgentBeadsCheck unions the town database with every rig database into one map
// before checking existence, so a bead that lives ONLY in a rig database passes
// — even though agent beads are town-owned and every writer routes through
// ForAgentBead(). That union hides exactly the split-brain that breaks
// heartbeat, hook-clearing and agent-state updates issued from a rig cwd.
//
// This check is deliberately report-only (StatusWarning): moving beads between
// databases is not something a doctor pass should do unattended.
type AgentBeadsTownResolvableCheck struct {
	BaseCheck
}

// NewAgentBeadsTownResolvableCheck creates the town-resolvability check.
func NewAgentBeadsTownResolvableCheck() *AgentBeadsTownResolvableCheck {
	return &AgentBeadsTownResolvableCheck{
		BaseCheck: BaseCheck{
			CheckName:        "agent-beads-town-resolvable",
			CheckDescription: "Verify agent beads resolve through the town database from a rig cwd",
			CheckCategory:    CategoryRig,
		},
	}
}

// Run checks town resolvability and reports rig-side duplicates.
func (c *AgentBeadsTownResolvableCheck) Run(ctx *CheckContext) *CheckResult {
	result := &CheckResult{
		Name:     c.Name(),
		Status:   StatusOK,
		Category: c.Category(),
	}

	townBd := beads.New(beads.GetTownBeadsPath(ctx.TownRoot)).ForAgentBead()
	townBeads, err := townBd.ListAgentBeads()
	if err != nil {
		result.Status = StatusWarning
		result.Message = fmt.Sprintf("could not list town agent beads: %v", err)
		return result
	}

	rigNames := []string{ctx.RigName}
	if ctx.RigName == "" {
		rigNames = formulaCheckRigNames(ctx.TownRoot)
	}

	var missingInTown []string
	var splitBrain []string
	checked := 0

	// Town-level singletons.
	for _, id := range []string{beads.MayorBeadIDTown(), beads.DeaconBeadIDTown()} {
		checked++
		if _, ok := townBeads[id]; !ok {
			missingInTown = append(missingInTown, id)
		}
	}

	for _, rigName := range rigNames {
		if rigName == "" {
			continue
		}
		prefix := config.GetRigPrefix(ctx.TownRoot, rigName)
		ids := []string{
			beads.WitnessBeadIDWithPrefix(prefix, rigName),
			beads.RefineryBeadIDWithPrefix(prefix, rigName),
		}

		// A rig-rooted client must resolve the same beads: this is what proves
		// the agentBeadTarget redirect works from a rig cwd, which is where
		// gt done, gt prime and the await-* steps actually run.
		rigBd := beads.New(filepath.Join(ctx.TownRoot, rigName, ".beads"))
		rigRouted, rigRoutedErr := rigBd.ForAgentBead().ListAgentBeads()
		rigLocal, _ := rigBd.ListAgentBeadsFromWisps()

		for _, id := range ids {
			checked++
			if _, ok := townBeads[id]; !ok {
				missingInTown = append(missingInTown, id)
				continue
			}
			if rigRoutedErr == nil {
				if _, ok := rigRouted[id]; !ok {
					splitBrain = append(splitBrain,
						fmt.Sprintf("%s: present in the town DB but not resolvable from %s/", id, rigName))
					continue
				}
			}
			if _, dup := rigLocal[id]; dup {
				splitBrain = append(splitBrain,
					fmt.Sprintf("%s: also exists in the %s rig database (split brain — writers may update either copy)", id, rigName))
			}
		}
	}

	sort.Strings(missingInTown)
	sort.Strings(splitBrain)

	if len(missingInTown) == 0 && len(splitBrain) == 0 {
		result.Message = fmt.Sprintf("%d agent bead(s) resolve through the town database", checked)
		return result
	}

	result.Status = StatusWarning
	var parts []string
	if len(missingInTown) > 0 {
		parts = append(parts, fmt.Sprintf("%d not in the town DB", len(missingInTown)))
		for _, id := range missingInTown {
			result.Details = append(result.Details, "missing from town DB: "+id)
		}
	}
	if len(splitBrain) > 0 {
		parts = append(parts, fmt.Sprintf("%d split-brain", len(splitBrain)))
		result.Details = append(result.Details, splitBrain...)
	}
	result.Message = strings.Join(parts, ", ")
	result.FixHint = "Agent beads are town-owned (gt:agent). Recreate the missing ones with gt rig add / gt crew add, and remove rig-side duplicates so every writer updates one copy"
	return result
}
