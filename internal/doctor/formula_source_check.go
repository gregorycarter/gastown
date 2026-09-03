package doctor

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/steveyegge/gastown/internal/config"
)

// FormulaSourceCheck warns when the same formula exists in more than one
// resolution tier with different content.
//
// Two independent resolvers read formulas from two different places: gt's
// formula.ResolveFormulaContent walks <town>/<rig>/.beads/formulas then
// <town>/.beads/formulas, while `bd cook` / `bd formula show` run with the rig
// working directory and follow the rig's .beads redirect — which can point at
// a third directory entirely. A fix committed to one copy is silently inert in
// the other, which is why several 2026-09 patrol-formula fixes never took
// effect. This check makes the divergence visible.
type FormulaSourceCheck struct {
	BaseCheck
}

// NewFormulaSourceCheck creates a new formula source divergence check.
func NewFormulaSourceCheck() *FormulaSourceCheck {
	return &FormulaSourceCheck{
		BaseCheck: BaseCheck{
			CheckName:        "formula-source",
			CheckDescription: "Detect formulas that differ between resolution tiers",
			CheckCategory:    CategoryConfig,
		},
	}
}

// formulaTier is one directory a formula may be resolved from.
type formulaTier struct {
	label string
	dir   string
}

// formulaVersionRe extracts a `version = N` line so the report can name the
// versions rather than just "they differ".
var formulaVersionRe = regexp.MustCompile(`(?m)^\s*version\s*=\s*"?(\d+)"?`)

// formulaVersion returns the declared version of a formula file, or "?" when
// the file declares none.
func formulaVersion(content []byte) string {
	if m := formulaVersionRe.FindSubmatch(content); len(m) == 2 {
		return string(m[1])
	}
	return "?"
}

// formulaDigest returns a short content hash for comparison.
func formulaDigest(content []byte) string {
	sum := sha256.Sum256(content)
	return hex.EncodeToString(sum[:])[:12]
}

// formulaTiersFor builds the ordered list of directories a formula may come
// from for a given town (and optional rig).
func formulaTiersFor(townRoot, rigName string) []formulaTier {
	var tiers []formulaTier

	if pinned := config.FormulasDir(townRoot); pinned != "" {
		tiers = append(tiers, formulaTier{label: "pinned (workflow.formulas_dir)", dir: pinned})
	}
	if rigName != "" {
		tiers = append(tiers, formulaTier{
			label: "rig " + rigName,
			dir:   filepath.Join(townRoot, rigName, ".beads", "formulas"),
		})
		// bd follows the rig's .beads redirect; that target is what `bd cook`
		// actually reads, and it is frequently a different directory.
		if redirect := resolveBeadsRedirect(filepath.Join(townRoot, rigName, ".beads")); redirect != "" {
			tiers = append(tiers, formulaTier{
				label: "rig " + rigName + " (redirect)",
				dir:   filepath.Join(redirect, "formulas"),
			})
		}
	}
	tiers = append(tiers, formulaTier{label: "town", dir: filepath.Join(townRoot, ".beads", "formulas")})

	// Drop duplicate directories (a redirect may point back at the town).
	seen := make(map[string]bool, len(tiers))
	unique := tiers[:0]
	for _, tier := range tiers {
		abs, err := filepath.Abs(tier.dir)
		if err != nil {
			abs = tier.dir
		}
		if seen[abs] {
			continue
		}
		seen[abs] = true
		unique = append(unique, tier)
	}
	return unique
}

// redirectChainMaxDepth bounds redirect following, matching beads.ResolveBeadsDir.
const redirectChainMaxDepth = 3

// resolveBeadsRedirect reads a .beads/redirect file and returns the absolute
// directory it points at, or "" when there is no redirect. This is the
// directory `bd cook` actually reads formulas from when run with the rig as
// its working directory.
//
// Redirect targets are relative to the PARENT of the .beads directory, not to
// .beads itself (see beads.ResolveBeadsDir), and may chain. This reimplements
// that resolution rather than calling ResolveBeadsDir because that function
// deletes a self-pointing redirect file as a side effect, which a read-only
// doctor check must not do.
func resolveBeadsRedirect(beadsDir string) string {
	current := filepath.Clean(beadsDir)
	start := current

	for depth := 0; depth < redirectChainMaxDepth; depth++ {
		data, err := os.ReadFile(filepath.Join(current, "redirect"))
		if err != nil {
			break
		}
		target := strings.TrimSpace(string(data))
		if target == "" {
			break
		}
		if !filepath.IsAbs(target) {
			// Relative to the directory containing .beads.
			target = filepath.Join(filepath.Dir(current), target)
		}
		target = filepath.Clean(target)
		if target == current {
			break // Self-pointing redirect.
		}
		current = target
	}

	if current == start {
		return ""
	}
	return current
}

// formulaVariant is one tier's copy of a formula.
type formulaVariant struct {
	tier    string
	digest  string
	version string
}

// divergedFormulas compares every formula found across a set of tiers and
// returns one description per formula whose copies do not match.
func divergedFormulas(tiers []formulaTier) []string {
	byFormula := make(map[string][]formulaVariant)
	seen := make(map[string]bool) // "<formula>\x00<tier>"

	for _, tier := range tiers {
		entries, err := os.ReadDir(tier.dir)
		if err != nil {
			continue
		}
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".formula.toml") {
				continue
			}
			content, err := os.ReadFile(filepath.Join(tier.dir, entry.Name()))
			if err != nil {
				continue
			}
			name := strings.TrimSuffix(entry.Name(), ".formula.toml")
			key := name + "\x00" + tier.label
			if seen[key] {
				continue
			}
			seen[key] = true
			byFormula[name] = append(byFormula[name], formulaVariant{
				tier:    tier.label,
				digest:  formulaDigest(content),
				version: formulaVersion(content),
			})
		}
	}

	var diverged []string
	for name, variants := range byFormula {
		if len(variants) < 2 {
			continue
		}
		digests := make(map[string]bool, len(variants))
		for _, v := range variants {
			digests[v.digest] = true
		}
		if len(digests) < 2 {
			continue // Same content in every tier — fine.
		}
		parts := make([]string, 0, len(variants))
		for _, v := range variants {
			parts = append(parts, fmt.Sprintf("%s v%s (%s)", v.tier, v.version, v.digest))
		}
		sort.Strings(parts)
		diverged = append(diverged, fmt.Sprintf("%s: %s", name, strings.Join(parts, " | ")))
	}
	sort.Strings(diverged)
	return diverged
}

// Run compares formula content across the resolution tiers, per rig.
func (c *FormulaSourceCheck) Run(ctx *CheckContext) *CheckResult {
	result := &CheckResult{
		Name:     c.Name(),
		Status:   StatusOK,
		Category: c.Category(),
	}

	rigs := []string{ctx.RigName}
	if ctx.RigName == "" {
		rigs = registeredRigNames(ctx.TownRoot)
	}
	if len(rigs) == 0 {
		rigs = []string{""} // Town-only comparison (pinned vs town).
	}

	var details []string
	for _, rigName := range rigs {
		for _, line := range divergedFormulas(formulaTiersFor(ctx.TownRoot, rigName)) {
			if rigName != "" {
				line = rigName + "/" + line
			}
			details = append(details, line)
		}
	}
	sort.Strings(details)

	if len(details) == 0 {
		result.Message = "no formula diverges across resolution tiers"
		return result
	}

	result.Status = StatusWarning
	result.Message = fmt.Sprintf("%d formula copy set(s) differ between resolution tiers", len(details))
	result.Details = details
	result.FixHint = "Pin one directory with workflow.formulas_dir in settings/config.json, or reconcile the copies — a fix in the wrong tier is silently inert"
	return result
}

// registeredRigNames returns the rigs registered in mayor/rigs.json.
//
// The directory heuristic used elsewhere (findRigDirs) also matches any
// directory that merely has a .beads or a mayor/rig — events/, tool checkouts —
// which would make these checks report missing agent beads for things that are
// not rigs. The registry is the authority on what a rig is.
func registeredRigNames(townRoot string) []string {
	data, err := os.ReadFile(filepath.Join(townRoot, "mayor", "rigs.json"))
	if err != nil {
		return nil
	}
	var parsed struct {
		Rigs map[string]json.RawMessage `json:"rigs"`
	}
	if err := json.Unmarshal(data, &parsed); err != nil {
		return nil
	}
	rigs := make([]string, 0, len(parsed.Rigs))
	for name := range parsed.Rigs {
		rigs = append(rigs, name)
	}
	sort.Strings(rigs)
	return rigs
}
