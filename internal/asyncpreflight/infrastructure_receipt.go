package asyncpreflight

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// InfrastructureReceiptStatus is what kasb's preflight records for a head whose
// every changed path is development infrastructure.
const InfrastructureReceiptStatus = "infrastructure-reviewed"

// ReceiptKindInfrastructure marks an async receipt adopted from preflight's
// infrastructure route.
const ReceiptKindInfrastructure = "infrastructure"

type infrastructureReceipt struct {
	Scope      string            `json:"scope"`
	Head       string            `json:"head"`
	Source     string            `json:"source_bead"`
	Status     string            `json:"status"`
	Product    []json.RawMessage `json:"product"`
	ProductCI  int               `json:"product_ci_runs"`
	AppBuilds  int               `json:"application_builds"`
	Repository string            `json:"repository"`
}

// adoptInfrastructureReceipt finds the infrastructure receipt this run wrote
// for the marker's exact head and source and returns an async receipt built
// from it (hq-4gmjjb). kasb's preflight.py takes its infrastructure route
// before it parses --receipt, so an infrastructure-only head exited 0 with no
// receipt at the requested path and the MR bounced as NEEDS-FIX.
//
// Only a receipt written during this run (mtime at or after runStart), bound
// to this head and source, with infrastructure scope, the reviewed status and
// no product paths, is adopted. Anything else keeps the old error.
func adoptInfrastructureReceipt(m *Marker, runStart time.Time) ([]byte, bool) {
	if m == nil || m.Worktree == "" || m.Source == "" || m.Head == "" || strings.ContainsAny(m.Source, `/\`) {
		return nil, false
	}
	dir := filepath.Join(m.Worktree, ".delivery", "infrastructure-receipts")
	matches, err := filepath.Glob(filepath.Join(dir, m.Source+"-*.json"))
	if err != nil || len(matches) == 0 {
		return nil, false
	}
	sort.Strings(matches)
	var newest []byte
	var newestAt time.Time
	for _, path := range matches {
		info, err := os.Stat(path)
		if err != nil || info.IsDir() || info.ModTime().Before(runStart) {
			continue
		}
		raw, err := os.ReadFile(path) //nolint:gosec // G304: path under the marker's own worktree
		if err != nil {
			continue
		}
		var r infrastructureReceipt
		if json.Unmarshal(raw, &r) != nil {
			continue
		}
		if r.Scope != "infrastructure" || r.Head != m.Head || r.Source != m.Source || r.Status != InfrastructureReceiptStatus || len(r.Product) != 0 || r.ProductCI != 0 || r.AppBuilds != 0 {
			continue
		}
		if newest == nil || info.ModTime().After(newestAt) {
			var full map[string]json.RawMessage
			if json.Unmarshal(raw, &full) != nil {
				continue
			}
			full["head_sha"], _ = json.Marshal(m.Head)
			full["source_bead_id"], _ = json.Marshal(m.Source)
			full["status"], _ = json.Marshal("passed")
			full["receipt_kind"], _ = json.Marshal(ReceiptKindInfrastructure)
			full["infrastructure_status"], _ = json.Marshal(r.Status)
			full["infrastructure_receipt"], _ = json.Marshal(filepath.Join(".delivery", "infrastructure-receipts", filepath.Base(path)))
			out, err := json.MarshalIndent(full, "", "  ")
			if err != nil {
				continue
			}
			newest, newestAt = out, info.ModTime()
		}
	}
	return newest, newest != nil
}
