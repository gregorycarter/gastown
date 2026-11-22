package asyncpreflight

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// infraExec writes what kasb's preflight.py writes on its infrastructure
// route: a receipt under .delivery/infrastructure-receipts/ and nothing at
// --receipt (hq-4gmjjb, kasb-qyar.228.1 on 22 Nov).
func infraExec(receipt map[string]any, exit int) func(context.Context, string, []string, io.Writer) (int, error) {
	return func(_ context.Context, dir string, _ []string, log io.Writer) (int, error) {
		fmt.Fprintln(log, "Infrastructure receipt recorded: zero product suites, CI runs or application builds.")
		if receipt != nil {
			p := filepath.Join(dir, ".delivery", "infrastructure-receipts", src+"-0cbe20c5.json")
			_ = os.MkdirAll(filepath.Dir(p), 0o755)
			raw, _ := json.Marshal(receipt)
			_ = os.WriteFile(p, raw, 0o644)
		}
		return exit, nil
	}
}

func infraReceipt() map[string]any {
	return map[string]any{
		"scope": "infrastructure", "head": headA, "source_bead": src, "status": InfrastructureReceiptStatus,
		"product": []string{}, "infrastructure": []string{"infra/aws/demo/x.spec.mjs"},
		"product_ci_runs": 0, "application_builds": 0, "schema_version": 1,
	}
}

func TestRunAdoptsInfrastructureReceipt(t *testing.T) {
	r := newRunFixture(t)
	deps := r.deps()
	deps.Exec = infraExec(infraReceipt(), 0)
	if err := Run(context.Background(), r.path, deps); err != nil {
		t.Fatal(err)
	}
	m, err := Load(r.path)
	if err != nil {
		t.Fatal(err)
	}
	if m.Status != StatusPassed || m.ReceiptStatus != "passed" {
		t.Fatalf("infrastructure-only head not passed: %+v", m)
	}
	raw, err := os.ReadFile(m.ReceiptCopy)
	if err != nil {
		t.Fatal(err)
	}
	var copy map[string]any
	_ = json.Unmarshal(raw, &copy)
	if copy["receipt_kind"] != ReceiptKindInfrastructure || copy["infrastructure_status"] != InfrastructureReceiptStatus || copy["head_sha"] != headA {
		t.Fatalf("adopted receipt lost its provenance: %v", copy)
	}
	if _, err := os.Stat(filepath.Join(r.worktree, m.ReceiptPath)); err != nil {
		t.Fatalf("requested receipt path not written: %v", err)
	}
	if g := Evaluate(filepath.Dir(filepath.Dir(filepath.Dir(r.path))), MRFields{Source: src, CommitSHA: headA, RunPath: r.path}, headA, time.Now()); g.State != GatePassed {
		t.Fatalf("gate after infrastructure pass: %+v", g)
	}
}

func TestRunRefusesInfrastructureReceiptThatDoesNotProveTheHead(t *testing.T) {
	cases := map[string]func(map[string]any) int{
		"other head":    func(r map[string]any) int { r["head"] = headB; return 0 },
		"other source":  func(r map[string]any) int { r["source_bead"] = "kasb-other"; return 0 },
		"product paths": func(r map[string]any) int { r["product"] = []string{"apps/api/kasb/x.py"}; return 0 },
		"not reviewed":  func(r map[string]any) int { r["status"] = "failed"; return 0 },
		"product scope": func(r map[string]any) int { r["scope"] = "product"; return 0 },
		"nonzero exit":  func(map[string]any) int { return 1 },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			r := newRunFixture(t)
			receipt := infraReceipt()
			exit := mutate(receipt)
			deps := r.deps()
			deps.Exec = infraExec(receipt, exit)
			if err := Run(context.Background(), r.path, deps); err != nil {
				t.Fatal(err)
			}
			m, _ := Load(r.path)
			if m.Status != StatusError || !strings.Contains(m.Reason, "without a receipt") {
				t.Fatalf("unproven infrastructure receipt accepted: %+v", m)
			}
		})
	}
}

func TestRunIgnoresInfrastructureReceiptFromEarlierRun(t *testing.T) {
	r := newRunFixture(t)
	p := filepath.Join(r.worktree, ".delivery", "infrastructure-receipts", src+"-old.json")
	_ = os.MkdirAll(filepath.Dir(p), 0o755)
	raw, _ := json.Marshal(infraReceipt())
	_ = os.WriteFile(p, raw, 0o644)
	old := time.Now().Add(-time.Hour)
	_ = os.Chtimes(p, old, old)
	deps := r.deps()
	deps.Exec = infraExec(nil, 0)
	if err := Run(context.Background(), r.path, deps); err != nil {
		t.Fatal(err)
	}
	m, _ := Load(r.path)
	if m.Status != StatusError {
		t.Fatalf("stale infrastructure receipt adopted: %+v", m)
	}
}
