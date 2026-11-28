package asyncpreflight

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func writeEnvReceipt(t *testing.T, dir, markerPath string, receipt map[string]any) {
	t.Helper()
	m, _ := Load(markerPath)
	p := filepath.Join(dir, m.ReceiptPath)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(receipt)
	if err := os.WriteFile(p, raw, 0o644); err != nil {
		t.Fatal(err)
	}
}

func envReceipt() map[string]any {
	return map[string]any{
		"head_sha": headA, "source_bead_id": src, "status": "failed",
		"failures_all_environmental": true,
		"errors": []string{
			"api-tests-replay-prefix: environmental test setup psycopg error persisted after one retry",
		},
	}
}

// TestAllEnvironmentalReceiptRerunsOnce: a failed receipt flagged
// failures_all_environmental is rerun once on the same head; a passing rerun
// makes the whole run pass, and both receipts are kept (kasb-9oupz.71).
func TestAllEnvironmentalReceiptRerunsOnce(t *testing.T) {
	r := newRunFixture(t)
	r.heads = []string{headA}
	deps := r.deps()
	calls := 0
	deps.Exec = func(ctx context.Context, dir string, argv []string, log io.Writer) (int, error) {
		calls++
		if calls == 1 {
			writeEnvReceipt(t, dir, r.path, envReceipt())
			return 1, nil
		}
		writeEnvReceipt(t, dir, r.path, map[string]any{"head_sha": headA, "source_bead_id": src, "status": "passed"})
		return 0, nil
	}
	if err := Run(context.Background(), r.path, deps); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("expected one rerun (2 execs), got %d", calls)
	}
	m, _ := Load(r.path)
	if m.Status != StatusPassed {
		t.Fatalf("expected pass after the environmental rerun, got %s / %q", m.Status, m.Reason)
	}
	if m.RecheckedEnvironment == "" {
		t.Fatalf("rechecked_environment should record the rerun")
	}
	if _, err := os.Stat(sibling(r.path, ".receipt-environmental.json")); err != nil {
		t.Fatalf("the first environmental receipt should be kept: %v", err)
	}
}

// TestAllEnvironmentalRerunStillFails: if the rerun also fails environmental,
// the run finishes failed — bounded to a single rerun.
func TestAllEnvironmentalRerunStillFails(t *testing.T) {
	r := newRunFixture(t)
	r.heads = []string{headA}
	deps := r.deps()
	calls := 0
	deps.Exec = func(ctx context.Context, dir string, argv []string, log io.Writer) (int, error) {
		calls++
		writeEnvReceipt(t, dir, r.path, envReceipt())
		return 1, nil
	}
	if err := Run(context.Background(), r.path, deps); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("expected exactly one rerun (2 execs), got %d", calls)
	}
	m, _ := Load(r.path)
	if m.Status != StatusFailed {
		t.Fatalf("expected failed after a second environmental failure, got %s", m.Status)
	}
}

// TestNonEnvironmentalReceiptDoesNotRerun: a failed receipt WITHOUT the field
// (an older preflight, or a real product failure) is not rerun — today's
// behaviour.
func TestNonEnvironmentalReceiptDoesNotRerun(t *testing.T) {
	r := newRunFixture(t)
	r.heads = []string{headA}
	deps := r.deps()
	calls := 0
	deps.Exec = func(ctx context.Context, dir string, argv []string, log io.Writer) (int, error) {
		calls++
		writeEnvReceipt(t, dir, r.path, map[string]any{
			"head_sha": headA, "source_bead_id": src, "status": "failed",
			"errors": []string{"tests/test_feature.py::test_x assert 1 == 2"},
		})
		return 1, nil
	}
	if err := Run(context.Background(), r.path, deps); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("a non-environmental failure must not rerun; got %d execs", calls)
	}
	m, _ := Load(r.path)
	if m.Status != StatusFailed {
		t.Fatalf("expected failed, got %s", m.Status)
	}
}
