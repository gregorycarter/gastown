package git

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// mechanicalFixture models a kasb migration-number collision (kasb-1dxro):
// the submission adds migration 0251_x after 0250_a; main meanwhile added
// 0251_m. The lander renames the submission's migration to 0252_x, re-points
// its dependency at 0251_m, rewrites the reference in a test and sets the
// max_migration index.
type mechanicalFixture struct {
	dir, branch, submitted string
}

func writeFiles(t *testing.T, dir string, files map[string]string) {
	t.Helper()
	for path, body := range files {
		full := filepath.Join(dir, path)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func commitFiles(t *testing.T, dir, msg string, files map[string]string, remove ...string) string {
	t.Helper()
	writeFiles(t, dir, files)
	for _, path := range remove {
		vswlGit(t, dir, "rm", "-q", path)
	}
	vswlGit(t, dir, "add", "-A")
	vswlGit(t, dir, "commit", "-q", "-m", msg)
	return vswlGit(t, dir, "rev-parse", "HEAD")
}

func newMechanicalFixture(t *testing.T) mechanicalFixture {
	t.Helper()
	dir, _, branch := initTestRepoWithRemote(t)
	commitFiles(t, dir, "base", map[string]string{
		"migrations/0250_a.py":         "deps = [('kasb', '0249_z')]\n",
		"migrations/max_migration.txt": "0250_a\n",
		"tests/test_flow.py":           "def test_flow():\n    assert True\n",
	})
	base := vswlGit(t, dir, "rev-parse", "HEAD")
	vswlGit(t, dir, "checkout", "-q", "-b", "submitted")
	submitted := commitFiles(t, dir, "add 0251_x", map[string]string{
		"migrations/0251_x.py":         "deps = [('kasb', '0250_a')]\nfields = ['signoff']\n",
		"migrations/max_migration.txt": "0251_x\n",
		"tests/test_flow.py":           "def test_flow():\n    assert True\n\ndef test_x():\n    assert run('0251_x')\n",
		"app/signoff.py":               "SIGNOFF = True\n",
	})
	vswlGit(t, dir, "checkout", "-q", branch)
	commitFiles(t, dir, "main adds 0251_m", map[string]string{
		"migrations/0251_m.py":         "deps = [('kasb', '0250_a')]\n",
		"migrations/max_migration.txt": "0251_m\n",
	})
	_ = base
	return mechanicalFixture{dir: dir, branch: branch, submitted: submitted}
}

var fixtureRewrite = MechanicalRewrite{
	Paths:   []string{"migrations/max_migration.txt"},
	Renames: [][2]string{{"0251_x", "0252_x"}, {"0250_a", "0251_m"}},
}

func (f mechanicalFixture) landMechanically(t *testing.T, extra map[string]string) string {
	t.Helper()
	files := map[string]string{
		"migrations/0252_x.py":         "deps = [('kasb', '0251_m')]\nfields = ['signoff']\n",
		"migrations/max_migration.txt": "0252_x\n",
		"tests/test_flow.py":           "def test_flow():\n    assert True\n\ndef test_x():\n    assert run('0252_x')\n",
		"app/signoff.py":               "SIGNOFF = True\n",
	}
	for k, v := range extra {
		files[k] = v
	}
	landed := commitFiles(t, f.dir, "land 0252_x mechanically", files)
	vswlGit(t, f.dir, "push", "-q", "origin", f.branch)
	return landed
}

func TestMechanicalLandingReceipt_AcceptsDeclaredRenumber(t *testing.T) {
	f := newMechanicalFixture(t)
	landed := f.landMechanically(t, nil)
	got, err := NewGit(f.dir).MechanicalLandingReceipt("origin", f.branch, f.submitted, landed, fixtureRewrite)
	if err != nil || got != landed {
		t.Fatalf("receipt=%q err=%v, want %s", got, err, landed)
	}
}

func TestMechanicalLandingReceipt_RejectsUndeclaredChanges(t *testing.T) {
	cases := map[string]struct {
		extra map[string]string
		rw    MechanicalRewrite
	}{
		"dropped source hunk":   {extra: map[string]string{"app/signoff.py": "SIGNOFF = False\n"}, rw: fixtureRewrite},
		"extra added file":      {extra: map[string]string{"app/backdoor.py": "x = 1\n"}, rw: fixtureRewrite},
		"rename not declared":   {rw: MechanicalRewrite{Paths: []string{"migrations/max_migration.txt"}}},
		"conflict not declared": {rw: MechanicalRewrite{Renames: fixtureRewrite.Renames}},
		"migration body edited": {extra: map[string]string{"migrations/0252_x.py": "deps = [('kasb', '0251_m')]\nfields = []\n"}, rw: fixtureRewrite},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			f := newMechanicalFixture(t)
			landed := f.landMechanically(t, tc.extra)
			got, err := NewGit(f.dir).MechanicalLandingReceipt("origin", f.branch, f.submitted, landed, tc.rw)
			if err == nil || got != "" {
				t.Fatalf("receipt=%q err=%v; undeclared change must be rejected", got, err)
			}
		})
	}
}

func TestSubstituteWords(t *testing.T) {
	renames := [][2]string{{"0251_x", "0252_x"}, {"0252_x", "0253_x"}}
	got := SubstituteWords("a 0251_x b 0251_xy c x0251_x 0252_x", renames)
	want := "a 0252_x b 0251_xy c x0251_x 0253_x"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

// TestMechanicalLandingReceipt_LiveRecord runs only when pointed at a real
// clone and record (operator verification against a recorded landing).
func TestMechanicalLandingReceipt_LiveRecord(t *testing.T) {
	dir, record := os.Getenv("GT_MECH_LIVE_DIR"), os.Getenv("GT_MECH_LIVE_RECORD")
	if dir == "" || record == "" {
		t.Skip("set GT_MECH_LIVE_DIR, GT_MECH_LIVE_RECORD, GT_MECH_LIVE_SUBMITTED, GT_MECH_LIVE_LANDED")
	}
	raw, err := os.ReadFile(record)
	if err != nil {
		t.Fatal(err)
	}
	var rw MechanicalRewrite
	if err := json.Unmarshal(raw, &rw); err != nil {
		t.Fatal(err)
	}
	got, err := NewGit(dir).MechanicalLandingReceipt("origin", "main", os.Getenv("GT_MECH_LIVE_SUBMITTED"), os.Getenv("GT_MECH_LIVE_LANDED"), rw)
	if err != nil {
		t.Fatalf("live proof failed: %v", err)
	}
	t.Logf("live proof receipt %s", got)
	if os.Getenv("GT_MECH_LIVE_TAMPER") != "" {
		rw.Renames = rw.Renames[:1]
		if _, err := NewGit(dir).MechanicalLandingReceipt("origin", "main", os.Getenv("GT_MECH_LIVE_SUBMITTED"), os.Getenv("GT_MECH_LIVE_LANDED"), rw); err == nil {
			t.Fatal("dropping the dependency re-point must fail")
		} else {
			t.Logf("tampered record rejected: %v", err)
		}
	}
}
