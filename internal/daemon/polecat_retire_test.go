package daemon

import (
	"context"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDaemonRunsBoundedPolecatRetirementInTown(t *testing.T) {
	town := t.TempDir()
	bin := filepath.Join(town, "bin")
	if err := os.MkdirAll(bin, 0755); err != nil {
		t.Fatal(err)
	}
	script := "#!/bin/sh\npwd > retirement-cwd\nprintf '%s\\n' \"$@\" > retirement-args\nprintf '[]\\n'\n"
	if err := os.WriteFile(filepath.Join(bin, "gt"), []byte(script), 0755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	d := &Daemon{ctx: context.Background(), config: &Config{TownRoot: town}, logger: log.New(io.Discard, "", 0)}
	d.retireMergedPolecats()
	data, err := os.ReadFile(filepath.Join(town, "retirement-args"))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "polecat\nretire\n--all\n--limit\n2\n--json\n" {
		t.Fatalf("unbounded or wrong cleanup: %s", data)
	}
	cwd, err := os.ReadFile(filepath.Join(town, "retirement-cwd"))
	if err != nil {
		t.Fatal(err)
	}
	actual, err := filepath.EvalSymlinks(strings.TrimSpace(string(cwd)))
	if err != nil {
		t.Fatal(err)
	}
	expected, _ := filepath.EvalSymlinks(town)
	if actual != expected {
		t.Fatalf("wrong authority: %s", cwd)
	}
}
