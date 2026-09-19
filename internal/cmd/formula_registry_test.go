package cmd

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPinnedRegistryAliasIsImmutableAndKeepsCanonicalTitle(t *testing.T) {
	dir := t.TempDir()
	data := []byte(`{"formula":"mol-refinery-patrol","steps":[{"id":"hisn"}]}`)
	name, err := publishFormulaAlias(dir, data)
	if err != nil || !strings.HasPrefix(name, "mol-gt-pin-") {
		t.Fatalf("alias %s: %v", name, err)
	}
	file := filepath.Join(dir, name+".formula.json")
	got, _ := os.ReadFile(file)
	if !bytes.Equal(got, data) {
		t.Fatal("formula title or bytes changed")
	}
	if again, err := publishFormulaAlias(dir, data); err != nil || again != name {
		t.Fatal("not idempotent", err)
	}
	if err := os.WriteFile(file, []byte("tampered"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := publishFormulaAlias(dir, data); err == nil {
		t.Fatal("overwrote changed alias")
	}
}

func TestPinnedRegistryRefusesSymlinkAndInvalidCook(t *testing.T) {
	dir := t.TempDir()
	if _, err := publishFormulaAlias(dir, []byte(`{}`)); err == nil {
		t.Fatal("empty cook accepted")
	}
	link := filepath.Join(t.TempDir(), "registry")
	if err := os.Symlink(dir, link); err != nil {
		t.Fatal(err)
	}
	if _, err := publishFormulaAlias(link, []byte(`{"formula":"mol-test"}`)); err == nil {
		t.Fatal("symlink registry accepted")
	}
}
