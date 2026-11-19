package formula

import (
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/BurntSushi/toml"
)

// StepTitles returns the distinct literal step titles of the embedded
// formulas plus any *.formula.toml in townRoot/.beads/formulas (townRoot may
// be empty). Titles containing template variables are skipped, and files that
// fail to parse are ignored. Maintenance uses the set to recognise step wisps
// whose parent molecule is gone.
func StepTitles(townRoot string) []string {
	titles := make(map[string]bool)
	add := func(data []byte) {
		var f struct {
			Steps []struct {
				Title string `toml:"title"`
			} `toml:"steps"`
		}
		if _, err := toml.Decode(string(data), &f); err != nil {
			return
		}
		for _, s := range f.Steps {
			t := strings.TrimSpace(s.Title)
			if t == "" || strings.Contains(t, "{") {
				continue
			}
			titles[t] = true
		}
	}

	_ = fs.WalkDir(formulasFS, "formulas", func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !hasFormulaSuffix(d.Name()) {
			return nil
		}
		if data, err := formulasFS.ReadFile(path); err == nil {
			add(data)
		}
		return nil
	})

	if townRoot != "" {
		matches, _ := filepath.Glob(filepath.Join(townRoot, ".beads", "formulas", "*.formula.toml"))
		for _, path := range matches {
			if data, err := os.ReadFile(path); err == nil { //nolint:gosec // G304: town formula directory
				add(data)
			}
		}
	}

	out := make([]string, 0, len(titles))
	for t := range titles {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}
