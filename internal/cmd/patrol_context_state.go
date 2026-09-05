package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// A bounded observation checkpoint survives wisp GC and session compaction.
// It is a recovery hint, never authority for worker ownership or destructive actions.
func savePatrolContext(townRoot, actor, patrol, summary string) error {
	text := []rune(summary)
	if len(text) > 1200 {
		text = text[:1200]
	}
	dir := filepath.Join(townRoot, ".runtime", "patrol-state")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	data, err := json.Marshal(map[string]string{"ts": time.Now().UTC().Format(time.RFC3339), "actor": actor, "patrol": patrol, "observations": string(text)})
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".checkpoint-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	name := strings.NewReplacer("/", "-", "\\", "-", "..", "-").Replace(actor)
	return os.Rename(f.Name(), filepath.Join(dir, name+".json"))
}
