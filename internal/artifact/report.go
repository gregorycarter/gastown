package artifact

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

type DiskEntry struct {
	Path  string `json:"path"`
	Bytes int64  `json:"bytes"`
}

type DiskReport struct {
	Root       string      `json:"root"`
	TotalBytes int64       `json:"total_bytes"`
	Entries    []DiskEntry `json:"entries"`
}

// ReportDisk returns the largest immediate children of root. It never follows
// symlinks and is intended as a compact pointer to the next safe investigation.
func ReportDisk(root string, limit int) (DiskReport, error) {
	canonical, err := canonicalRoot(root)
	if err != nil {
		return DiskReport{}, err
	}
	entries, err := os.ReadDir(canonical)
	if err != nil {
		return DiskReport{}, fmt.Errorf("reading report root: %w", err)
	}
	report := DiskReport{Root: canonical, Entries: []DiskEntry{}}
	for _, entry := range entries {
		path := filepath.Join(canonical, entry.Name())
		info, statErr := os.Lstat(path)
		if statErr != nil || info.Mode()&os.ModeSymlink != 0 {
			continue
		}
		var bytes int64
		if info.IsDir() {
			var scanErr error
			bytes, _, scanErr = directoryFacts(path)
			if scanErr != nil {
				return DiskReport{}, fmt.Errorf("scanning %s: %w", path, scanErr)
			}
		} else if info.Mode().IsRegular() {
			bytes = info.Size()
		}
		report.TotalBytes += bytes
		report.Entries = append(report.Entries, DiskEntry{Path: entry.Name(), Bytes: bytes})
	}
	sort.Slice(report.Entries, func(i, j int) bool { return report.Entries[i].Bytes > report.Entries[j].Bytes })
	if limit > 0 && len(report.Entries) > limit {
		report.Entries = report.Entries[:limit]
	}
	return report, nil
}
