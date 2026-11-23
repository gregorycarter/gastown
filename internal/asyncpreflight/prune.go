package asyncpreflight

import (
	"os"
	"path/filepath"
	"strings"
	"time"
)

// MRSidecarPath records which MR a marker belongs to (written by gt done once
// the MR exists; the runner owns the marker file itself).
func MRSidecarPath(markerPath string) string { return sibling(markerPath, ".mr") }

// RecordMR writes the MR id next to the marker.
func RecordMR(markerPath, mrID string) error {
	return writeFileAtomic(MRSidecarPath(markerPath), []byte(mrID+"\n"))
}

// RecordedMR returns the MR id recorded for a marker, or "".
func RecordedMR(markerPath string) string {
	raw, err := os.ReadFile(MRSidecarPath(markerPath))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(raw))
}

// MRState is what Prune needs to know about a marker's MR.
type MRState struct {
	Known  bool   // the MR could be read
	Closed bool   // the MR is closed
	Head   string // the MR's commit_sha
}

// PruneResult lists what Prune did.
type PruneResult struct {
	Removed []string // marker paths removed
	Stopped []string // marker paths whose runner was signalled
}

// Prune removes markers (with their log, receipt copy and MR sidecar) that
// are older than StaleAfter and not live, or whose MR is closed; a still-live
// runner whose MR is closed or whose MR head moved past it is stopped first
// (superseded). mrState is consulted only for markers with a recorded MR.
func Prune(rigPath string, now time.Time, mrState func(mrID string) MRState) PruneResult {
	var res PruneResult
	files, _ := filepath.Glob(filepath.Join(Dir(rigPath), "*.json"))
	for _, p := range files {
		if strings.HasSuffix(p, ".receipt.json") {
			continue
		}
		m, err := Load(p)
		if err != nil {
			if info, statErr := os.Stat(p); statErr == nil && now.Sub(info.ModTime()) > StaleAfter {
				removeRun(p)
				res.Removed = append(res.Removed, p)
			}
			continue
		}
		live := Live(m, now)
		var st MRState
		if mr := RecordedMR(p); mr != "" && mrState != nil {
			st = mrState(mr)
		}
		superseded := st.Known && (st.Closed || (st.Head != "" && st.Head != m.Head))
		if live && superseded && Stop(p, m) {
			res.Stopped = append(res.Stopped, p)
			continue // the runner records its stop; prune on a later pass
		}
		created, ok := age(m.CreatedAt, now)
		stale := ok && created > StaleAfter
		if !live && ((st.Known && st.Closed) || stale) {
			removeRun(p)
			res.Removed = append(res.Removed, p)
		}
	}
	prev, _ := filepath.Glob(filepath.Join(Dir(rigPath), "*.prev"))
	for _, p := range prev {
		if info, err := os.Stat(p); err == nil && now.Sub(info.ModTime()) > StaleAfter {
			_ = os.Remove(p)
		}
	}
	return res
}

func removeRun(markerPath string) {
	for _, p := range []string{markerPath, LogPath(markerPath), ReceiptCopyPath(markerPath), MRSidecarPath(markerPath), StopRequestPath(markerPath)} {
		_ = os.Remove(p)
	}
}
