package git

import (
	"fmt"
	"strings"
)

// LandedIssueCommit returns the first commit on one of targets (default: the
// remote default branch, then origin/main and origin/master) whose message
// names issueID as a whole token. The lander rebases polecat branches before
// it fast-forwards main, so a landed change keeps its bead ID in the message
// but not its SHA, and often not its patch-id once conflicts were resolved.
// It returns "" with no error when no target commit names the issue, and an
// error only when no target ref resolves or git fails (kasb-84hoc).
func (g *Git) LandedIssueCommit(issueID string, targets []string) (string, string, error) {
	issueID = strings.TrimSpace(issueID)
	if issueID == "" {
		return "", "", fmt.Errorf("empty issue id")
	}
	var refs []string
	for _, target := range nonEmptyUnique(targets) {
		if ref, ok := g.resolveComparisonRef(target, "origin"); ok {
			refs = append(refs, ref)
		}
	}
	if len(refs) == 0 {
		for _, ref := range []string{"origin/" + g.RemoteDefaultBranch(), "origin/main", "origin/master"} {
			if resolved, ok := g.resolveComparisonRef(ref, "origin"); ok {
				refs = append(refs, resolved)
			}
		}
	}
	refs = nonEmptyUnique(refs)
	if len(refs) == 0 {
		return "", "", fmt.Errorf("no target ref resolves")
	}
	for _, ref := range refs {
		// %x1e separates records, %x1f the SHA from the message.
		out, err := g.run("log", ref, "--fixed-strings", "--grep="+issueID, "--format=%H%x1f%B%x1e")
		if err != nil {
			return "", "", err
		}
		for _, record := range strings.Split(out, "\x1e") {
			sha, message, ok := strings.Cut(strings.TrimSpace(record), "\x1f")
			if ok && MessageNamesIssue(message, issueID) {
				return strings.TrimSpace(sha), ref, nil
			}
		}
	}
	return "", "", nil
}

// MessageNamesIssue reports whether message names issueID as a whole token:
// kasb-9oupz.2 must not match kasb-9oupz.20 or xkasb-9oupz.2.
func MessageNamesIssue(message, issueID string) bool {
	if issueID == "" {
		return false
	}
	for offset := 0; ; {
		i := strings.Index(message[offset:], issueID)
		if i < 0 {
			return false
		}
		start := offset + i
		end := start + len(issueID)
		if (start == 0 || !issueIDByte(message[start-1])) && (end == len(message) || !issueIDContinues(message, end)) {
			return true
		}
		offset = start + 1
	}
}

func issueIDByte(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_'
}

// issueIDContinues reports whether the ID at message[:end] continues past end:
// another ID byte, or a dot followed by an ID byte (a child ID such as .2.1).
// A sentence-ending dot does not continue it.
func issueIDContinues(message string, end int) bool {
	c := message[end]
	if issueIDByte(c) {
		return true
	}
	return c == '.' && end+1 < len(message) && issueIDByte(message[end+1])
}
