package beads

import "strings"

// RejectionFields records, on the SOURCE work bead, the merge candidate that
// the Refinery rejected. They are durable (description "key: value" lines), so
// a later candidate is compared against a stored value instead of the Refinery
// agent's session memory — hq-tx4md, where a re-push to the same branch with a
// genuinely new SHA (9a2bf2d → e1e30e2) was classified as the already-rejected
// candidate and stalled for three hours.
type RejectionFields struct {
	RejectedSHA    string // Head SHA of the rejected candidate
	RejectedBranch string // Branch the rejected candidate was on
	RejectedReason string // Why the merge gate refused it
}

// Rejection field keys, in the canonical and tolerated spellings.
var rejectionFieldKeys = map[string]bool{
	"rejected_sha":    true,
	"rejected-sha":    true,
	"rejectedsha":     true,
	"rejected_branch": true,
	"rejected-branch": true,
	"rejectedbranch":  true,
	"rejected_reason": true,
	"rejected-reason": true,
	"rejectedreason":  true,
}

// ParseRejectionFields extracts rejection fields from an issue's description.
// Returns nil when the bead carries no rejection record.
func ParseRejectionFields(issue *Issue) *RejectionFields {
	if issue == nil || issue.Description == "" {
		return nil
	}
	fields := &RejectionFields{}
	hasFields := false
	for _, line := range strings.Split(issue.Description, "\n") {
		line = strings.TrimSpace(line)
		colonIdx := strings.Index(line, ":")
		if colonIdx == -1 {
			continue
		}
		key := strings.ToLower(strings.TrimSpace(line[:colonIdx]))
		value := strings.TrimSpace(line[colonIdx+1:])
		if value == "" || strings.EqualFold(value, "null") {
			continue
		}
		switch key {
		case "rejected_sha", "rejected-sha", "rejectedsha":
			fields.RejectedSHA = value
			hasFields = true
		case "rejected_branch", "rejected-branch", "rejectedbranch":
			fields.RejectedBranch = value
			hasFields = true
		case "rejected_reason", "rejected-reason", "rejectedreason":
			// Reasons are free text; keep the whole remainder of the line.
			fields.RejectedReason = value
			hasFields = true
		}
	}
	if !hasFields {
		return nil
	}
	return fields
}

// FormatRejectionFields renders rejection fields as description lines.
func FormatRejectionFields(fields *RejectionFields) string {
	if fields == nil {
		return ""
	}
	var lines []string
	if fields.RejectedSHA != "" {
		lines = append(lines, "rejected_sha: "+fields.RejectedSHA)
	}
	if fields.RejectedBranch != "" {
		lines = append(lines, "rejected_branch: "+fields.RejectedBranch)
	}
	if fields.RejectedReason != "" {
		// A reason is single-line by construction: newlines would be parsed as
		// unrelated description content on the way back in.
		lines = append(lines, "rejected_reason: "+singleLine(fields.RejectedReason))
	}
	return strings.Join(lines, "\n")
}

// SetRejectionFields returns a new description with the rejection fields
// replaced. Passing nil (or an empty struct) clears them, which is how a
// superseding candidate erases the memory of the rejected one.
func SetRejectionFields(issue *Issue, fields *RejectionFields) string {
	description := ""
	if issue != nil {
		description = issue.Description
	}

	var kept []string
	for _, line := range strings.Split(description, "\n") {
		trimmed := strings.TrimSpace(line)
		if colonIdx := strings.Index(trimmed, ":"); colonIdx != -1 {
			key := strings.ToLower(strings.TrimSpace(trimmed[:colonIdx]))
			if rejectionFieldKeys[key] {
				continue
			}
		}
		kept = append(kept, line)
	}
	for len(kept) > 0 && strings.TrimSpace(kept[len(kept)-1]) == "" {
		kept = kept[:len(kept)-1]
	}

	formatted := FormatRejectionFields(fields)
	body := strings.Join(kept, "\n")
	switch {
	case formatted == "":
		return body
	case body == "":
		return formatted
	default:
		return body + "\n" + formatted
	}
}

func singleLine(s string) string {
	s = strings.ReplaceAll(s, "\r\n", " ")
	s = strings.ReplaceAll(s, "\n", " ")
	return strings.TrimSpace(s)
}
