package beads

import (
	"strings"
	"testing"
)

func TestRejectionFieldsRoundTrip(t *testing.T) {
	issue := &Issue{Description: "some prose\nattached_formula: mol-polecat-work\n"}
	desc := SetRejectionFields(issue, &RejectionFields{
		RejectedSHA:    "9a2bf2d",
		RejectedBranch: "polecat/warboy/bt-j6uu.12+mtip9i05",
		RejectedReason: "gate failed",
	})
	got := ParseRejectionFields(&Issue{Description: desc})
	if got == nil || got.RejectedSHA != "9a2bf2d" ||
		got.RejectedBranch != "polecat/warboy/bt-j6uu.12+mtip9i05" || got.RejectedReason != "gate failed" {
		t.Fatalf("round trip lost data: %+v\n%s", got, desc)
	}
	if !strings.Contains(desc, "attached_formula: mol-polecat-work") || !strings.Contains(desc, "some prose") {
		t.Fatalf("unrelated description content dropped:\n%s", desc)
	}
	// Attachment parsing must be unaffected by the new lines.
	if af := ParseAttachmentFields(&Issue{Description: desc}); af == nil || af.AttachedFormula != "mol-polecat-work" {
		t.Fatalf("attachment fields broken by rejection fields: %+v", af)
	}
}

func TestRejectionFieldsClearing(t *testing.T) {
	issue := &Issue{Description: "body\nrejected_sha: 9a2bf2d\nrejected_branch: b\nrejected_reason: r\n"}
	desc := SetRejectionFields(issue, nil)
	if ParseRejectionFields(&Issue{Description: desc}) != nil {
		t.Fatalf("fields not cleared:\n%s", desc)
	}
	if !strings.Contains(desc, "body") {
		t.Fatalf("body lost:\n%s", desc)
	}
}

func TestRejectionFieldsAbsent(t *testing.T) {
	if ParseRejectionFields(nil) != nil {
		t.Fatal("nil issue")
	}
	if ParseRejectionFields(&Issue{Description: "no fields here"}) != nil {
		t.Fatal("false positive")
	}
	if ParseRejectionFields(&Issue{Description: "rejected_sha: null\n"}) != nil {
		t.Fatal("null must read as absent, matching every other field parser")
	}
}

func TestRejectionReasonIsFlattened(t *testing.T) {
	desc := SetRejectionFields(nil, &RejectionFields{RejectedSHA: "abc", RejectedReason: "line one\nline two"})
	got := ParseRejectionFields(&Issue{Description: desc})
	if got == nil || got.RejectedReason != "line one line two" {
		t.Fatalf("multi-line reason must be flattened, got %+v from:\n%s", got, desc)
	}
}

func TestRejectionFieldsHyphenSpellings(t *testing.T) {
	got := ParseRejectionFields(&Issue{Description: "rejected-sha: abc\nRejectedBranch: main\n"})
	if got == nil || got.RejectedSHA != "abc" || got.RejectedBranch != "main" {
		t.Fatalf("alternate spellings not accepted: %+v", got)
	}
}
