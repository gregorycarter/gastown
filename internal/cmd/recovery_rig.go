package cmd

import (
	"fmt"
	"strings"
)

// Explicit opt-in. Do not reinterpret another rig's lifecycle or infer it from cwd.
type recoveryRigSpec struct{ Name, Prefix, Origin string }

func recoveryRig(name string) (recoveryRigSpec, error) {
	switch name {
	case "hisn":
		return recoveryRigSpec{name, "hisn", "https://github.com/gregorycarter/hisn-core"}, nil
	case "kasb_core":
		return recoveryRigSpec{name, "kasb", "https://github.com/gregorycarter/kasb-core"}, nil
	default:
		return recoveryRigSpec{}, fmt.Errorf("recovery is not enabled for rig %q", name)
	}
}
func recoveryName(names []string) string {
	if len(names) == 0 {
		return "hisn"
	} // existing Hisn callers retain their contract
	return names[0]
}
func recoveryRigForSource(id string) (recoveryRigSpec, error) {
	for _, name := range []string{"hisn", "kasb_core"} {
		spec, _ := recoveryRig(name)
		if strings.HasPrefix(id, spec.Prefix+"-") {
			return spec, nil
		}
	}
	return recoveryRigSpec{}, fmt.Errorf("unsupported recovery source %q", id)
}
func recoveryInstructions(r mqResumeRecord) string {
	if r.Rig == "hisn" {
		return fmt.Sprintf("Original worker hisn/%s: recover source %s on %s; resolve/rebase onto %s, commit as Greg Carter <email@gregorycarter.net>, run and publish renewed exact-HEAD preflight, push only this worker branch with an explicit expected-old-head lease, run node tools/delivery/resubmit.mjs %s --apply, then gt done --pre-verified. Preserve this MR and source; do not create conflict/verifier tasks or close the source.", r.Worker, r.Source, r.Branch, r.Base, r.MR)
	}
	return fmt.Sprintf("Original worker %s/%s: recover source %s on %s; resolve/rebase onto %s. Preserve source and MR %s. Reuse the green receipt for an unchanged patch; if the product patch changes run targeted preflight once. Infrastructure-only changes use local infrastructure receipt checks, never product CI. Push only this branch with an explicit expected-old-head lease, then gt mq resubmit-recovery kasb_core %s followed by gt done to finish the SAME MR. Never create replacement work or close the source.", r.Rig, r.Worker, r.Source, r.Branch, r.Base, r.MR, r.MR)
}
