# Operator managed merge landing

Use [`scripts/operator-land.sh`](../../scripts/operator-land.sh) when a rig's
Refinery is unavailable and an operator has decided to land its ready merge
requests. The script is separate from the Refinery's published script and
formula. It requires an explicit rig and reads ready queue IDs through
`gt mq list <rig> --ready --json`, so it does not depend on a bead prefix.

From a Gas Town workspace, run:

```bash
gastown_runtime/mayor/rig/scripts/operator-land.sh --rig gastown_runtime --queue 20
```

Or land one specific ready MR:

```bash
gastown_runtime/mayor/rig/scripts/operator-land.sh --rig gastown_runtime gtr-wisp-47e
```

The script checks that each MR belongs to the selected rig, is ready, and still
points to its recorded source commit. It rebases that branch onto the target
branch recorded in the MR, then makes a normal fast-forward push. Finally,
`gt mq post-merge` verifies the landed patch and handles issue and branch
cleanup. Rebase conflicts block the same MR with `needs-rebase` for its worker
to resolve. A failed push or cleanup is reported in the town log and returned
as a nonzero exit status.

The town root is discovered from the working directory or script path. Use
`--town <path>` or `GT_TOWN_ROOT` if the script is launched from outside that
workspace. `--clone <path>` or `GT_OPERATOR_LAND_CLONE` can select a dedicated
operator clone; the default is `<town>/<rig>/.runtime/operator-land`.
