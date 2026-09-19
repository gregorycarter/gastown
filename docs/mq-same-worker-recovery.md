# Admission-aware same-worker recovery

`gt mq resume hisn <MR> --json` queues a stopped original worker for Hisn's `same-mr-rebase` receipt protocol. `--dry-run --json` validates without creating a context. The command never starts a session itself.

The JSON contract is `{status, context?, mr, source, rig, worker, branch, head}`. `worker` is the bare original name, not an address. Status is `ready-to-queue` for dry run, `queued` after confirmed context publication, or `already-queued` for an identical existing context. Only the latter two include a context ID. Refusals exit nonzero; unknown publication preserves any created context for inspection and retry. Neither queueing nor a running session proves conflict resolution or landing.

The command is available to a neutral operator, the correctly identified Hisn Refinery, or Mayor. Other rig/worker identities are rejected. Hisn's Node recovery router can retain its own MR lock while calling this command: the Go command uses the existing sling source lock, not that MR directory lock.

## Admission and integrity

Enqueue, readiness, dispatch and the immediately-before-launch callback check the original blocked `needs-rebase` MR, full submitted SHA, worker/branch/source mapping, description hash and durable recovery receipt. A changed/closed/held MR or source, unknown dependency, inherited prerequisite, reassigned worker, dirty/missing/redirected worktree, wrong branch, foreign remote or changed pushed SHA refuses recovery. The source's own recorded molecule bond is retained; unrelated open prerequisites are not bypassed.

Enqueue is serialized against normal slings with the source lock. It creates only an ordinary scheduler context carrying the exact resume identity; no source, molecule, branch, convoy or MR is created. Multiple concurrent calls cannot create multiple contexts. An existing incompatible or duplicate context refuses visibly.

Only the normal daemon scheduler call executes recovery. It uses the existing pressure check and batch/capacity path, repeats the pause check before startup, reserves capacity and holds the source/assignee locks. An ad-hoc `gt scheduler run` skips recovery without consuming failure retries. The daemon's existing `GT_DAEMON` internal marker is a trusted local process convention, not a cryptographic authorization token; operators must not spoof it.

Startup never checks out, resets, stashes, mints a branch or reaps an existing session. It confirms the source hook write before launching and passes the validated recovery instructions to both startup-prompt and delayed-nudge runtimes. A hook failure prevents launch. The original source status may change to `hooked` and its assignee is set to its original worker; labels, workflow description and metadata must remain intact. A subsequent runtime-start failure leaves the source, worktree and recovery evidence intact for inspection, not force-close or deletion.

Recovery contexts do not enter orphan-molecule cleanup. Their success bookkeeping closes only the scheduler context after startup; the worker still resolves the conflict, runs the renewed exact-head preflight, pushes with an expected-old-head lease, and resubmits the same MR using Hisn's existing helper.

## Rig capacity

Explicit positive `max_polecats` values in `.beads-wisp/config/<rig>.json` now constrain the existing admission reservation path independently of the town cap. Actual rig polecat sessions plus in-flight reservations are counted under the same town admission lock. Hisn's configured four slots therefore remain four if the town cap rises; other rigs with no explicit local cap retain their existing behavior. Invalid or blocked explicit caps fail closed. Witness, Refinery and other rigs' sessions do not consume Hisn's four slots.

This covers supported sling and queued-resume admission. It does not retrofit every legacy raw operator session-start command, nor authorize their use to bypass the scheduler. The bounded recovery command requires enabled deferred scheduling even when a rig cap exists.

## Verification and limits

The focused Go tests use temporary Git repositories, in-memory issue/context adapters, fake `bd`/`tmux` commands and temporary admission locks. They cover wrong/stale/held identity, inherited gates, exact branch/head, hook/startup failure, pause/full/late-pause admission, concurrent queue and capacity reservations, four rig slots with a larger/unlimited town cap, JSON contract, and preservation of the original source/molecule/MR. They never contact production Dolt or start real agents. Existing pure capacity and daemon pressure tests exercise the shared scheduling primitives.

On macOS, use the ICU include/link flags exported by the Makefile when running Go directly:

```sh
CGO_CFLAGS=-I/opt/homebrew/opt/icu4c@78/include \
CGO_CXXFLAGS=-I/opt/homebrew/opt/icu4c@78/include \
CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib \
go test -race ./internal/cmd ./internal/polecat ./internal/scheduler/capacity ./internal/daemon \
  -run 'TestMQResume|TestPreservedSession|TestPlanDispatch|TestReconstruct|TestCPUPressureDeferred' -count=1
```

These tests are not a live conflict round-trip receipt. M0.4 still requires a real original-worker recovery and same-MR resubmission/landing rehearsal. Existing Beads status CAS and process-local coordination do not provide a cross-bead transaction: arbitrary external edits that ignore the assignment/MR locks can race a final check and require inspection. Git branch, source, molecule and MR evidence must be preserved on uncertain outcomes. Rollback is a reviewed code revert and restoration of the previous binary, not deletion of recovery evidence or workers.
