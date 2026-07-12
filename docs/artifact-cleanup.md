# Lifecycle Artifact Cleanup

Gas Town can account for and remove reproducible build/cache directories without
treating an entire rig or runner workspace as disposable. The policy is deny by
default: a manual command is a dry run, automatic cleanup is disabled, and each
candidate needs an exact relative allowlist rule.

## Commands

```bash
# Uses the merged town + rig policy. Does not delete anything.
gt cleanup artifacts --rig bridge_town_core --scope rig --json

# Manual mutation needs an explicit apply flag and MR verification.
gt cleanup artifacts --rig bridge_town_core --scope rig \
  --verify-no-active-mr --apply

# A CI maintenance dry run. Paths may be supplied while evaluating a policy.
gt cleanup artifacts --scope ci \
  --path 'work/*/.pnpm-store' --path 'shared/trivy-cache' --json

# CI apply is host-side only and requires town policy, installed runner hooks,
# no active job/handoff markers, and the explicit operator verification.
gt cleanup artifacts --scope ci --verify-runners-idle --apply

# Compact, read-only top-consumer report.
gt cleanup disk --top 15 --json

# Dolt is accounting/recommendation-only, even if --apply is supplied.
gt cleanup artifacts --scope dolt --json
```

`--root` is available for a specific polecat or dry-run fixture but must resolve
inside the town workspace. Mutating CI maintenance is restricted to the
canonical `<town>/ci-runner` root, and protected paths remain anchored above a
narrowed root. Mutating rig cleanup must use its canonical rig worktree;
mutating polecat cleanup must use a registered polecat worktree top level. It
cannot select the town root except for the report-only Dolt scope. JSON mode
writes one JSON document with no styled prose.

## Policy

Town defaults live at `settings/config.json`; rig overrides live at
`<rig>/settings/config.json`. Rig values replace town scalar/list values when
present. A rig is the only level allowed to opt business data out of protection.

```json
{
  "type": "rig-settings",
  "version": 1,
  "lifecycle": {
    "cleanup": {
      "enabled": true,
      "mode": "apply",
      "paths": [
        "target",
        ".next",
        "coverage",
        "dist",
        ".pytest_cache",
        ".ruff_cache",
        ".mypy_cache",
        ".pnpm-store"
      ],
      "protected_paths": ["generated/release-evidence"],
      "max_age": "168h",
      "max_bytes": 10737418240,
      "on_polecat_reuse": true,
      "on_post_merge": false,
      "on_ci_maintenance": false
    }
  }
}
```

- `enabled` and `mode=apply` authorize unattended hooks. A manual command still
  needs `--apply`; manual `--apply` is itself the enable switch.
- `paths` is the complete relative allowlist. Globs are allowed only below a
  literal first component, for example `work/*/.pnpm-store`; `*`, `..`, absolute
  paths, recursive `**`, and root selection are rejected.
- `max_age` uses the newest descendant modification time, so one recently used
  file keeps its containing cache. Zero/omitted disables the age threshold.
- `max_bytes` is the retained high-water mark across eligible candidates. The
  oldest candidates are selected until eligible bytes fall below it. Zero/omitted
  disables the size threshold.
- `protected_paths` adds rig/project protections; it never subtracts built-ins.
- `allow_protected_paths` is a rig-only second key for deliberate business-data
  retention. The same path must also appear in `paths`. It may opt in only
  `data`, `ml/models`, or `seif_ingestion/checkpoints*`. Use this only with a
  documented backup/retention plan.

The following metadata, secrets, and runner-managed setup roots are permanently
protected and cannot be overridden: `.git`, `.repo.git`, `.beads`, `.dolt-*`,
`.env*`, `secrets`, `_actions`, `_temp`, `_work`, and `_tool`.
Business data is protected by default: `data`, `ml/models`, and
`seif_ingestion/checkpoints*`. Permanent names are protected even when nested
inside an allowlisted parent; business/custom protections are anchored at the
worktree policy root so a generated cache subdirectory merely named `data`
does not disable cleanup.

## Safety and accounting

Before deletion, the engine requires containment under the canonical root,
rejects every symlink path component, rejects tracked paths, and (for Git
worktrees) requires candidates to be Git-ignored. Directory scans fail closed
on an unreadable entry or Unix filesystem/mount boundary. Permanent metadata or
secret descendants block deletion of their parent candidate. It refuses
mutation for dirty/untracked non-ignored work, open or unverified MRs, and
active or unverified runner jobs. Immediately before mutation it repeats the
lifecycle and Git checks, verifies candidate identity/content/age, then deletes
through a confined `os.Root` handle rather than an unrestricted pathname.

Manual worktree cleanup cannot infer authoritative MR state from the absence of
a marker; `--verify-no-active-mr` records that the operator checked. Likewise,
CI apply requires `--verify-runners-idle`, and manual polecat apply additionally
requires `--verify-polecat-idle`. These flags never override a detected active
marker or dirty worktree.

Every result records `bytes_considered`, `bytes_eligible`, `bytes_freed`, paths
cleaned or planned, paths skipped with reasons, scope, and hook point. Protected
Dolt paths contribute to considered bytes but remain ineligible. Byte counts are
logical-size estimates (not APFS clone or sparse-file physical blocks); partial
removal is remeasured and marked `apply_incomplete`. A refused or incomplete
manual apply exits nonzero after emitting its report.

## Lifecycle placement

The first automatic hook is `polecat-pre-reuse`. It runs only after Gas Town's
canonical reuse decision has established an idle slot, clean Git state, and no
pending MR. Cleanup happens before the new branch assignment, when previous
ignored build products are no longer evidence for the completed assignment.
The decision is re-read before mutation. Hook errors are logged as structured
accounting and remain non-fatal to dispatch.

The legacy `polecat.target_clean_policy` remains compatible and still controls
its narrow `target/` cleaner. The generic hook is a sibling and is opt-in.

Post-merge cleanup is intentionally not enabled by this initial hook: merge/CI
evidence may still be needed immediately after landing. A future post-merge
caller can use `on_post_merge` only after it proves evidence retention complete.

## CI runner policy

Persistent runner workdirs and shared caches need a job protocol, not a broad
`rm -rf`:

1. Install start/completion hooks on every selected persistent runner. The
   Bridge Town runner configuration writes `.ci-job-hooks-installed` and
   publishes `shared/.ci-active-jobs/<runner>/.job-active` at job start; only
   the runner-level completion wrapper removes that runner entry after post-job
   cleanup. The sentinel must contain the exact `mkdir-v1` protocol version.
   Before acquisition, Gas Town verifies every live runner in the exact Compose
   project: current environment, canonical start/completion paths, byte-identical
   executable hook/helper scripts, and exact read-only `scripts` plus writable
   `shared` and per-runner `work` bind sources. Any uninstrumented or stale
   runner family blocks maintenance.
2. Keep `.handoff-active` inside each run-scoped coverage/auth handoff directory
   until the final consumer removes that directory. Either marker blocks the
   whole CI cleanup run; a crash intentionally fails closed.
3. Configure explicit nested allowlists such as `work/*/.pnpm-store` and
   `shared/trivy-cache`, plus meaningful age/byte thresholds. Runner-managed
   `_actions`, `_temp`, `_work`, and `_tool` paths are permanently denied:
   GitHub Runner can mutate them while preparing actions before the configured
   started-job hook runs, so the hook protocol cannot prove them idle.
4. Set `lifecycle.cleanup.on_ci_maintenance=true` in town settings. Apply only
   against `<town>/ci-runner`. A GitHub Actions job self-identifies as active and
   cannot clean its own persistent workdir. Gas Town generates a cryptographic
   nonce and uses `docker exec` in one verified runner to invoke
   `scripts/ci-maintenance-protocol.sh acquire <nonce>`; it proceeds only after
   the helper returns the exact nonce attestation. Job start, completion, and
   maintenance all serialize through the runner-side `.ci-protocol-mutex`
   atomic-mkdir protocol. The helper refuses acquisition while
   `.ci-active-jobs` has any child or any `.handoff-active` exists, then publishes
   `.maintenance-active`. This keeps the race decision in one Linux container
   filesystem domain instead of relying on macOS-to-Docker advisory-lock or
   visibility semantics. Gas Town re-verifies the complete live runner set before
   each deletion and surfaces release failures.
5. Run package-native pruning first (`pnpm store prune`, Trivy scan-cache clean,
   and BuildKit cache pruning on a dedicated CI builder). Reclaim runner-managed
   setup/checkouts only through a separately cordoned runner recreation process,
   never through artifact-path deletion.

Runner container writable layers should be made reproducible and recreated on
a maintenance cadence; they are not host-path artifact candidates. Docker
maintenance must select only CI-labelled containers/images/builders, refuse
running jobs, omit `docker volume prune -a`, and use Docker/BuildKit native
prune filters and byte caps. Application images and named volumes are outside
this policy. A crash or lost runner while `.maintenance-active` is owned fails
closed; an operator must verify runner/job/handoff state and use the same helper
recovery procedure rather than deleting protocol directories opportunistically.

## Dolt retention

`.dolt-data` is the live shared database store and `.dolt-backup` is recovery
state, not an artifact cache. Direct filesystem deletion is permanently denied.
The `dolt` scope only reports their sizes and recommendations.

Before native Dolt compaction or backup expiration:

1. Check `bd vc status`, configured Dolt remotes, and backup recency/integrity.
2. Quiesce writers and use Dolt's native GC/backup mechanisms in a maintenance
   window; never delete chunk files by age.
3. Treat `hq` as control-plane data shared across rigs. Its issue, agent, mail,
   and MR state can outlive any one checkout, so require a verified backup and
   remote before changing retention.
4. Record before/after sizes for `.dolt-data`, `.dolt-backup`, and `hq`
   separately. If no verified remote exists, retain rather than prune.
