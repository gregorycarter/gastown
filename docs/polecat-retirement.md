# Completed polecat sandboxes

A directory is not an execution slot. Pending merge requests and recovery work
retain their original checkouts; completed, merged work does not need one.

Each daemon heartbeat runs `gt polecat retire --all --limit 2 --json`, before
pressure-gated dispatch. Parked/docked rigs are included: parking prevents new
execution, not reclamation of demonstrably completed work. No rig is unpaused
and no session is started. A backlog may take more
than one heartbeat to drain. The same guarded operation can be inspected or run:

```sh
gt polecat retire hisn --dry-run --limit 30 --json
gt polecat retire hisn --limit 30 --json
```

Retirement requires a dormant completed/idle identity, no hook or nonterminal
assignment (including blocked work), no pending MR or startup reservation, a
terminal source bead, and clean Git state without branch-local stashes. The
branch must already be represented on the rig's default target by ancestry or
rebased patch equivalence. A pushed feature branch alone is insufficient;
unlanded merge commits require review. Unknown database/session/Git state keeps
the directory. Empty leftover directories use the same lifecycle checks.

Startup, reuse and retirement share a per-worker filesystem lock. Retirement
also holds the assignment lock and repeats its checks before mutation. It does
not kill sessions or run wisp GC, reaper, compact, or product-bead deletion.

Before removing a Git worktree, its `.delivery` evidence and an identity/merge
receipt are preserved under `<rig>/.runtime/polecat-retirements/<worker>/<time>/`.
The disposable checkout, CodeGraph index and ignored build caches are removed.
The agent identity and history, product/MR beads and Git branch refs survive.
Recognized sibling `.cache/polecat-deps` dependency caches are retired under the
same worker locks after checking layout, ownership, filesystem and open files.
Read-only cache directories are made removable without following symlinks.
Unexpected siblings or uncertain cache ownership retain the checkout and cache.
Only an empty parent is removed; unexpected files are retained for inspection.
No recursive deletion fallback is used if Git worktree removal fails.

Receipts record physical filesystem free bytes before and after retirement and
their signed delta. This is an observation of shared host space, not a sum of
logical file sizes or a claim that concurrent writes did not happen. The existing
two-retirement/two-minute daemon budget remains unchanged.

Allocation also considers safe `done` workers for reuse, using the canonical
work-preservation check. This avoids new-directory growth if dispatch reaches a
completed worker before the next retirement pass. Worker and directory limits
are unchanged.
