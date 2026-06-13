# Done — Submit Work to Merge Queue

Signal that your work is complete and ready for the merge queue.

Arguments: $ARGUMENTS

## Pre-flight Checks

Before running `gt done`, verify your work is ready:

```bash
git status                          # Must be clean (no uncommitted changes)
git log --oneline origin/main..HEAD # Must have at least 1 commit
```

If there are uncommitted changes, commit them first:
```bash
git add <files>
git commit -m "<type>: <description>"
```

## Land It GREEN — CI is part of "done"

On rigs with CI (e.g. **bridge_town_core**), **"done" means your PR is green**, not just
pushed. Pushing STARTS CI; it is not the finish line. After your branch is pushed:

```bash
SHA=$(git rev-parse HEAD)
gh pr checks                         # or: gh run list --commit "$SHA" --limit 10
```

Wait for the `CI` workflow (`ci.yml`) **conclusion == success** for your SHA, then run `gt done`.

- **If CI goes red, it is YOURS to fix.** Read the failing job and loop until green:
  ```bash
  gh run view <run-id> --log-failed   # find the real failure
  # fix the cause, commit, push, re-check — repeat: diagnose -> fix -> re-push
  ```
  Do NOT `gt done --status ESCALATED` just to move on, and do NOT merge red. A red PR you
  abandon strands the work and breaks main for everyone behind you.
- **`pending` is NOT a pass.** If checks sit pending, distinguish *still running* from
  *runners offline*: `gh api repos/Bridge-Town/bridge-town-core/actions/runners`. If runners
  are offline, recover them (`cd ci-runner && docker compose up -d`) and let CI run.
  NEVER treat stuck-pending as success.

Only once CI is green is the bead's definition-of-done satisfied — then run `gt done`.

## Execute

Run `gt done` with any provided arguments:

```bash
gt done $ARGUMENTS
```

**Common usage:**
- `gt done` — Submit completed work (default: --status COMPLETED)
- `gt done --pre-verified` — Submit with pre-verification (you ran gates after rebase)
- `gt done --status ESCALATED` — Signal blocker, skip MR
- `gt done --status DEFERRED` — Pause work, skip MR

**If the bead has nothing to implement** (already fixed, can't reproduce):
```bash
bd close <issue-id> --reason="no-changes: <brief explanation>"
gt done
```

This command pushes your branch, submits an MR to the merge queue, and transitions
you to IDLE. The Refinery handles the actual merge. You are done after this.
