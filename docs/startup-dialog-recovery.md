# Stale Codex startup dialogs

Hisn's normal scheduler launch on 2026-09-19 exposed a false rollback: a worker
already executing its task was reported as blocked by workspace trust text left
in terminal scrollback. A later normal scheduler retry succeeded. No trust scope,
agent configuration, session, or scheduler bypass was changed for this repair.

## Sanitized evidence

All times below are UTC. Only native event kinds and timestamps are recorded;
conversation content and authentication files are not included.

- Shared Codex configuration was modified at 06:04:12; subsequent inspection
  found the exact worker-project trust entry. This is consistent with accepted
  trust, but the file timestamp alone does not identify its writer. The failed
  launch's native transcript records `task_started` at 06:04:12.995.
- That same transcript records an assistant message at 06:04:19.465, a tool call
  at 06:04:20.267, its output at 06:04:21.399, and another assistant message at
  06:04:26.796. The agent was executing, not waiting for trust approval.
- The daemon subsequently recorded `interactive startup dialog still visible`
  with `workspace trust prompt` and ran its ordinary failed-start rollback.
- A normal scheduler retry launched at 06:08:34 and was observed working. Its
  native transcript begins at 06:08:35. No intervention in that session was used
  to validate or apply this patch.

The failed terminal pane was not retained, so its exact layout is unknown. The
code-level reproducer is a trust marker followed by the observed Codex 0.155
composer, `› Ask Codex to do anything`. The previous suffix-only prompt matcher
reported that synthetic active-composer layout as blocked. Five stale-modal
regression cases failed before the patch; genuine-dialog cases remained blocked.

## Bounded correction

`internal/tmux/tmux.go` recognizes the complete known composer line, normalizing
whitespace, only after the last known startup-modal marker. A selected numbered
menu row, arbitrary chevron text, a quoted example, or an old composer preceding
a new dialog does not qualify. This allows the existing startup check to stop
mistaking accepted dialog scrollback for a live modal, and prevents Enter/Down
from being sent into an already-running composer by the acceptance helpers.

The ordinary handling of genuine trust and bypass dialogs, timeouts, general
shell/agent prompt detection, and busy detection are unchanged. No new trust
entry is written by this code, no blind-dismiss routine is used, and no force
dispatch or lifecycle workaround is added. Unknown future composer text remains
conservative and may need a separately captured, reviewed regression.

Project trust remains a separate Codex setting; see the
[official configuration reference](https://learn.chatgpt.com/docs/config-file/config-reference).
This fix changes observation of an already-started agent, not that setting.

## Verification

Focused race-enabled tests include synthetic fake-tmux captures and key-send
recording. An active composer causes zero key sends and no blocked-start error;
an undismissed genuine trust modal still gets the existing single Enter attempt
and then refuses startup after the normal timeout. Pure cases cover trust,
bypass, update, stale/new ordering, menu rows, and unknown placeholders. The test
package uses an isolated tmux test socket; the new fake-tmux fixtures contact no
live agents or Beads/Dolt database.

```sh
CGO_CFLAGS=-I/opt/homebrew/opt/icu4c@78/include \
CGO_CXXFLAGS=-I/opt/homebrew/opt/icu4c@78/include \
CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib \
go test -race -p 1 ./internal/tmux \
  -run '^Test(ContainsPromptIndicator|ContainsWorkspaceTrustDialog|ContainsBlockingStartupDialog|StartupDialogs_)' \
  -count=1
```

The initial focused patched run passed all 42 tests/subtests. The successful live
worker predated this patch and is not a live deployment receipt for it. Rollout
and any next startup observation remain operator-owned; reverting this small
code change does not require deleting trust records, native archives, or work.
