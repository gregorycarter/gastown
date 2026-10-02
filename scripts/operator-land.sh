#!/usr/bin/env bash
# Operator-managed merge-queue lander for rigs whose Refinery is unavailable.
# It deliberately lives outside the published Refinery formula and requires an
# explicit rig so queue IDs, Beads records, and target branches stay aligned.
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
RIG=""
TOWN_ROOT="${GT_TOWN_ROOT:-}"
CLONE=""
QUEUE=0
LIMIT=100
MR_IDS=()

usage() {
  cat <<'USAGE'
Usage:
  operator-land.sh --rig <rig> [--town <town-root>] [--clone <path>] <mr-id>...
  operator-land.sh --rig <rig> [--town <town-root>] [--clone <path>] --queue [limit]

Land ready MRs by rebasing each submitted branch onto the MR's recorded target,
fast-forward pushing that target, and asking `gt mq post-merge` to verify the
landing and perform normal Beads and branch cleanup.

Options:
  --rig <name>       Required rig name, for example gastown_runtime
  --town <path>      Town root (otherwise found from the working directory or script path)
  --clone <path>     Dedicated operator clone (default: <town>/<rig>/.runtime/operator-land)
  --queue [limit]    Land up to limit ready MRs in score order (default limit: 100)
  -h, --help         Show this help

Examples:
  scripts/operator-land.sh --rig gastown_runtime --queue 20
  scripts/operator-land.sh --rig gastown_runtime gtr-wisp-47e
USAGE
}

fail() {
  printf 'operator-land: %s\n' "$*" >&2
  exit 2
}

while (($#)); do
  case "$1" in
    --rig)
      (($# >= 2)) || fail "--rig requires a value"
      RIG="$2"
      shift 2
      ;;
    --town)
      (($# >= 2)) || fail "--town requires a path"
      TOWN_ROOT="$2"
      shift 2
      ;;
    --clone)
      (($# >= 2)) || fail "--clone requires a path"
      CLONE="$2"
      shift 2
      ;;
    --queue)
      QUEUE=1
      shift
      if (($#)) && [[ "$1" =~ ^[0-9]+$ ]]; then
        LIMIT="$1"
        shift
      fi
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    --)
      shift
      while (($#)); do MR_IDS+=("$1"); shift; done
      ;;
    -* )
      fail "unknown option: $1"
      ;;
    *)
      MR_IDS+=("$1")
      shift
      ;;
  esac
done

[[ -n "$RIG" ]] || fail "--rig is required"
[[ "$RIG" != */* && "$RIG" != . && "$RIG" != .. ]] || fail "invalid rig name: $RIG"
[[ "$LIMIT" =~ ^[0-9]+$ && "$LIMIT" -gt 0 ]] || fail "queue limit must be a positive integer"
if ((QUEUE)) && ((${#MR_IDS[@]})); then
  fail "pass MR IDs or --queue, not both"
fi
if ((!QUEUE)) && ((${#MR_IDS[@]} == 0)); then
  usage >&2
  exit 2
fi

find_town_root() {
  local dir="$1"
  [[ -d "$dir" ]] || return 1
  dir="$(cd "$dir" && pwd -P)" || return 1
  while :; do
    if [[ -f "$dir/mayor/rigs.json" ]]; then
      printf '%s\n' "$dir"
      return 0
    fi
    [[ "$dir" == / ]] && return 1
    dir="$(dirname "$dir")"
  done
}

if [[ -n "$TOWN_ROOT" ]]; then
  TOWN_ROOT="$(cd "$TOWN_ROOT" && pwd -P)" || fail "town root does not exist: $TOWN_ROOT"
else
  TOWN_ROOT="$(find_town_root "$PWD" || find_town_root "$SCRIPT_DIR")" ||
    fail "could not find a town root; pass --town or set GT_TOWN_ROOT"
fi

for tool in bd git gt jq; do
  command -v "$tool" >/dev/null 2>&1 || fail "required command is missing: $tool"
done

RIG_ROOT="$TOWN_ROOT/$RIG"
RIGDIR="$RIG_ROOT/mayor/rig"
if [[ ! -d "$RIGDIR/.git" && ! -f "$RIGDIR/.git" ]]; then
  if [[ -d "$RIG_ROOT/refinery/rig/.git" || -f "$RIG_ROOT/refinery/rig/.git" ]]; then
    RIGDIR="$RIG_ROOT/refinery/rig"
  else
    fail "could not find $RIG's mayor/rig or refinery/rig clone under $RIG_ROOT"
  fi
fi

if [[ -z "$CLONE" ]]; then
  CLONE="${GT_OPERATOR_LAND_CLONE:-$RIG_ROOT/.runtime/operator-land}"
fi
if [[ "$CLONE" != /* ]]; then
  CLONE="$PWD/$CLONE"
fi
mkdir -p "$(dirname "$CLONE")"
CLONE="$(cd "$(dirname "$CLONE")" && pwd -P)/$(basename "$CLONE")"
LOCK="$CLONE.operator-land-lock"
LOG="${GT_OPERATOR_LAND_LOG:-$TOWN_ROOT/logs/operator-land-$RIG.log}"

mkdir -p "$(dirname "$LOG")" "$(dirname "$LOCK")"
if ! mkdir "$LOCK" 2>/dev/null; then
  fail "another operator lander is using $CLONE (remove $LOCK after confirming it is stale)"
fi
trap 'rmdir "$LOCK" 2>/dev/null || true' EXIT

ts() { date -u '+%Y-%m-%dT%H:%M:%SZ'; }
say() {
  printf '%s %s\n' "$(ts)" "$*" | tee -a "$LOG"
}
bdq() { (cd "$RIGDIR" && bd "$@"); }
gtq() { (cd "$TOWN_ROOT" && gt "$@"); }
field() {
  local json="$1" key="$2"
  printf '%s' "$json" | jq -r --arg key "$key" \
    '.[0].description // "" | split("\n") | map(select(startswith($key + ": "))) | first // "" | ltrimstr($key + ": ")'
}

queue_ids() {
  gtq mq list "$RIG" --ready --json | jq -r '.[].id'
}

origin_url="$(git -C "$RIGDIR" remote get-url origin 2>/dev/null)" ||
  fail "could not read origin URL from $RIGDIR"
[[ -n "$origin_url" ]] || fail "origin URL is empty for $RIGDIR"

prepare_clone() {
  if [[ ! -e "$CLONE" ]]; then
    mkdir -p "$(dirname "$CLONE")"
    git clone --quiet "$origin_url" "$CLONE" || fail "could not clone $origin_url into $CLONE"
  fi
  git -C "$CLONE" rev-parse --git-dir >/dev/null 2>&1 || fail "operator clone is not a Git repository: $CLONE"
  local clone_url
  clone_url="$(git -C "$CLONE" remote get-url origin 2>/dev/null)" || fail "operator clone has no origin remote: $CLONE"
  [[ "$clone_url" == "$origin_url" ]] || fail "operator clone origin differs from $RIG origin"
  [[ ! -d "$CLONE/.git/rebase-merge" && ! -d "$CLONE/.git/rebase-apply" ]] ||
    fail "operator clone has an unfinished rebase: $CLONE"
  [[ -z "$(git -C "$CLONE" status --porcelain)" ]] || fail "operator clone has local changes: $CLONE"
}

mark_needs_rebase() {
  local mr="$1" worker="$2" target="$3" details="$4"
  bdq update "$mr" --status blocked >/dev/null
  bdq label add "$mr" needs-rebase >/dev/null
  bdq comments add "$mr" "operator-land $(ts): rebase conflict onto origin/$target. Resolve on the same branch and MR, then push and resubmit. Details: $details (worker: $worker)" >/dev/null
}

land_one() {
  local requested="$1" meta mr status branch target source sha worker labels
  local source_meta source_status source_labels ready_json current_meta current_status
  local branch_head land_head output

  meta="$(bdq show "$requested" --json 2>/dev/null)" || { say "SKIP $requested: bd show failed"; return 1; }
  mr="$(printf '%s' "$meta" | jq -r '.[0].id // empty')"
  [[ -n "$mr" ]] || { say "SKIP $requested: not found"; return 1; }
  status="$(printf '%s' "$meta" | jq -r '.[0].status // empty')"
  branch="$(field "$meta" branch)"
  target="$(field "$meta" target)"
  source="$(field "$meta" source_issue)"
  sha="$(field "$meta" commit_sha)"
  worker="$(field "$meta" worker)"
  labels="$(printf '%s' "$meta" | jq -r '.[0].labels // [] | join(",")')"

  [[ "$(field "$meta" rig)" == "$RIG" ]] || { say "SKIP $mr: MR does not belong to rig $RIG"; return 1; }
  [[ "$status" == open ]] || { say "SKIP $mr: status=$status"; return 1; }
  [[ -n "$branch" && -n "$target" && -n "$source" && -n "$sha" ]] || { say "SKIP $mr: missing branch, target, source issue, or commit SHA"; return 1; }
  git check-ref-format --branch "$branch" >/dev/null 2>&1 || { say "SKIP $mr: invalid branch name: $branch"; return 1; }
  git check-ref-format --branch "$target" >/dev/null 2>&1 || { say "SKIP $mr: invalid target branch: $target"; return 1; }
  [[ ",$labels," != *,needs-operator-rollout,* ]] || { say "SKIP $mr: source requires operator rollout"; return 1; }

  ready_json="$(gtq mq list "$RIG" --ready --json 2>/dev/null)" || { say "SKIP $mr: could not read ready queue for $RIG"; return 1; }
  printf '%s' "$ready_json" | jq -e --arg id "$mr" 'any(.[]; .id == $id)' >/dev/null || {
    say "SKIP $mr: not ready in $RIG merge queue"
    return 1
  }

  source_meta="$(bdq show "$source" --json 2>/dev/null)" || { say "SKIP $mr: source issue $source could not be read"; return 1; }
  source_status="$(printf '%s' "$source_meta" | jq -r '.[0].status // empty')"
  source_labels="$(printf '%s' "$source_meta" | jq -r '.[0].labels // [] | join(",")')"
  [[ -n "$source_status" ]] || { say "SKIP $mr: source issue $source could not be found"; return 1; }
  [[ "$source_status" != closed ]] || { say "SKIP $mr: source $source is already closed"; return 1; }
  [[ "$source_status" != blocked ]] || { say "SKIP $mr: source $source is blocked"; return 1; }
  [[ ",$source_labels," != *,needs-operator-rollout,* ]] || { say "SKIP $mr: source $source requires operator rollout"; return 1; }

  # A previous run may have pushed successfully and stopped before queue cleanup.
  # gt verifies exact or complete patch-equivalent landing before changing Beads.
  if output="$(gtq mq post-merge "$RIG" "$mr" 2>&1)"; then
    say "ALREADY LANDED $mr: $output"
    return 0
  fi
  current_meta="$(bdq show "$mr" --json 2>/dev/null)" || { say "CLEANUP CHECK $mr: $output"; return 1; }
  current_status="$(printf '%s' "$current_meta" | jq -r '.[0].status // empty')"
  if [[ "$current_status" != open ]]; then
    say "CLEANUP REQUIRED $mr: post-merge reported '$output' and MR status is $current_status"
    return 1
  fi

  prepare_clone
  git -C "$CLONE" fetch --quiet origin \
    "+refs/heads/$target:refs/remotes/origin/$target" \
    "+refs/heads/$branch:refs/remotes/origin/$branch" || {
      say "SKIP $mr: could not fetch target $target or branch $branch"
      return 1
    }
  branch_head="$(git -C "$CLONE" rev-parse "refs/remotes/origin/$branch")" || return 1
  [[ "$branch_head" == "$sha" ]] || {
    say "SKIP $mr: recorded SHA ${sha:0:12} differs from origin/$branch head ${branch_head:0:12}; resubmit the same MR"
    return 1
  }

  git -C "$CLONE" checkout --quiet --detach "refs/remotes/origin/$target" || return 1
  git -C "$CLONE" checkout --quiet -B operator-land "refs/remotes/origin/$branch" || return 1
  if ! git -C "$CLONE" rebase "refs/remotes/origin/$target"; then
    local conflict_files
    conflict_files="$(git -C "$CLONE" diff --name-only --diff-filter=U | tr '\n' ' ')"
    git -C "$CLONE" rebase --abort >/dev/null 2>&1 || true
    mark_needs_rebase "$mr" "$worker" "$target" "conflicts: ${conflict_files:-rebase failed}"
    say "CONFLICT $mr ($source, $worker): ${conflict_files:-rebase failed}; MR blocked with needs-rebase"
    return 1
  fi

  land_head="$(git -C "$CLONE" rev-parse HEAD)"
  if ! git -C "$CLONE" push --quiet origin "HEAD:refs/heads/$target"; then
    say "PUSH FAILED $mr: target $target moved or rejected the update; retry after checking origin/$target"
    return 1
  fi
  say "PUSHED $mr ($source, $worker): ${sha:0:12} -> $target at ${land_head:0:12}"

  if output="$(gtq mq post-merge "$RIG" "$mr" 2>&1)"; then
    say "LANDED $mr: $output"
    return 0
  fi
  say "CLEANUP REQUIRED $mr: target was pushed, but gt mq post-merge failed: $output"
  return 1
}

if ((QUEUE)); then
  ids="$(queue_ids)" || fail "could not read ready queue for $RIG"
  MR_IDS=()
  if [[ -n "$ids" ]]; then
    while IFS= read -r id; do
      [[ -n "$id" ]] && MR_IDS+=("$id")
    done <<< "$ids"
  fi
  ((${#MR_IDS[@]})) || { say "QUEUE EMPTY $RIG"; exit 0; }
  say "QUEUE START $RIG: processing up to $LIMIT ready MRs"
fi

prepare_clone
failures=0
processed=0
for mr in "${MR_IDS[@]}"; do
  ((processed < LIMIT)) || break
  land_one "$mr" || failures=$((failures + 1))
  processed=$((processed + 1))
done

if ((failures)); then
  say "COMPLETE $RIG: $failures of $processed selected MR(s) need attention"
  exit 1
fi
say "COMPLETE $RIG: $processed MR(s) landed or were already landed"
