#!/bin/bash
set -e

# validate_commit_trailers.sh
# No assistant attribution in the git history. Nicolas does not want Claude, Anthropic or any
# tool byline appearing in this repository's commits or PR bodies.
#
# Why this is a script and not a line of documentation: the rule was written down three times
# (CLAUDE.md, the plan documents' survival rules, and the session memory) and broken three times,
# because a harness-level attribution default can instruct the opposite and a model reasons about
# which instruction wins instead of applying the rule. Twelve commits have been rewritten after
# the fact so far. A grep does not reason.
#
# Scope: commits not yet pushed (@{u}..HEAD) — the set that can still be rewritten cheaply with
# git rebase. Once a commit is pushed the fix is a force-push, which is a different decision.
# With no upstream, falls back to a recent window so the check still says something.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$PROJECT_ROOT"

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

pass() { echo -e "  ${GREEN}OK: $1${NC}"; }
fail() { echo -e "  ${RED}FAIL: $1${NC}"; exit 1; }

FALLBACK_WINDOW=50

echo "========================================"
echo "    DtPipe Commit Trailer Validation"
echo "========================================"

if UPSTREAM="$(git rev-parse --abbrev-ref --symbolic-full-name '@{u}' 2>/dev/null)"; then
    RANGE="$UPSTREAM..HEAD"
    SCOPE="unpushed commits ($RANGE)"
else
    RANGE="-$FALLBACK_WINDOW"
    SCOPE="last $FALLBACK_WINDOW commits (no upstream)"
fi

COUNT="$(git log --format='%H' $RANGE 2>/dev/null | wc -l | tr -d ' ')"

if [ "$COUNT" = "0" ]; then
    pass "nothing to check — $SCOPE is empty"
    echo ""
    echo -e "${GREEN}Commit trailer checks passed.${NC}"
    exit 0
fi

# Matched case-insensitively against the full message of each commit. Kept deliberately broad:
# the rule is "no assistant byline", not "no one exact trailer".
PATTERN='co-authored-by:.*(claude|anthropic|noreply@anthropic)|generated with \[?claude|🤖 generated with'

OFFENDERS=""
while read -r SHA; do
    [ -z "$SHA" ] && continue
    if git log -1 --format='%B' "$SHA" | grep -qiE "$PATTERN"; then
        OFFENDERS="$OFFENDERS\n  $(git log -1 --format='%h %s' "$SHA")"
    fi
done < <(git log --format='%H' $RANGE 2>/dev/null)

if [ -n "$OFFENDERS" ]; then
    echo -e "$OFFENDERS"
    echo ""
    echo "  Rewrite them before pushing:"
    echo "    git rebase -i $RANGE   (reword each, drop the trailer)"
    fail "assistant attribution found in $SCOPE"
fi

pass "no assistant attribution in $SCOPE ($COUNT commit(s))"
echo ""
echo -e "${GREEN}Commit trailer checks passed.${NC}"
