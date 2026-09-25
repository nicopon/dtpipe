#!/bin/bash
set -e

# validate_comments.sh
# 1. A code comment may only cite what a clone of this repository can open. Planning documents live
#    in .notes/, which .gitignore excludes, so "voie 4 §6 lot E2b" points at nothing. Names the
#    repository carries stay legal: F1-F7, F16, REFERENCE.md anchors.
# 2. Guidance files (CLAUDE.md at any depth, .claude/skills/) state rules, not history: no ISO
#    date, no cycle name, no commit hash. History lives in git.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

pass() { echo -e "  ${GREEN}OK: $1${NC}"; }
fail() { echo -e "  ${RED}FAIL: $1${NC}"; exit 1; }

echo "========================================"
echo "    DtPipe Comment Reference Validation"
echo "========================================"

# "voie 3" / "Voie 4 §6" · "lot E2b" / "lot C" · "§5.2" · "cycle 1.7"
PATTERN='[Vv]oie ?[0-9]|\blot [A-Z][0-9a-z]*\b|§[0-9]|[Cc]ycle 1\.[0-9]'

offenders="$(cd "$PROJECT_ROOT" && grep -rnE "$PATTERN" src/ tests/ \
    --include="*.cs" --include="*.sh" --include="*.csproj" 2>/dev/null \
    | grep -v "/obj/" | grep -v "/bin/" \
    | grep -v "^tests/scripts/validate_comments.sh:" || true)"   # it has to spell what it forbids

if [ -n "$offenders" ]; then
    echo "$offenders" | sed 's/^/    /'
    fail "a comment cites a planning document (.notes/ is gitignored — a clone cannot open it)"
fi
pass "no comment cites a document absent from the repository"

# Untracked files are included: a new CLAUDE.md must be checked before its first commit.
guidance="$(cd "$PROJECT_ROOT" && git ls-files -co --exclude-standard \
    | grep -E '(^|/)CLAUDE\.md$|^\.claude/skills/' || true)"

if [ -z "$guidance" ]; then
    fail "no guidance file found — the discovery rule no longer matches the tree"
fi

# An ISO date, a cycle name, or a commit hash (7-12 hex digits holding both a digit and a letter).
history="$(cd "$PROJECT_ROOT" && echo "$guidance" | tr '\n' '\0' | xargs -0 perl -ne '
    print "$ARGV:$.: $_" if /20[0-9]{2}-[0-9]{2}-[0-9]{2}/
        || /[Cc]ycle [0-9]/ || /[Vv]oie ?[0-9]/ || /§[0-9]/
        || /(?<![0-9A-Za-z_])(?=[0-9a-f]*[0-9])(?=[0-9a-f]*[a-f])[0-9a-f]{7,12}(?![0-9A-Za-z_])/;
    close ARGV if eof;
')"

if [ -n "$history" ]; then
    echo "$history" | sed 's/^/    /'
    fail "a guidance file carries history (date, cycle, commit hash) — it belongs in git"
fi
pass "guidance files state rules, not history ($(echo "$guidance" | wc -l | tr -d ' ') files)"

echo ""
echo -e "${GREEN}Comment references intact.${NC}"
