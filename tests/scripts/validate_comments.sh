#!/bin/bash
set -e

# validate_comments.sh
# A comment may only cite what a clone of this repository can open.
#
# Planning documents live in .notes/, which .gitignore excludes. A comment that says "voie 4 §6
# lot E2b" therefore points at a file nobody but the author has, and it says nothing a reader can
# act on — CLAUDE.md's Comments section already rules it out ("that history already lives in git,
# .notes/ and CHANGELOG.md"). Forty-four of them accumulated anyway, because the rule had no check.
#
# They are also self-propagating: every contributor, human or model, reads the surrounding code and
# matches its idiom. That is the mechanism this script exists to stop.
#
# Names that ARE resolvable stay legal: F1-F7 and F16 are specified in CLAUDE.md and REFERENCE.md,
# both committed. The test is not "does it look like a reference" but "can a clone open it".

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

echo ""
echo -e "${GREEN}Comment references intact.${NC}"
