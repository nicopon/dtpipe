#!/bin/bash
set -e

# validate_agent_boundary.sh
# Source-level boundary guard: the agent talks to a host through ITurnView and plain data, and
# names no terminal library.
#
# The agent (src/DtPipe/Cli/Agent/) is a loop over a model and a catalogue of tools. What a turn
# tells a person goes through ITurnView and TurnSummaryModel, and the host decides how it is shown.
# A console or a toolkit named under Agent/ would tie the loop to one host again, and a turn could
# no longer run, or be tested, without a terminal.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

pass() { echo -e "  ${GREEN}OK: $1${NC}"; }
fail() { echo -e "  ${RED}FAIL: $1${NC}"; exit 1; }

echo "========================================"
echo "    DtPipe Agent Boundary Validation"
echo "========================================"

AGENT_DIR="src/DtPipe/Cli/Agent"

# 1. Neither Spectre.Console nor Terminal.Gui under Agent/.
offenders="$(cd "$PROJECT_ROOT" && grep -rnE "Spectre\.Console|Terminal\.Gui" "$AGENT_DIR" --include="*.cs" 2>/dev/null \
    | grep -v "/obj/" | grep -v "/bin/" || true)"
if [ -n "$offenders" ]; then
    echo "$offenders" | sed 's/^/    /'
    fail "a console or a toolkit is named under ${AGENT_DIR}/"
fi
pass "${AGENT_DIR}/ names no console and no toolkit"

# 2. No project of the repository carries a full-screen toolkit: the engine, the adapters, the
#    standalone Arrow libraries and the CLI have no screen to own.
offenders="$(cd "$PROJECT_ROOT" && grep -rlE 'PackageReference[^>]*Terminal\.Gui' src/ \
    --include="*.csproj" --include="*.props" --include="*.targets" 2>/dev/null \
    | grep -v "/obj/" | grep -v "/bin/" || true)"
if [ -n "$offenders" ]; then
    echo "$offenders" | sed 's/^/    /'
    fail "a project takes a dependency on Terminal.Gui"
fi
pass "no project depends on Terminal.Gui"

# 3. No source names it either.
offenders="$(cd "$PROJECT_ROOT" && grep -rlE "using Terminal\.Gui|Terminal\.Gui\." src/ --include="*.cs" 2>/dev/null \
    | grep -v "/obj/" | grep -v "/bin/" || true)"
if [ -n "$offenders" ]; then
    echo "$offenders" | sed 's/^/    /'
    fail "Terminal.Gui is referenced from source"
fi
pass "no source references Terminal.Gui"

echo ""
echo -e "${GREEN}Agent boundary intact.${NC}"
