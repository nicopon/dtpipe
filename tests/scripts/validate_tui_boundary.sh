#!/bin/bash
set -e

# validate_tui_boundary.sh
# Source-level boundary guard: Terminal.Gui is the agent's full-screen surface and nothing else.
#
# Two rendering stacks coexist on purpose and they are NOT interchangeable. Spectre.Console emits
# text into a stream — that is what scrollback, pipes and CI assert against. Terminal.Gui owns the
# screen, and a piped run has no screen to own. Letting the toolkit spread past the agent surface
# is how the piped path — the project's non-regression proof — would quietly stop being reachable.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

pass() { echo -e "  ${GREEN}OK: $1${NC}"; }
fail() { echo -e "  ${RED}FAIL: $1${NC}"; exit 1; }

echo "========================================"
echo "    DtPipe TUI Boundary Validation"
echo "========================================"

TUI_DIR="src/DtPipe/Cli/Agent/Tui"

# 1. Terminal.Gui may only be referenced from the agent's full-screen surface.
offenders="$(cd "$PROJECT_ROOT" && grep -rlE "using Terminal\.Gui|Terminal\.Gui\." src/ --include="*.cs" 2>/dev/null \
    | grep -v "^${TUI_DIR}/" | grep -v "/obj/" | grep -v "/bin/" || true)"
if [ -n "$offenders" ]; then
    echo "$offenders" | sed 's/^/    /'
    fail "Terminal.Gui referenced outside ${TUI_DIR}/"
fi
pass "Terminal.Gui confined to ${TUI_DIR}/"

# 2. Only the CLI project may carry the package. The engine, the adapters and the standalone
#    Arrow libraries have no terminal at all.
for proj in DtPipe.Core DtPipe.Adapters DtPipe.Adapters.Shared DtPipe.Processors DtPipe.Transformers \
            Apache.Arrow.Ado Apache.Arrow.Serialization; do
    if [ -d "$PROJECT_ROOT/src/$proj" ] && \
       grep -rq 'PackageReference[^>]*Terminal\.Gui' "$PROJECT_ROOT/src/$proj/" --include="*.csproj" 2>/dev/null; then
        fail "$proj takes a dependency on Terminal.Gui"
    fi
done
pass "only the CLI project depends on Terminal.Gui"

# 3. The surface must not be the only way in. A turn view that the scrollback path can no longer
#    select would make the piped proof unreachable no matter what the gate says.
if ! grep -q "class ScrollbackTurnView" "$PROJECT_ROOT/src/DtPipe/Cli/Agent/TurnView.cs" 2>/dev/null; then
    fail "ScrollbackTurnView is gone — the piped / CI rendering path lost its implementation"
fi
pass "the scrollback turn view still exists"

echo ""
echo -e "${GREEN}TUI boundary intact.${NC}"
