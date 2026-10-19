#!/bin/bash
set -e

# validate_distributed_boundary.sh
# Source-level guard on the distributed-pipeline split: TransportR is named only by the distributed
# family (every project named DtPipe.Coordinator* or DtPipe.PipelineNode*), its tests and the lab; no
# core project reaches it; the two base libraries stay libraries; the lab and its sample data stay
# under samples/, outside the solution and the build, and the product's images (deploy/) name none of them; the family reaches the solution only through
# DtPipe.Jobs; the CLI maps no SignalR hub.
#
# Scope: names and references, not behaviour. A type that crosses the line without naming TransportR
# (a transitive use) is not seen here.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$PROJECT_ROOT"

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

pass() { echo -e "  ${GREEN}OK: $1${NC}"; }
fail() { echo -e "  ${RED}FAIL: $1${NC}"; exit 1; }

# Tracked files plus new ones not yet added, so the guard sees a change before it is committed.
FILES=$(git ls-files --cached --others --exclude-standard)

# Files that are not part of the distributed family, its tests or the lab.
OUTSIDE=$(echo "$FILES" | grep -vE '^((src|tests)/DtPipe\.(Coordinator|PipelineNode)(\.[A-Za-z]+)*/|samples/distributed-lab/)' || true)

echo "========================================"
echo "  DtPipe Distributed Boundary Validation"
echo "========================================"

# 1. TransportR is named by project files and sources of the distributed family only.
HITS=""
for f in $(echo "$OUTSIDE" | grep -E '\.(cs|csproj|props|targets|sln)$' || true); do
    [ -f "$f" ] || continue
    if grep -qE 'TransportR' "$f"; then HITS="$HITS\n  $f"; fi
done
if [ -n "$HITS" ]; then
    echo -e "$HITS"
    fail "TransportR named outside the distributed family, its tests and the lab"
fi
pass "TransportR is named only by the distributed family, its tests and the lab"

# 2. No project outside the family (and outside the lab) references a family project or the lab.
HITS=""
for f in $(echo "$OUTSIDE" | grep -E '\.csproj$' || true); do
    [ -f "$f" ] || continue
    if grep -qE 'ProjectReference Include="[^"]*(DtPipe\.Coordinator|DtPipe\.PipelineNode|distributed-lab)' "$f"; then
        HITS="$HITS\n  $f"
    fi
done
if [ -n "$HITS" ]; then
    echo -e "$HITS"
    fail "a project outside the distributed family and the lab references a family project or the lab"
fi
pass "no core project references the distributed family or the lab"

# 3. The distributed family and the lab stay out of the solution.
if grep -qE 'DtPipe\.(Coordinator|PipelineNode)|distributed-lab|Lab\.(Coordinator|NodeHost|Contracts)' DtPipe.sln; then
    fail "DtPipe.sln names a distributed-family project or a lab project"
fi
pass "DtPipe.sln names neither the distributed family nor the lab"

# 4. The lab, and the pipelines and bricks of its sample data, are named under samples/ only. The
#    product's images (deploy/) are such a place: every file there, whatever its type.
HITS=""
for f in $(
    {
        echo "$OUTSIDE" | grep -E '^(src/|tests/|build\.sh$|\.github/|Directory\.Build\.props$)' | grep -E '\.(cs|csproj|props|sh|yml|yaml)$' || true
        echo "$OUTSIDE" | grep -E '^deploy/' || true
    }
); do
    [ -f "$f" ] || continue
    # This guard names the lab on purpose.
    [ "$f" = "tests/scripts/validate_distributed_boundary.sh" ] && continue
    if grep -qE 'distributed-lab|DtPipe\.Lab\b|Lab\.(Coordinator|NodeHost|Contracts)|sensor-stream|order-history|customers-anonymized' "$f"; then
        HITS="$HITS\n  $f"
    fi
done
if [ -n "$HITS" ]; then
    echo -e "$HITS"
    fail "the lab or its sample pipelines are named outside samples/distributed-lab"
fi
pass "the lab and its sample data are named only under samples/distributed-lab"

# 5. The two base libraries stay libraries: a host owns Program, authentication and the address. An
#    executable of the family (DtPipe.Coordinator.Server, a runner) is such a host and is not checked.
HITS=""
for f in src/DtPipe.Coordinator/DtPipe.Coordinator.csproj src/DtPipe.PipelineNode/DtPipe.PipelineNode.csproj; do
    if grep -qE 'Sdk="Microsoft\.NET\.Sdk\.Web"|<OutputType>[[:space:]]*(Exe|WinExe)' "$f"; then HITS="$HITS\n  $f"; fi
done
for f in $(echo "$FILES" | grep -E '^src/DtPipe\.(Coordinator|PipelineNode)/(Program\.cs)$' || true); do
    [ -f "$f" ] && HITS="$HITS\n  $f"
done
if [ -n "$HITS" ]; then
    echo -e "$HITS"
    fail "a base library of the family is built as an application (web SDK, executable output type or Program.cs)"
fi
pass "DtPipe.Coordinator and DtPipe.PipelineNode are libraries"

# 6. The family reaches the solution through DtPipe.Jobs only: a job file's YAML and the designer's
#    cards, no engine, so a host of the family never runs dtpipe in its own process.
HITS=""
for f in $(echo "$FILES" | grep -E '^src/DtPipe\.(Coordinator|PipelineNode)[^/]*/[^/]*\.csproj$' || true); do
    [ -f "$f" ] || continue
    if grep -oE 'ProjectReference Include="[^"]*"' "$f" | grep -vqE 'DtPipe\.(Coordinator|PipelineNode)[^/\\]*\.csproj|DtPipe\.Jobs\.csproj'; then
        HITS="$HITS\n  $f"
    fi
done
if [ -n "$HITS" ]; then
    echo -e "$HITS"
    fail "a project of the family references a solution project other than DtPipe.Jobs"
fi
pass "the family references DtPipe.Jobs and nothing else of the solution"

# 7. The CLI hosts no hub. The ASP.NET Core framework the studio brings carries SignalR, so no
#    reference check sees a hub of the CLI's own.
HITS=""
for f in $(echo "$FILES" | grep -E '^src/DtPipe/.*\.cs$' || true); do
    [ -f "$f" ] || continue
    if grep -qE 'AddSignalR|MapHub|IHubContext|Microsoft\.AspNetCore\.SignalR' "$f"; then HITS="$HITS\n  $f"; fi
done
if [ -n "$HITS" ]; then
    echo -e "$HITS"
    fail "the CLI maps or uses a SignalR hub"
fi
pass "the CLI hosts no SignalR hub"

echo ""
echo -e "${GREEN}Distributed boundary checks passed.${NC}"
