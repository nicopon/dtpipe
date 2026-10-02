#!/bin/bash
set -e

# validate_distributed_boundary.sh
# Source-level guard on the distributed-pipeline split: TransportR is named only by the two projects
# that wrap it (DtPipe.Coordinator, DtPipe.PipelineNode), their tests and the lab; no core project
# reaches them; the lab and its sample data stay under samples/, outside the solution and the build.
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

# Files that are not part of the two wrapper projects, their tests or the lab.
OUTSIDE=$(echo "$FILES" | grep -vE '^(src/DtPipe\.(Coordinator|PipelineNode)/|tests/DtPipe\.(Coordinator|PipelineNode)\.Tests/|samples/distributed-lab/)' || true)

echo "========================================"
echo "  DtPipe Distributed Boundary Validation"
echo "========================================"

# 1. TransportR is named by project files and sources of the wrapper projects only.
HITS=""
for f in $(echo "$OUTSIDE" | grep -E '\.(cs|csproj|props|targets|sln)$' || true); do
    [ -f "$f" ] || continue
    if grep -qE 'TransportR' "$f"; then HITS="$HITS\n  $f"; fi
done
if [ -n "$HITS" ]; then
    echo -e "$HITS"
    fail "TransportR named outside DtPipe.Coordinator, DtPipe.PipelineNode, their tests and the lab"
fi
pass "TransportR is named only by the wrapper projects, their tests and the lab"

# 2. No project outside them (and outside the lab) references a wrapper project or the lab.
HITS=""
for f in $(echo "$OUTSIDE" | grep -E '\.csproj$' || true); do
    [ -f "$f" ] || continue
    if grep -qE 'ProjectReference Include="[^"]*(DtPipe\.Coordinator|DtPipe\.PipelineNode|distributed-lab)' "$f"; then
        HITS="$HITS\n  $f"
    fi
done
if [ -n "$HITS" ]; then
    echo -e "$HITS"
    fail "a project outside the wrapper projects, their tests and the lab references one of them or the lab"
fi
pass "no core project references the wrapper projects or the lab"

# 3. The wrapper projects and the lab stay out of the solution.
if grep -qE 'DtPipe\.(Coordinator|PipelineNode)|distributed-lab|Lab\.(Coordinator|NodeHost|Contracts)' DtPipe.sln; then
    fail "DtPipe.sln names a wrapper project or a lab project"
fi
pass "DtPipe.sln names neither the wrapper projects nor the lab"

# 4. The lab, and the pipelines and bricks of its sample data, are named under samples/ only.
HITS=""
for f in $(echo "$OUTSIDE" | grep -E '^(src/|tests/|build\.sh$|\.github/|Directory\.Build\.props$)' | grep -E '\.(cs|csproj|props|sh|yml|yaml)$' || true); do
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

# 5. The wrapper projects are libraries: a host owns Program, the identity provider and the address.
HITS=""
for f in src/DtPipe.Coordinator/DtPipe.Coordinator.csproj src/DtPipe.PipelineNode/DtPipe.PipelineNode.csproj; do
    if grep -qE 'Sdk="Microsoft\.NET\.Sdk\.Web"|<OutputType>[[:space:]]*(Exe|WinExe)' "$f"; then HITS="$HITS\n  $f"; fi
done
for f in $(echo "$FILES" | grep -E '^src/DtPipe\.(Coordinator|PipelineNode)/(Program\.cs)$' || true); do
    [ -f "$f" ] && HITS="$HITS\n  $f"
done
if [ -n "$HITS" ]; then
    echo -e "$HITS"
    fail "a wrapper project is built as an application (web SDK, executable output type or Program.cs)"
fi
pass "DtPipe.Coordinator and DtPipe.PipelineNode are libraries"

echo ""
echo -e "${GREEN}Distributed boundary checks passed.${NC}"
