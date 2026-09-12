#!/bin/bash
set -e

# validate_branch_projection.sh
# One door from a job to a branch.
#
# Building a BranchDefinition out of a JobDefinition was written by hand at four sites — the CLI
# argument path, the CLI --job path, the YAML path behind MCP, and the linear path's synthetic
# branch. The copies drifted, and the drift was not theoretical: two of them left PreParsedJob
# unset, so get-dag-topology promised a branch's transformers in its own description and emitted
# none, while the CLI panel listed branches with no stages above a results table showing those
# same stages running. Both were fixed as instances. This guard is about the fifth copy.
#
# --------------------------------------------------------------------------
# Scope, stated honestly — this is a grep
# --------------------------------------------------------------------------
# It catches the plausible regression: a hand-written object initializer added next to the ones
# that were removed. It does NOT catch a projection assembled through a local helper, a `with`
# expression applied to a branch obtained elsewhere, or reflection.
#
# Tests are deliberately out of scope: GoldenDagDefinitions and the engine suites hand-build
# branches on purpose, to pin topologies that no job file produces.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

pass() { echo -e "  ${GREEN}OK: $1${NC}"; }
fail() { echo -e "  ${RED}FAIL: $1${NC}"; exit 1; }

echo "========================================"
echo "  DtPipe Branch Projection Validation"
echo "========================================"

PROJECTION="$PROJECT_ROOT/src/DtPipe.Core/Pipelines/Dag/BranchDefinition.cs"

# 1. The single projection exists and is what the call sites reach for.
if ! grep -q 'public static BranchDefinition FromJob(' "$PROJECTION"; then
    fail "BranchDefinition.FromJob is gone — it is the one projection every path goes through"
fi
pass "BranchDefinition.FromJob is the declared projection"

# 2. Production code builds a branch through it and nowhere else. The record's own factory is
#    the one place the initializer is legal, so it is excluded by path rather than by shape.
offenders="$(grep -rn 'new BranchDefinition' "$PROJECT_ROOT/src" \
    --include='*.cs' 2>/dev/null \
    | grep -v '/bin/' | grep -v '/obj/' \
    | grep -v 'Pipelines/Dag/BranchDefinition.cs' || true)"

if [ -n "$offenders" ]; then
    echo "$offenders"
    fail "a branch is built by hand — call BranchDefinition.FromJob instead"
fi
pass "no hand-written branch initializer in src/"

# 3. PreParsedJob stays init-only. It was the record's one mutable member, which is what let a
#    call site construct a branch and attach the job later — or forget to.
if grep -q 'PreParsedJob { get; set; }' "$PROJECTION"; then
    fail "BranchDefinition.PreParsedJob is settable again — a branch and its job must arrive together"
fi
pass "PreParsedJob is init-only"

echo ""
echo -e "${GREEN}Branch projection validation passed${NC}"
