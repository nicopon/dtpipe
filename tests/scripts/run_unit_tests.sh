#!/usr/bin/env bash
# Runs every suite that needs no infrastructure: the `.Unit.` tests of DtPipe.Tests, then every other
# tests/*/*.Tests.csproj, discovered rather than listed so a new suite cannot stay outside the gate.
# The node's tests spawn the real binary, so dist/release/dtpipe is built first (build.sh and the CI test jobs do).
# Called by build.sh, by the test job of build.yml and by the release's; any failing suite fails the script.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

YELLOW='\033[1;33m'
NC='\033[0m'
MAIN="tests/DtPipe.Tests/DtPipe.Tests.csproj"

echo ""
echo -e "${YELLOW}Running Tests...${NC}"
dotnet test "$MAIN" -c Release --filter "FullyQualifiedName~.Unit."

for proj in tests/*/*.Tests.csproj; do
    [ "$proj" = "$MAIN" ] && continue
    echo ""
    echo -e "${YELLOW}Running $(basename "$(dirname "$proj")")...${NC}"
    dotnet test "$proj" -c Release
done
