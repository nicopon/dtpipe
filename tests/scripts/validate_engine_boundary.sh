#!/bin/bash
set -e

# validate_engine_boundary.sh
# The engine (src/DtPipe/Engine/) names no console, command-line or logging library of the host.
#
# What a run shows belongs to the host: the engine hands it plain text through IRunHost and
# IExportObserver, and the host formats it. A using here, or a fully qualified name, is the engine
# formatting for one terminal, which every other host (MCP, the studio, a node's child) then inherits.
#
# Scope — this is a grep over the folder: it does not see a type of those libraries reached through
# another assembly's signature.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
ENGINE="$PROJECT_ROOT/src/DtPipe/Engine"

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

echo "========================================"
echo "    DtPipe Engine Boundary Validation"
echo "========================================"

if [ ! -d "$ENGINE" ]; then
    echo -e "  ${RED}FAIL: $ENGINE does not exist; update this check with the engine's folder${NC}"
    exit 1
fi

hits=$(grep -rnE --include='*.cs' '\b(Spectre\.Console|System\.CommandLine|Serilog)\b' "$ENGINE" || true)
if [ -n "$hits" ]; then
    echo "$hits"
    echo -e "  ${RED}FAIL: the engine names a library of the host; pass plain text to IRunHost or IExportObserver${NC}"
    exit 1
fi
echo -e "  ${GREEN}OK: the engine names neither Spectre.Console, System.CommandLine nor Serilog${NC}"
