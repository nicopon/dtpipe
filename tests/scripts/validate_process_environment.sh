#!/bin/bash
set -e

# validate_process_environment.sh
# No source writes the process environment.
#
# A value written there is global: every branch of the run, every run beside it and every child
# process reads it. A value that belongs to one run is armed for its call chain instead
# (LaunchValues, CursorOverride); a child is given its own environment (ProcessStartInfo).
#
# Scope — this is a grep over src/: it does not catch a call made by reflection or through
# a native setenv.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

echo "========================================"
echo "    DtPipe Process Environment Validation"
echo "========================================"

hits=$(grep -rn --include='*.cs' 'SetEnvironmentVariable' "$PROJECT_ROOT/src" | grep -v '/bin/\|/obj/' || true)
if [ -n "$hits" ]; then
    echo "$hits"
    echo -e "  ${RED}FAIL: a source writes the process environment; arm the value for its run instead${NC}"
    exit 1
fi
echo -e "  ${GREEN}OK: no source writes the process environment${NC}"
