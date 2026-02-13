#!/bin/bash
# test_local.sh - Run integration tests using persistent local infrastructure
# This script speeds up local development by reusing containers instead of starting new ones for each test.

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR"
INFRA_START="$PROJECT_ROOT/tests/infra/start_infra.sh"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}Starting Local Test Suite with Persistent Infrastructure...${NC}"

# 1. Start Infrastructure
if [ -f "$INFRA_START" ]; then
    echo -e "${YELLOW}Ensuring Docker infrastructure is up and healthy...${NC}"
    "$INFRA_START"
else
    echo -e "${RED}Error: start_infra.sh not found at $INFRA_START${NC}"
    exit 1
fi

# 2. Run Tests with Reuse Variable
echo -e "${YELLOW}Running dotnet test with DTPIPE_TEST_REUSE_INFRA=true...${NC}"
export DTPIPE_TEST_REUSE_INFRA=true

# Pass all arguments to dotnet test (e.g. --filter, -v, etc.)
dotnet test "$@"

echo -e "${GREEN}Tests completed! Containers are still running for your next run.${NC}"
echo -e "Use ${YELLOW}./tests/infra/stop_infra.sh${NC} when you want to clean up."
