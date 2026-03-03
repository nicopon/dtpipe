#!/bin/bash
# ==============================================================================
# test_local.sh - Integration test suite with persistent infrastructure.
# ==============================================================================
#
# WHY THIS SCRIPT?
# By default, integration tests use "Testcontainers" to spin up fresh 
# database instances for each test session. This is ideal for CI, but
# TOO SLOW for local development (especially for Oracle and SQL Server).
#
# WHAT THIS SCRIPT DOES:
# 1. It starts a fixed Docker infrastructure (via docker-compose).
# 2. It tells the .NET tests: "Don't try to create containers, use the ones
#    already running on these specific ports".
#
# RESULT: Tests start almost instantly.
# ==============================================================================

set -e # Exit immediately if a command exits with a non-zero status.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR"
INFRA_START="$PROJECT_ROOT/tests/infra/start_infra.sh"

# Colors for terminal output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}Starting Local Test Suite with Persistent Infrastructure...${NC}"

# 1. Infrastructure Preparation
# We call start_infra.sh which is smart: it checks if containers are already
# running AND if they are "healthy" (ready to accept connections).
if [ -f "$INFRA_START" ]; then
    echo -e "${YELLOW}Checking Docker containers status...${NC}"
    "$INFRA_START"
else
    echo -e "${RED}Error: start_infra.sh not found at $INFRA_START${NC}"
    exit 1
fi

# 2. Enable Infrastructure Reuse Mode
# This environment variable is read by DockerHelper.cs in the test project.
# Its presence forces tests to use static ports (e.g., 5440 for Postgres)
# instead of asking Testcontainers to generate a random port.
echo -e "${YELLOW}REUSE_INFRA mode enabled. Running dotnet test...${NC}"
export DTPIPE_TEST_REUSE_INFRA=true

# 3. Test Execution
# "$@" captures all arguments passed to test_local.sh and forwards them
# to dotnet test (e.g., ./test_local.sh --filter NameOfTest)
dotnet test "$@"

echo -e "${GREEN}Tests completed! Containers are still running for your next run.${NC}"
echo -e "💡 Use ${YELLOW}./tests/infra/stop_infra.sh${NC} to shut down and free up RAM."
