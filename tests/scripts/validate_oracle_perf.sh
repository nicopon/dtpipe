#!/bin/bash

# Configuration
SCRIPT_DIR="$(dirname "$0")"
PROJECT_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"
DIST_DIR="$PROJECT_ROOT/dist/release"
DTPIPE_BIN="$DIST_DIR/dtpipe"

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if dtpipe exists
if [ ! -f "$DTPIPE_BIN" ]; then
    echo -e "${RED}Error: dtpipe binary not found at $DTPIPE_BIN${NC}"
    echo "Please run ./build.sh first."
    exit 1
fi

# Function to check Docker availability
check_docker() {
    if ! command -v docker &> /dev/null; then
        echo -e "${YELLOW}Docker is not installed. Skipping container tests.${NC}"
        return 1
    fi

    if ! docker info &> /dev/null; then
        echo -e "${YELLOW}Docker is installed but not running (or permission denied). Skipping container tests.${NC}"
        return 1
    fi
    
    echo -e "${GREEN}Docker is available.${NC}"
    return 0
}

# Run Docker Check
if ! check_docker; then
    echo -e "${YELLOW}SKIP: Docker-based integration tests skipped.${NC}"
    exit 0
fi

# Resolve Infrastructure Dir
INFRA_DIR="$PROJECT_ROOT/tests/infra"

echo -e "${GREEN}Starting Oracle Performance Test...${NC}"

# Cleanup function (modified for shared infra)
cleanup() {
    echo "Cleaning up..."
    # We don't delete shared containers anymore
}
trap cleanup EXIT

# Use shared infrastructure
echo "------------------------------------------------"
echo "Ensuring Shared Infrastructure is Ready..."
"$INFRA_DIR/start_infra.sh"

ORA_CONTAINER="dtpipe-integ-oracle"
ORA_PORT=1522

# Create Target Table
echo "Creating target table..."
docker exec -i "$ORA_CONTAINER" sqlplus system/password@localhost:"$ORA_PORT"/FREEPDB1 <<EOF
CREATE TABLE PerformanceTest (
    Id NUMBER,
    Name VARCHAR2(100),
    CreatedDate TIMESTAMP
);
EXIT;
EOF

# Run Benchmark
ROW_COUNT=1000000
echo -e "${YELLOW}Running export of $ROW_COUNT rows...${NC}"

run_test() {
    local MODE=$1
    echo -e "\n${YELLOW}Testing Mode: $MODE${NC}"
    
    local START_TIME=$(date +%s)
    
    $DTPIPE_BIN --input "generate:$ROW_COUNT" \
                   --fake "Id:random.number" --fake "Name:name.fullName" --fake "CreatedDate:date.past" \
                   --drop "GenerateIndex" \
                   --query "SELECT 1" \
                   --output "ora:Data Source=localhost:$ORA_PORT/FREEPDB1;User Id=system;Password=password;" \
                   --ora-table "PerformanceTest" \
                   --ora-strategy "Truncate" \
                   --ora-insert-mode "$MODE" \
                   --batch-size 10000

    local END_TIME=$(date +%s)
    local DURATION=$((END_TIME - START_TIME))
    echo -e "${GREEN}Mode $MODE completed in $DURATION seconds.${NC}"
}

run_test "Standard"
run_test "Append"
run_test "Bulk"

# Verify count
echo "Verifying row count..."
COUNT=$(docker exec -i "$ORA_CONTAINER" sqlplus -s system/password@localhost:"$ORA_PORT"/FREEPDB1 <<EOF
SET HEADING OFF;
SELECT COUNT(*) FROM PerformanceTest;
EXIT;
EOF
)
COUNT=$(echo $COUNT | xargs) # trim

if [ "$COUNT" == "$ROW_COUNT" ]; then
    echo -e "${GREEN}✓ Count matches: $COUNT${NC}"
else
    echo -e "${RED}✗ Count mismatch: expected $ROW_COUNT, got $COUNT${NC}"
    exit 1
fi
