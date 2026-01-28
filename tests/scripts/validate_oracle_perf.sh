#!/bin/bash

# Configuration
SCRIPT_DIR="$(dirname "$0")"
PROJECT_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"
DIST_DIR="$PROJECT_ROOT/dist/release"
QUERYDUMP_BIN="$DIST_DIR/querydump"

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if querydump exists
if [ ! -f "$QUERYDUMP_BIN" ]; then
    echo -e "${RED}Error: querydump binary not found at $QUERYDUMP_BIN${NC}"
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

echo -e "${GREEN}Starting Oracle Performance Test...${NC}"

# Cleanup function
cleanup() {
    echo "Cleaning up containers..."
    docker rm -f querydump-oracle-perf &> /dev/null
}
trap cleanup EXIT

# Start Oracle Container
echo "------------------------------------------------"
echo "Starting Oracle Container..."

# Use slim image for speed
docker run --name querydump-oracle-perf -e ORACLE_PASSWORD=MySecretPassword123! -p 1521:1521 -d gvenzl/oracle-free:slim

# Wait for Oracle
echo "Waiting for Oracle (30s)..."
# In a real script we might want a loop checking port availability or log output, but sleep is simple
sleep 30

# Create Target Table
echo "Creating target table..."
docker exec -i querydump-oracle-perf sqlplus system/MySecretPassword123!@localhost:1521/FREEPDB1 <<EOF
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
    
    $QUERYDUMP_BIN --input "sample:$ROW_COUNT;Id=int;Name=string;CreatedDate=date" \
                   --query "SELECT 1" \
                   --output "ora:Data Source=localhost:1521/FREEPDB1;User Id=system;Password=MySecretPassword123!;" \
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
COUNT=$(docker exec -i querydump-oracle-perf sqlplus -s system/MySecretPassword123!@localhost:1521/FREEPDB1 <<EOF
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
