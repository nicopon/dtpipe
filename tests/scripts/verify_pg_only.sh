#!/bin/bash

# Configuration
SCRIPT_DIR="$(dirname "$0")"
PROJECT_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"
DIST_DIR="$PROJECT_ROOT/dist/release"
DTPIPE_BIN="$DIST_DIR/dtpipe"

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

ERRORS=0

track_error() {
    ERRORS=$((ERRORS + 1))
}

# Check if dtpipe exists
if [ ! -f "$DTPIPE_BIN" ]; then
    echo -e "${RED}Error: dtpipe binary not found at $DTPIPE_BIN${NC}"
    echo "Please run ./build.sh first."
    exit 1
fi

# Cleanup function
cleanup() {
    echo "Cleaning up..."
    docker rm -f dtpipe-inc-postgres-debug &> /dev/null
    rm -f source_v1.csv source_v2.csv result_*.csv
}
trap cleanup EXIT

# --- DATA GENERATION ---

generate_data() {
    echo "Generating Test Data..."
    
    # V1: Initial Load (IDs 1, 2)
    echo "Id,Name,Value" > source_v1.csv
    echo "1,Alice,100" >> source_v1.csv
    echo "2,Bob,200" >> source_v1.csv
    
    # V2: Delta Load (ID 1 Updated, ID 3 New)
    echo "Id,Name,Value" > source_v2.csv
    echo "1,Alice_Updated,150" >> source_v2.csv
    echo "3,Charlie,300" >> source_v2.csv
}

verify_upsert() {
    local file=$1
    
    if [ ! -f "$file" ]; then
        echo -e "${RED}✗ Output file not found: $file${NC}"
        track_error
        return 1
    fi

    # Expect: 1,Alice_Updated,150 | 2,Bob,200 | 3,Charlie,300
    if grep -q "1,Alice_Updated,150" "$file" && \
       grep -q "2,Bob,200" "$file" && \
       grep -q "3,Charlie,300" "$file"; then
        echo -e "${GREEN}✓ Postgres Upsert Passed${NC}"
    else
        echo -e "${RED}✗ Postgres Upsert Failed${NC}"
        cat "$file"
        track_error
        return 1
    fi
}

verify_ignore() {
    local file=$1
    
    if [ ! -f "$file" ]; then
        echo -e "${RED}✗ Output file not found: $file${NC}"
        track_error
        return 1
    fi

    # Expect: 1,Alice,100 (Ignored update) | 2,Bob,200 (Unchanged) | 3,Charlie,300 (New)
    if grep -q "1,Alice,100" "$file" && \
       grep -q "2,Bob,200" "$file" && \
       grep -q "3,Charlie,300" "$file"; then
        echo -e "${GREEN}✓ Postgres Ignore Passed${NC}"
    else
        echo -e "${RED}✗ Postgres Ignore Failed${NC}"
        cat "$file"
        track_error
        return 1
    fi
}

generate_data

echo -e "\n${CYAN}--- Testing PostgreSQL (Debug) ---${NC}"
docker run --name dtpipe-inc-postgres-debug -e POSTGRES_PASSWORD=password -p 5434:5432 -d postgres:latest > /dev/null
echo "Waiting for Postgres (5s)..."
sleep 5

PG_CONN="pg:Host=localhost;Port=5434;Username=postgres;Password=password;Database=postgres"

# A. Upsert Test
echo "Postgres: Upsert Strategy"
$DTPIPE_BIN -i "source_v1.csv" -o "$PG_CONN" --pg-table "users_upsert" --pg-strategy Recreate --key "Id" > /dev/null
echo "Running Upsert..."
$DTPIPE_BIN -i "source_v2.csv" -o "$PG_CONN" --pg-table "users_upsert" --pg-strategy Upsert --key "Id"
$DTPIPE_BIN -i "$PG_CONN" -q "SELECT * FROM users_upsert ORDER BY id" -o "result_pg_upsert.csv" > /dev/null
verify_upsert "result_pg_upsert.csv"

# B. Ignore Test
echo "Postgres: Ignore Strategy"
$DTPIPE_BIN -i "source_v1.csv" -o "$PG_CONN" --pg-table "users_ignore" --pg-strategy Recreate --key "Id" > /dev/null
$DTPIPE_BIN -i "source_v2.csv" -o "$PG_CONN" --pg-table "users_ignore" --pg-strategy Ignore --key "Id" > /dev/null
$DTPIPE_BIN -i "$PG_CONN" -q "SELECT * FROM users_ignore ORDER BY id" -o "result_pg_ignore.csv" > /dev/null
verify_ignore "result_pg_ignore.csv"


if [ "$ERRORS" -eq 0 ]; then
    echo -e "\n${GREEN}Postgres tests passed!${NC}"
    exit 0
else
    echo -e "\n${RED}$ERRORS tests failed!${NC}"
    exit 1
fi
