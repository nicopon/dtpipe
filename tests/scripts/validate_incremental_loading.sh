#!/bin/bash

# Configuration
SCRIPT_DIR="$(dirname "$0")"
PROJECT_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"
DIST_DIR="$PROJECT_ROOT/dist/release"
DTPIPE_BIN="$DIST_DIR/dtpipe"
ARTIFACTS_DIR="tests/scripts/artifacts"
mkdir -p "$ARTIFACTS_DIR"

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
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

# Cleanup Function
cleanup() {
    echo "Cleaning up..."
    rm -f "$ARTIFACTS_DIR"/source_v1.csv "$ARTIFACTS_DIR"/source_v2.csv "$ARTIFACTS_DIR"/result_*.csv "$ARTIFACTS_DIR"/*.db "$ARTIFACTS_DIR"/*.duckdb
}
trap cleanup EXIT

# --- DATA GENERATION ---

generate_data() {
    echo "Generating Test Data..."
    
    # V1: Initial Load (IDs 1, 2)
    echo "Id,Name,Value" > "$ARTIFACTS_DIR/source_v1.csv"
    echo "1,Alice,100" >> "$ARTIFACTS_DIR/source_v1.csv"
    echo "2,Bob,200" >> "$ARTIFACTS_DIR/source_v1.csv"
    
    # V2: Delta Load (ID 1 Updated, ID 3 New)
    echo "Id,Name,Value" > "$ARTIFACTS_DIR/source_v2.csv"
    echo "1,Alice_Updated,150" >> "$ARTIFACTS_DIR/source_v2.csv"
    echo "3,Charlie,300" >> "$ARTIFACTS_DIR/source_v2.csv"
}

verify_upsert() {
    local file=$1
    local context=$2
    
    if [ ! -f "$file" ]; then
        echo -e "${RED}✗ $context Output file not found: $file${NC}"
        track_error
        return 1
    fi

    # Expect: 1,Alice_Updated,150 | 2,Bob,200 | 3,Charlie,300
    if grep -q "1,Alice_Updated,150" "$file" && \
       grep -q "2,Bob,200" "$file" && \
       grep -q "3,Charlie,300" "$file"; then
        echo -e "${GREEN}✓ $context Upsert Passed${NC}"
    else
        echo -e "${RED}✗ $context Upsert Failed${NC}"
        cat "$file"
        track_error
        return 1
    fi
}

verify_ignore() {
    local file=$1
    local context=$2
    
    if [ ! -f "$file" ]; then
        echo -e "${RED}✗ $context Output file not found: $file${NC}"
        track_error
        return 1
    fi

    # Expect: 1,Alice,100 (Ignored update) | 2,Bob,200 (Unchanged) | 3,Charlie,300 (New)
    if grep -q "1,Alice,100" "$file" && \
       grep -q "2,Bob,200" "$file" && \
       grep -q "3,Charlie,300" "$file"; then
        echo -e "${GREEN}✓ $context Ignore Passed${NC}"
    else
        echo -e "${RED}✗ $context Ignore Failed${NC}"
        cat "$file"
        track_error
        return 1
    fi
}

# Use Docker?
USE_DOCKER=0
if check_docker; then
    USE_DOCKER=1
fi

generate_data

# ==============================================================================
# 1. DUCKDB (Local)
# ==============================================================================
echo -e "\n${CYAN}--- Testing DuckDB ---${NC}"
DB_PATH="$ARTIFACTS_DIR/test_inc.duckdb"
rm -f "$DB_PATH"

# A. Upsert Test
echo "DuckDB: Upsert Strategy"
# 1. Load V1
$DTPIPE_BIN -i "$ARTIFACTS_DIR/source_v1.csv" -o "duck:$DB_PATH" --duck-table "users_upsert" --duck-strategy Recreate --key "Id" > /dev/null
# 2. Load V2 (Upsert)
$DTPIPE_BIN -i "$ARTIFACTS_DIR/source_v2.csv" -o "duck:$DB_PATH" --duck-table "users_upsert" --duck-strategy Upsert --key "Id" > /dev/null
# 3. Export & Verify
$DTPIPE_BIN -i "duck:$DB_PATH" -q "SELECT * FROM users_upsert ORDER BY Id" -o "$ARTIFACTS_DIR/result_duck_upsert.csv" > /dev/null
verify_upsert "$ARTIFACTS_DIR/result_duck_upsert.csv" "DuckDB"

# B. Ignore Test
echo "DuckDB: Ignore Strategy"
# 1. Load V1
$DTPIPE_BIN -i "$ARTIFACTS_DIR/source_v1.csv" -o "duck:$DB_PATH" --duck-table "users_ignore" --duck-strategy Recreate --key "Id" > /dev/null
# 2. Load V2 (Ignore)
$DTPIPE_BIN -i "$ARTIFACTS_DIR/source_v2.csv" -o "duck:$DB_PATH" --duck-table "users_ignore" --duck-strategy Ignore --key "Id" > /dev/null
# 3. Export & Verify
$DTPIPE_BIN -i "duck:$DB_PATH" -q "SELECT * FROM users_ignore ORDER BY Id" -o "$ARTIFACTS_DIR/result_duck_ignore.csv" > /dev/null
verify_ignore "$ARTIFACTS_DIR/result_duck_ignore.csv" "DuckDB"


# ==============================================================================
# 2. SQLITE (Local)
# ==============================================================================
echo -e "\n${CYAN}--- Testing SQLite ---${NC}"
DB_PATH="$ARTIFACTS_DIR/test_inc.db"
rm -f "$DB_PATH"

# A. Upsert Test
echo "SQLite: Upsert Strategy"
$DTPIPE_BIN -i "$ARTIFACTS_DIR/source_v1.csv" -o "sqlite:$DB_PATH" --sqlite-table "users_upsert" --sqlite-strategy Recreate --key "Id" > /dev/null
$DTPIPE_BIN -i "$ARTIFACTS_DIR/source_v2.csv" -o "sqlite:$DB_PATH" --sqlite-table "users_upsert" --sqlite-strategy Upsert --key "Id" > /dev/null
$DTPIPE_BIN -i "sqlite:$DB_PATH" -q "SELECT * FROM users_upsert ORDER BY Id" -o "$ARTIFACTS_DIR/result_sqlite_upsert.csv" > /dev/null
verify_upsert "$ARTIFACTS_DIR/result_sqlite_upsert.csv" "SQLite"

# B. Ignore Test
echo "SQLite: Ignore Strategy"
$DTPIPE_BIN -i "$ARTIFACTS_DIR/source_v1.csv" -o "sqlite:$DB_PATH" --sqlite-table "users_ignore" --sqlite-strategy Recreate --key "Id" > /dev/null
$DTPIPE_BIN -i "$ARTIFACTS_DIR/source_v2.csv" -o "sqlite:$DB_PATH" --sqlite-table "users_ignore" --sqlite-strategy Ignore --key "Id" > /dev/null
$DTPIPE_BIN -i "sqlite:$DB_PATH" -q "SELECT * FROM users_ignore ORDER BY Id" -o "$ARTIFACTS_DIR/result_sqlite_ignore.csv" > /dev/null
verify_ignore "$ARTIFACTS_DIR/result_sqlite_ignore.csv" "SQLite"


if [ "$USE_DOCKER" -eq 1 ]; then

    # Resolve Infrastructure Dir
    INFRA_DIR="$PROJECT_ROOT/tests/infra"
    echo -e "\n${CYAN}--- Ensuring Shared Infrastructure is Ready ---${NC}"
    "$INFRA_DIR/start_infra.sh"

    # ==============================================================================
    # 3. POSTGRESQL (Docker)
    # ==============================================================================
    echo -e "\n${CYAN}--- Testing PostgreSQL ---${NC}"
    PG_CONN="pg:Host=localhost;Port=5440;Username=postgres;Password=password;Database=integration"
    
    # A. Upsert Test
    echo "Postgres: Upsert Strategy"
    $DTPIPE_BIN -i "$ARTIFACTS_DIR/source_v1.csv" -o "$PG_CONN" --pg-table "users_upsert" --pg-strategy Recreate --key "id" > /dev/null
    $DTPIPE_BIN -i "$ARTIFACTS_DIR/source_v2.csv" -o "$PG_CONN" --pg-table "users_upsert" --pg-strategy Upsert --key "Id" > /dev/null
    $DTPIPE_BIN -i "$PG_CONN" -q "SELECT * FROM users_upsert ORDER BY Id" -o "$ARTIFACTS_DIR/result_pg_upsert.csv" > /dev/null
    verify_upsert "$ARTIFACTS_DIR/result_pg_upsert.csv" "Postgres"
    
    # B. Ignore Test
    echo "Postgres: Ignore Strategy"
    $DTPIPE_BIN -i "$ARTIFACTS_DIR/source_v1.csv" -o "$PG_CONN" --pg-table "users_ignore" --pg-strategy Recreate --key "Id" > /dev/null
    $DTPIPE_BIN -i "$ARTIFACTS_DIR/source_v2.csv" -o "$PG_CONN" --pg-table "users_ignore" --pg-strategy Ignore --key "Id" > /dev/null
    $DTPIPE_BIN -i "$PG_CONN" -q "SELECT * FROM users_ignore ORDER BY Id" -o "$ARTIFACTS_DIR/result_pg_ignore.csv" > /dev/null
    verify_ignore "$ARTIFACTS_DIR/result_pg_ignore.csv" "Postgres"


    # ==============================================================================
    # 4. SQL SERVER (Docker)
    # ==============================================================================
    echo -e "\n${CYAN}--- Testing SQL Server ---${NC}"
    # Use Master for simplicity as in other tests, or integration if we create it
    MSSQL_CONN="mssql:Server=localhost,1434;Database=master;User Id=sa;Password=Password123!;TrustServerCertificate=True;MultipleActiveResultSets=True"
    
    # A. Upsert Test
    echo "MSSQL: Upsert Strategy"
    $DTPIPE_BIN -i "$ARTIFACTS_DIR/source_v1.csv" -o "$MSSQL_CONN" --mssql-table "UsersUpsert" --mssql-strategy Recreate --key "Id" > /dev/null
    $DTPIPE_BIN -i "$ARTIFACTS_DIR/source_v2.csv" -o "$MSSQL_CONN" --mssql-table "UsersUpsert" --mssql-strategy Upsert --key "Id" > /dev/null
    $DTPIPE_BIN -i "$MSSQL_CONN" -q "SELECT * FROM UsersUpsert ORDER BY Id" -o "$ARTIFACTS_DIR/result_mssql_upsert.csv" > /dev/null
    verify_upsert "$ARTIFACTS_DIR/result_mssql_upsert.csv" "MSSQL"
    
    # B. Ignore Test
    echo "MSSQL: Ignore Strategy"
    $DTPIPE_BIN -i "$ARTIFACTS_DIR/source_v1.csv" -o "$MSSQL_CONN" --mssql-table "UsersIgnore" --mssql-strategy Recreate --key "Id" > /dev/null
    $DTPIPE_BIN -i "$ARTIFACTS_DIR/source_v2.csv" -o "$MSSQL_CONN" --mssql-table "UsersIgnore" --mssql-strategy Ignore --key "Id" > /dev/null
    $DTPIPE_BIN -i "$MSSQL_CONN" -q "SELECT * FROM UsersIgnore ORDER BY Id" -o "$ARTIFACTS_DIR/result_mssql_ignore.csv" > /dev/null
    verify_ignore "$ARTIFACTS_DIR/result_mssql_ignore.csv" "MSSQL"
    
    
    # ==============================================================================
    # 5. ORACLE (Docker)
    # ==============================================================================
    echo -e "\n${CYAN}--- Testing Oracle ---${NC}"
    ORA_CONN="ora:Data Source=localhost:1522/FREEPDB1;User Id=testuser;Password=password;"
    
    # A. Upsert Test
    echo "Oracle: Upsert Strategy"
    $DTPIPE_BIN -i "$ARTIFACTS_DIR/source_v1.csv" -o "$ORA_CONN" --ora-table "USERS_UPSERT" --ora-strategy Recreate --key "Id" > /dev/null
    $DTPIPE_BIN -i "$ARTIFACTS_DIR/source_v2.csv" -o "$ORA_CONN" --ora-table "USERS_UPSERT" --ora-strategy Upsert --key "Id" > /dev/null
    $DTPIPE_BIN -i "$ORA_CONN" -q "SELECT * FROM USERS_UPSERT ORDER BY Id" -o "$ARTIFACTS_DIR/result_ora_upsert.csv" > /dev/null
    verify_upsert "$ARTIFACTS_DIR/result_ora_upsert.csv" "Oracle"
    
    # B. Ignore Test
    echo "Oracle: Ignore Strategy"
    $DTPIPE_BIN -i "$ARTIFACTS_DIR/source_v1.csv" -o "$ORA_CONN" --ora-table "USERS_IGNORE" --ora-strategy Recreate --key "Id" > /dev/null
    $DTPIPE_BIN -i "$ARTIFACTS_DIR/source_v2.csv" -o "$ORA_CONN" --ora-table "USERS_IGNORE" --ora-strategy Ignore --key "Id" > /dev/null
    $DTPIPE_BIN -i "$ORA_CONN" -q "SELECT * FROM USERS_IGNORE ORDER BY Id" -o "$ARTIFACTS_DIR/result_ora_ignore.csv" > /dev/null
    verify_ignore "$ARTIFACTS_DIR/result_ora_ignore.csv" "Oracle"

fi

if [ "$ERRORS" -eq 0 ]; then
    echo -e "\n${GREEN}All Incremental Loading tests passed!${NC}"
    exit 0
else
    echo -e "\n${RED}$ERRORS tests failed!${NC}"
    exit 1
fi
