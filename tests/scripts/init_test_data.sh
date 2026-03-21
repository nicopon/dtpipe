#!/bin/bash
# init_test_data.sh - Start infrastructure and initialize sources with fake data
# This script is idempotent: it cleans up artifacts and recreates all sources.

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
ARTIFACTS_DIR="$SCRIPT_DIR/artifacts"
INFRA_START="$PROJECT_ROOT/tests/infra/start_infra.sh"
# During initialization, we use --no-schema-validation because we are CREATING the schema.
DTPIPE="$PROJECT_ROOT/dist/release/dtpipe --no-schema-validation"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${YELLOW}Step 1: Starting infrastructure...${NC}"
if [ -f "$INFRA_START" ]; then
    "$INFRA_START" || { echo -e "${RED}Infrastructure failed to start${NC}"; exit 1; }
else
    echo "Error: start_infra.sh not found"
    exit 1
fi

echo -e "${YELLOW}Step 2: Cleaning up existing artifacts...${NC}"
mkdir -p "$ARTIFACTS_DIR"
rm -rf "$ARTIFACTS_DIR"/*

echo -e "${YELLOW}Step 3: Initializing file-based sources...${NC}"

# 1. CSV
echo "Generating test_data.csv..."
$DTPIPE -i "generate:1000" \
  --fake "Id:random.uuid" \
  --fake "FirstName:name.firstName" \
  --fake "LastName:name.lastName" \
  --fake "Email:internet.email" \
  --fake "Company:company.companyname" \
  --fake "BirthDate:date.past" \
  --fake "Score:random.number" \
  --drop "GenerateIndex" \
  -o "$ARTIFACTS_DIR/test_data.csv" || exit 1

# 2. Parquet
echo "Generating test_data.parquet..."
$DTPIPE -i "generate:1000" \
  --fake "Id:random.uuid" \
  --fake "Name:name.fullName" \
  --fake "Category:commerce.department" \
  --fake "Price:commerce.price" \
  --drop "GenerateIndex" \
  -o "$ARTIFACTS_DIR/test_data.parquet" --strategy Recreate || exit 1

# 2b. Parquet (BIG - 1M rows)
echo "Generating test_data_big.parquet (1,000,000 rows)..."
$DTPIPE -i "generate:1000000" \
  --fake "Id:random.uuid" \
  --fake "Timestamp:date.past" \
  --fake "Value:random.number" \
  --drop "GenerateIndex" \
  -o "$ARTIFACTS_DIR/test_data_big.parquet" --strategy Recreate || exit 1

# 3. Arrow
echo "Generating test_data.arrow..."
$DTPIPE -i "generate:1000" \
  --fake "Id:random.uuid" \
  --fake "Timestamp:date.recent" \
  --fake "Level:lorem.word" \
  --fake "Message:lorem.sentence" \
  --drop "GenerateIndex" \
  -o "$ARTIFACTS_DIR/test_data.arrow" --strategy Recreate || exit 1

echo -e "${YELLOW}Step 4: Initializing database sources...${NC}"

# 4. DuckDB
echo "Generating test_data.duckdb..."
$DTPIPE -i "generate:1000" \
  --fake "Id:random.uuid" \
  --fake "City:address.city" \
  --fake "Country:address.country" \
  --drop "GenerateIndex" \
  -o "$ARTIFACTS_DIR/test_data.duckdb" --table "geography" --strategy Recreate || exit 1

# 5. PostgreSQL
echo "Generating PostgreSQL users_test..."
$DTPIPE -i "generate:1000" \
  --fake "id:random.uuid" \
  --fake "username:internet.userName" \
  --fake "last_login:date.past" \
  --drop "GenerateIndex" \
  -o "pg:Host=localhost;Port=5440;Database=integration;Username=postgres;Password=password" \
  --table "users_test" --strategy Recreate || exit 1

# 6. SQL Server
echo "Generating SQL Server users_test..."
$DTPIPE -i "generate:1000" \
  --fake "id:random.uuid" \
  --fake "display_name:name.fullName" \
  --fake "credit_card:finance.creditCardNumber" \
  --drop "GenerateIndex" \
  -o "mssql:Server=localhost,1434;Database=master;User Id=sa;Password=Password123!;Encrypt=False" \
  --table "users_test" --strategy Recreate || exit 1

# 7. Oracle
echo "Generating Oracle USERS_TEST_DATA..."
# Using USERS_TEST_DATA to ensure a clean schema creation without legacy column names
$DTPIPE -i "generate:1000" \
  --fake "ID:random.uuid" \
  --fake "FULL_NAME:name.fullName" \
  --fake "JOB_TITLE:name.jobTitle" \
  --drop "GenerateIndex" \
  -o "ora:Data Source=localhost:1522/FREEPDB1;User Id=testuser;Password=password" \
  --table "USERS_TEST_DATA" \
  --strategy Recreate \
  --pre-exec "BEGIN EXECUTE IMMEDIATE 'DROP TABLE USERS_TEST_DATA'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;" || exit 1

echo -e "${GREEN}✓ All sources initialized successfully and idempotently!${NC}"

# Create a restricted directory for T77 (tests that write to an inaccessible path)
# chmod 000 means no read/write/execute for anyone
mkdir -p "$ARTIFACTS_DIR/restricted"
chmod 000 "$ARTIFACTS_DIR/restricted"
echo "Created restricted/ directory (chmod 000) for T77 access-denied tests."
