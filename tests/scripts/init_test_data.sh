#!/bin/bash
# init_test_data.sh - Start infrastructure and initialize sources with fake data.
# Idempotent: skips files that already exist, only creates what is missing.
# Run clean_test_data.sh first if you need a full reset.

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Test infrastructure endpoints. Sourcing this is what declares that this script
# needs tests/infra running (see lib/test_connections.sh).
# shellcheck source=lib/test_connections.sh
source "$SCRIPT_DIR/lib/test_connections.sh"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
ARTIFACTS_DIR="$SCRIPT_DIR/artifacts"
INFRA_START="$PROJECT_ROOT/tests/infra/start_infra.sh"
DTPIPE="$PROJECT_ROOT/dist/release/dtpipe"

CLEAN=0
for arg in "$@"; do
    case "$arg" in
        --clean) CLEAN=1 ;;
        --help|-h)
            echo "Usage: $(basename "$0") [--clean]"
            echo "  --clean   Also regenerate the two volume fixtures (29 MB and 2.7 GB)."
            echo "            Everything else is regenerated on every run regardless."
            exit 0
            ;;
        *) echo "Unknown option: $arg (see --help)"; exit 1 ;;
    esac
done

# The two volume fixtures kept from a previous run. Everything whose CONTENT the catalog
# exercises is regenerated every time — those files are under 600 KB in total, so skipping
# them bought nothing and risked testing data older than the generator that made it. Only
# test_data_big.parquet (29 MB) and test_data_massive.xml (2.7 GB) are kept, and for those
# it is the volume that matters, not the values.
KEPT=0

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

echo -e "${YELLOW}Step 2: Ensuring artifacts directory exists...${NC}"
mkdir -p "$ARTIFACTS_DIR"

if [ $CLEAN -eq 1 ]; then
    echo -e "${YELLOW}--clean requested: removing existing input data...${NC}"
    "$SCRIPT_DIR/clean_test_data.sh"
    mkdir -p "$ARTIFACTS_DIR"
fi

echo -e "${YELLOW}Step 3: Initializing file-based sources...${NC}"

# 1. CSV
echo "Generating test_data.csv..."
$DTPIPE -i "generate:1000" \
  --fake "Id:random.guid" \
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
  --fake "Id:random.guid" \
  --fake "Name:name.fullName" \
  --fake "Category:commerce.department" \
  --fake "Price:commerce.price" \
  --drop "GenerateIndex" \
  -o "$ARTIFACTS_DIR/test_data.parquet" || exit 1

# 2b. Parquet (BIG - 1M rows)
if [ ! -f "$ARTIFACTS_DIR/test_data_big.parquet" ]; then
    echo "Generating test_data_big.parquet (1,000,000 rows)..."
    $DTPIPE -i "generate:1000000" \
      --fake "Id:random.guid" \
      --fake "Timestamp:date.past" \
      --fake "Value:random.number" \
      --drop "GenerateIndex" \
      -o "$ARTIFACTS_DIR/test_data_big.parquet" || exit 1
else
    echo "  Skipping test_data_big.parquet (already exists)"
    KEPT=$((KEPT + 1))
fi

# 3. Arrow
echo "Generating test_data.arrow..."
$DTPIPE -i "generate:1000" \
  --fake "Id:random.guid" \
  --fake "Timestamp:date.recent" \
  --fake "Level:lorem.word" \
  --fake "Message:lorem.sentence" \
  --drop "GenerateIndex" \
  -o "$ARTIFACTS_DIR/test_data.arrow" || exit 1

echo -e "${YELLOW}Step 4: Initializing database sources...${NC}"

# 4. DuckDB
echo "Generating test_data.duckdb..."
$DTPIPE -i "generate:1000" \
  --fake "Id:random.guid" \
  --fake "City:address.city" \
  --fake "Country:address.country" \
  --drop "GenerateIndex" \
  -o "$ARTIFACTS_DIR/test_data.duckdb" --table "geography" --no-schema-validation --strategy Recreate || exit 1

# 5. PostgreSQL (always runs: --pre-exec drops any stale table so Recreate always uses source schema)
echo "Initializing PostgreSQL users_test..."
$DTPIPE -i "generate:1000" \
  --fake "id:random.guid" \
  --fake "username:internet.userName" \
  --fake "last_login:date.past" \
  --drop "GenerateIndex" \
  -o "$PG" \
  --pre-exec "DROP TABLE IF EXISTS users_test CASCADE" \
  --table "users_test" --no-schema-validation --strategy Recreate || exit 1

# 6. SQL Server (always runs: --pre-exec drops any stale table so Recreate always uses source schema)
echo "Initializing SQL Server users_test..."
$DTPIPE -i "generate:1000" \
  --fake "id:random.guid" \
  --fake "display_name:name.fullName" \
  --fake "credit_card:finance.creditCardNumber" \
  --drop "GenerateIndex" \
  -o "$MSSQL" \
  --pre-exec "IF OBJECT_ID('users_test', 'U') IS NOT NULL DROP TABLE users_test" \
  --table "users_test" --no-schema-validation --strategy Recreate || exit 1

# 7. Oracle (always runs)
echo "Initializing Oracle USERS_TEST_DATA..."
$DTPIPE -i "generate:1000" \
  --fake "ID:random.guid" \
  --fake "FULL_NAME:name.fullName" \
  --fake "JOB_TITLE:name.jobTitle" \
  --drop "GenerateIndex" \
  -o "$ORA" \
  --table "USERS_TEST_DATA" \
  --no-schema-validation \
  --strategy Recreate \
  --pre-exec "BEGIN EXECUTE IMMEDIATE 'DROP TABLE USERS_TEST_DATA'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;" || exit 1

# 8. PostgreSQL wrong_schema (always runs: intentionally incompatible types for T80/T140 tests)
# test_data.parquet has (Id:uuid, Name:text, Category:text, Price:text).
# wrong_schema must have columns with incompatible types so --strict-schema (T80) and
# Recreate incompatibility detection (T140) both reject the write.
echo "Initializing PostgreSQL wrong_schema (incompatible types for T80/T140)..."
$DTPIPE -i "generate:1" \
  --fake "id:random.number" \
  --fake "name:random.number" \
  --fake "category:random.number" \
  --fake "price:random.number" \
  --drop "GenerateIndex" \
  -o "$PG" \
  --pre-exec "DROP TABLE IF EXISTS wrong_schema CASCADE" \
  --table "wrong_schema" --no-schema-validation --strategy Recreate || exit 1

# Restricted directory for T77 (chmod 000 = no read/write/execute)
if [ ! -d "$ARTIFACTS_DIR/restricted" ]; then
    mkdir -p "$ARTIFACTS_DIR/restricted"
    chmod 000 "$ARTIFACTS_DIR/restricted"
    echo "Created restricted/ directory (chmod 000) for T77 access-denied tests."
else
    echo "  Skipping restricted/ (already exists, has no content to go stale)"
fi

# 9. JS script file for T50 (--compute "@file" test)
echo "Generating my_script.js..."
cat <<'EOF' > "$ARTIFACTS_DIR/my_script.js"
row.FirstName + ' ' + row.LastName
EOF

# 8. Complex JSONL (Nested Structures)
echo "Generating complex_data.jsonl..."
cat <<EOF > "$ARTIFACTS_DIR/complex_data.jsonl"
{"id": 1, "user": {"name": "Alice", "points": 100}, "items": [{"id": "A1", "price": 10.5}, {"id": "A2", "price": 5.0}]}
{"id": 2, "user": {"name": "Bob", "points": 200}, "items": [{"id": "B1", "price": 20.0}]}
{"id": 3, "user": {"name": "Charlie", "points": 300}, "items": []}
EOF

# 9. Massive XML (for XML streaming validation)
if [ ! -f "$ARTIFACTS_DIR/test_data_massive.xml" ]; then
    echo "Generating test_data_massive.xml (2GB)..."
    bash "$SCRIPT_DIR/generate_massive_xml.sh" "$ARTIFACTS_DIR/test_data_massive.xml" 2 || exit 1
else
    echo "  Skipping test_data_massive.xml (already exists)"
    KEPT=$((KEPT + 1))
fi

if [ $KEPT -gt 0 ]; then
    echo ""
    echo -e "${YELLOW}NOTE: $KEPT volume fixture(s) were kept, not regenerated.${NC}"
    echo -e "${YELLOW}      Only their size matters to the tests that read them, so age is${NC}"
    echo -e "${YELLOW}      harmless. Regenerate anyway with: $(basename "$0") --clean${NC}"
fi

echo -e "${GREEN}All sources initialized successfully!${NC}"
