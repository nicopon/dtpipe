#!/bin/bash
set -e

# Setup
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$SCRIPT_DIR/../../"
TEST_DIR="$ROOT_DIR/tests/output/yaml_options"
rm -rf "$TEST_DIR"
mkdir -p "$TEST_DIR"

DB_IN="$TEST_DIR/input.db"
DB_OUT="$TEST_DIR/output.db"
JOB_FILE="$TEST_DIR/job.yaml"

# Create input db
sqlite3 "$DB_IN" "CREATE TABLE Source (id INTEGER, name TEXT); INSERT INTO Source VALUES (1, 'Test');"

# Create Job File with providerOptions
# We verify that 'strategy' (enum) and 'table' (string) are correctly mapped
cat <<EOF > "$JOB_FILE"
input: "$DB_IN"
query: "SELECT * FROM Source"
output: "sqlite:$DB_OUT"
provider-options:
  sqlite:
    table: "CustomTable"
    strategy: "Recreate"
EOF

echo "Running QueryDump..."
"$ROOT_DIR/dist/release/querydump" --job "$JOB_FILE"

echo "Verifying output..."

# 1. Verify Table Name
TABLE_COUNT=$(sqlite3 "$DB_OUT" "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='CustomTable'")
if [[ "$TABLE_COUNT" -eq "1" ]]; then
    echo "[PASS] Table 'CustomTable' found."
else
    echo "[FAIL] Table 'CustomTable' NOT found."
    exit 1
fi

# 2. Verify Data
ROW_COUNT=$(sqlite3 "$DB_OUT" "SELECT count(*) FROM CustomTable")
if [[ "$ROW_COUNT" -eq "1" ]]; then
    echo "[PASS] Data row found."
else
    echo "[FAIL] Data row NOT found."
    exit 1
fi

echo "========================================"
echo "YAML Provider Options Validation PASSED!"
echo "========================================"
