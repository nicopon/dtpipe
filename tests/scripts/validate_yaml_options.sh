#!/bin/bash
set -e

# Setup
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$SCRIPT_DIR/../../"
TEST_DIR="$SCRIPT_DIR/artifacts/yaml_options"
rm -rf "$TEST_DIR"
mkdir -p "$TEST_DIR"

DB_OUT="$TEST_DIR/output.db"
JOB_FILE="$TEST_DIR/job.yaml"
CHECK_TABLE_CSV="$TEST_DIR/check_table.csv"
CHECK_DATA_CSV="$TEST_DIR/check_data.csv"

# Create Job File with providerOptions
# We verify that 'strategy' (enum) and 'table' (string) are correctly mapped
cat <<EOF > "$JOB_FILE"
input: "duck::memory:"
query: "SELECT 1 as id, 'Test' as name"
output: "sqlite:$DB_OUT"
provider-options:
  sqlite:
    table: "CustomTable"
    strategy: "Recreate"
EOF

echo "Running DtPipe (Creation)..."
"$ROOT_DIR/dist/release/dtpipe" --job "$JOB_FILE"

echo "Verifying output..."

# 1. Verify Table Name using DtPipe
# We query sqlite_master table from the generated DB
"$ROOT_DIR/dist/release/dtpipe" --input "sqlite:$DB_OUT" \
    --query "SELECT count(*) as cnt FROM sqlite_master WHERE type='table' AND name='CustomTable'" \
    --output "csv:$CHECK_TABLE_CSV"

TABLE_COUNT=$(tail -n 1 "$CHECK_TABLE_CSV" | tr -d '\r' | tr -d ' ')

if [[ "$TABLE_COUNT" == "1" ]]; then
    echo "[PASS] Table 'CustomTable' found."
else
    echo "[FAIL] Table 'CustomTable' NOT found (Count: $TABLE_COUNT)."
    exit 1
fi

# 2. Verify Data using DtPipe
"$ROOT_DIR/dist/release/dtpipe" --input "sqlite:$DB_OUT" \
    --query "SELECT count(*) as cnt FROM CustomTable" \
    --output "csv:$CHECK_DATA_CSV"

ROW_COUNT=$(tail -n 1 "$CHECK_DATA_CSV" | tr -d '\r' | tr -d ' ')
if [[ "$ROW_COUNT" == "1" ]]; then
    echo "[PASS] Data row found."
else
    echo "[FAIL] Data row NOT found (Count: $ROW_COUNT)."
    exit 1
fi

echo "========================================"
echo "YAML Provider Options Validation PASSED!"
echo "========================================"
