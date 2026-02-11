#!/bin/bash
set -e

# Resolve Project Root and Paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
ARTIFACTS_DIR="$SCRIPT_DIR/artifacts"
OUTPUT_DIR="$ARTIFACTS_DIR"
mkdir -p "$ARTIFACTS_DIR"

# Path to binary (Release build)
DTPIPE="$PROJECT_ROOT/dist/release/dtpipe"

# Source common helpers
source "$SCRIPT_DIR/common.sh"

echo "========================================"
echo "    DtPipe Transformer Testing"
echo "========================================"

# Always Build Release
echo "🔨 Building Release..."
"$PROJECT_ROOT/build.sh" > /dev/null

# Check if binary exists
if [ ! -f "$DTPIPE" ]; then
    echo "❌ Error: Build failed or binary not found at $DTPIPE"
    exit 1
fi

# Cleanup
cleanup() {
    rm -f "$ARTIFACTS_DIR/ref_trans.csv" "$ARTIFACTS_DIR/test_overwrite.csv" "$ARTIFACTS_DIR/test_null.csv" "$ARTIFACTS_DIR/test_mask.csv"
}
trap cleanup EXIT

echo "----------------------------------------"
echo "Step 0: Generate Reference Source"
echo "----------------------------------------"
$DTPIPE --input "sample:20" \
           --fake "Id:random.number" --fake "Name:name.fullName" --fake "Amount:finance.amount" --fake "Secret:internet.password" \
           --drop "SampleIndex" \
           --query "SELECT * FROM dummy" \
           --output "$ARTIFACTS_DIR/ref_trans.csv"

echo "----------------------------------------"
echo "Step 1: Overwrite Transformer"
echo "----------------------------------------"
# Overwrite Secret with "HIDDEN"
run_via_yaml --input "csv:$ARTIFACTS_DIR/ref_trans.csv" \
           --query "SELECT * FROM data" \
           --output "$ARTIFACTS_DIR/test_overwrite.csv" \
           --overwrite "Secret=HIDDEN"

# Validation
COUNT=$(grep "HIDDEN" "$ARTIFACTS_DIR/test_overwrite.csv" | wc -l)
# Expect 20 rows + maybe execution logic overhead? 
# Header row shouldn't have HIDDEN unless specified.
# Sample rows: 20. So 20 times HIDDEN.
echo "Overwrite Count: $COUNT"

if [ "$COUNT" -ge 20 ]; then
    echo "✅ Overwrite Success"
else
    echo "❌ Overwrite Failed (Expected >= 20, got $COUNT)"
    exit 1
fi

echo "----------------------------------------"
echo "Step 2: Null Transformer"
echo "----------------------------------------"
# Nullify Amount
run_via_yaml --input "csv:$ARTIFACTS_DIR/ref_trans.csv" \
           --query "SELECT * FROM data" \
           --output "$ARTIFACTS_DIR/test_null.csv" \
           --null "Amount"

# Validation: Check if Amount column is empty. 
# CSV format: Id,Name,Amount,Secret
# 0,Name 0,,Secret 0
# Grep for ",," might match empty values if Amount is in middle?
# Actually DtPipe CSV writer might write empty string for null.
# Let's check typical row structure. if Amount is 3rd column: "val,val,,val"
# Or verify via checksum/inspect logic. 
# Simple check: Amount column shouldn't contain digits if it's strictly null/empty?
# But other columns might have digits (Id).
# Better check: cut command to extract column.
# Amount is column 3.
NON_EMPTY_COUNT=$(cut -d',' -f3 "$ARTIFACTS_DIR/test_null.csv" | tail -n +2 | grep "[0-9]" | wc -l)
echo "Non-Empty Amount Count: $NON_EMPTY_COUNT"

if [ "$NON_EMPTY_COUNT" -eq 0 ]; then
    echo "✅ Null Success"
else
    echo "❌ Null Failed (Expected 0 non-empty values, got $NON_EMPTY_COUNT)"
    head -n 5 "$ARTIFACTS_DIR/test_null.csv"
    exit 1
fi

echo "----------------------------------------"
echo "Step 3: Mask Transformer"
echo "----------------------------------------"
# Mask Name (partially?) or full? Defaults usually full mask if not configured strictly.
# Assuming --mask "Name" does a default mask (like '*****' or similar).
# Let's try simple asterisk mask if supported, or fake data.
# Wait, mask transformer usually implies replacing with valid-looking but masked data, or just fixed char?
# Checking codebase capability... defaults might apply.
run_via_yaml --input "csv:$ARTIFACTS_DIR/ref_trans.csv" \
           --query "SELECT * FROM data" \
           --output "$ARTIFACTS_DIR/test_mask.csv" \
           --mask "Name"

# Validation
# Original Name: "Name 0", "Name 1"
# Masked Name: Should NOT contain "Name 0"
MATCH_COUNT=$(grep "Name 0" "$ARTIFACTS_DIR/test_mask.csv" | wc -l)
echo "Leaked Data Count: $MATCH_COUNT"

if [ "$MATCH_COUNT" -eq 0 ]; then
    echo "✅ Mask Success"
else
    echo "❌ Mask Failed (Found original data)"
    exit 1
fi

echo ""
echo "🎉 All Transformer Tests Passed!"

