#!/bin/bash
set -e

# Resolve Project Root and Paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
OUTPUT_DIR="$SCRIPT_DIR/output"

# Path to binary (Release build)
QUERYDUMP="$PROJECT_ROOT/dist/release/querydump"

# Source common helpers
source "$SCRIPT_DIR/common.sh"

echo "========================================"
echo "    QueryDump Project Transformer Testing"
echo "========================================"

# Always Build Release
echo "🔨 Building Release..."
"$PROJECT_ROOT/build.sh" > /dev/null

# Check if binary exists
if [ ! -f "$QUERYDUMP" ]; then
    echo "❌ Error: Build failed or binary not found at $QUERYDUMP"
    exit 1
fi

# Cleanup
cleanup() {
    rm -f "$OUTPUT_DIR/ref_project.csv" "$OUTPUT_DIR/test_drop.csv" "$OUTPUT_DIR/test_select.csv"
}
trap cleanup EXIT

echo "----------------------------------------"
echo "Step 0: Generate Reference Source"
echo "----------------------------------------"
# Create sample with 4 columns: Id, Name, Amount, Secret
# note: sample generation can be direct, no need to test yaml for setup phase usually, 
# but lets stick to consistent usage or just keep setup direct.
# Let's keep setup direct to avoid complexity if yaml export fails on sample generation (unlikely but simpler).
$QUERYDUMP --input "sample:10;Id=int;Name=string;Amount=double;Secret=string" \
           --query "SELECT * FROM dummy" \
           --output "$OUTPUT_DIR/ref_project.csv"

echo "----------------------------------------"
echo "Step 1: Drop Transformer (--drop)"
echo "----------------------------------------"
# Drop 'Secret' column
run_via_yaml --input "csv:$OUTPUT_DIR/ref_project.csv" \
           --query "SELECT * FROM data" \
           --output "$OUTPUT_DIR/test_drop.csv" \
           --drop "Secret"

# Validation
# Check header for 'Secret' (Should NOT be present)
if grep -q "Secret" "$OUTPUT_DIR/test_drop.csv"; then
    echo "❌ Drop Failed: 'Secret' column found in output."
    head -n 1 "$OUTPUT_DIR/test_drop.csv"
    exit 1
else
    echo "✅ Drop Success: 'Secret' column removed."
fi

# Check column count (Should be 3: Id, Name, Amount)
COL_COUNT=$(head -n 1 "$OUTPUT_DIR/test_drop.csv" | tr -cd ',' | wc -c)
# 3 columns = 2 commas
if [ "$COL_COUNT" -eq 2 ]; then
    echo "✅ Column Count Correct (3 columns)."
else
    echo "❌ Column Count Incorrect. Expected 2 commas (3 columns), got $COL_COUNT."
    head -n 1 "$OUTPUT_DIR/test_drop.csv"
    exit 1
fi

echo "----------------------------------------"
echo "Step 2: Project Transformer (--project)"
echo "----------------------------------------"
# Keep only 'Id' and 'Amount'
run_via_yaml --input "csv:$OUTPUT_DIR/ref_project.csv" \
           --query "SELECT * FROM data" \
           --output "$OUTPUT_DIR/test_select.csv" \
           --project "Id, Amount"

# Validation
# Check header
# Aggressively keep only alphanumeric and commas
HEADER=$(head -n 1 "$OUTPUT_DIR/test_select.csv" | tr -cd '[:alnum:],')
if [[ "$HEADER" == "Id,Amount" ]]; then
    echo "✅ Project Success: Header is '$HEADER'."
else
    echo "❌ Project Failed: Expected 'Id,Amount', got '$HEADER'."
    exit 1
fi

echo "----------------------------------------"
echo "Step 3: Drop + Project Combined"
echo "----------------------------------------"
# Should work? If supported.
# We implemented "Drop first, then Project from remaining".
# Let's try: Drop 'Name', Project 'Id, Secret' -> result 'Id, Secret'
# (Name is dropped, so ignoring it in project is easy. If we projected 'Name', it should be gone)

run_via_yaml --input "csv:$OUTPUT_DIR/ref_project.csv" \
           --query "SELECT * FROM data" \
           --output "$OUTPUT_DIR/test_combined.csv" \
           --drop "Name" \
           --project "Id, Secret"

HEADER_COMB=$(head -n 1 "$OUTPUT_DIR/test_combined.csv" | tr -cd '[:alnum:],')
if [[ "$HEADER_COMB" == "Id,Secret" ]]; then
    echo "✅ Combined Success: Header is '$HEADER_COMB'."
else
    echo "❌ Combined Failed: Expected 'Id,Secret', got '$HEADER_COMB'."
    exit 1
fi

echo "🎉 All Project Tests Passed!"

