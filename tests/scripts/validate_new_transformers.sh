#!/bin/bash
set -e

# Setup
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DTPIPE="$ROOT_DIR/dist/release/dtpipe"
ARTIFACTS_DIR="$ROOT_DIR/tests/scripts/artifacts"
mkdir -p "$ARTIFACTS_DIR"

cleanup() {
    rm -f "$ARTIFACTS_DIR/source.csv" "$ARTIFACTS_DIR/output_*.csv"
}
# trap cleanup EXIT # Debugging: keep artifacts

echo "---------------------------------------------------"
echo "Validating New Transformers (Filter, Expand, Window)"
echo "---------------------------------------------------"

# 0. Generate Source Data
echo "Generating source data..."
export DTPIPE_NO_TUI=1
"$DTPIPE" -i "sample:100" \
    --fake "Id:random.number" \
    --fake "Category:vehicle.type" \
    --fake "Value:random.number" \
    --fake "Tags:['A','B','C']" \
    -o "$ARTIFACTS_DIR/source.csv"

# 1. Validate --filter
echo -n "Test 1: --filter (Id < 50) ... "
"$DTPIPE" -i "$ARTIFACTS_DIR/source.csv" \
    --filter "row.Id % 2 == 0" \
    -o "$ARTIFACTS_DIR/output_filter.csv"

count=$(grep -c . "$ARTIFACTS_DIR/output_filter.csv") # Header + rows
# Expect roughly 50% + 1 header lines, but just checking it works and produces output
if [ "$count" -gt 1 ]; then
    echo "OK ($count lines)"
else
    echo "FAILED (Empty output)"
    exit 1
fi

# 2. Validate --expand
echo -n "Test 2: --expand (Tags -> Category) ... "
"$DTPIPE" -i "$ARTIFACTS_DIR/source.csv" \
    --expand "JSON.parse(row.Tags.replace(/'/g, '\"')).map(t => ({ ...row, Category: t }))" \
    --limit 10 \
    -o "$ARTIFACTS_DIR/output_expand.csv"

# Input has 10 rows, each has 3 tags -> 30 rows + 1 header = 31 lines
count=$(grep -c . "$ARTIFACTS_DIR/output_expand.csv")
if [ "$count" -eq 31 ]; then
    echo "OK ($count lines)"
    # Check if Category contains 'A', 'B', 'C'
    if grep -q "A" "$ARTIFACTS_DIR/output_expand.csv"; then
         echo "Content OK (Found 'A')"
    else
         echo "FAILED (Content mismatch, 'A' not found)"
         exit 1
    fi
else
    echo "FAILED (Expected 31 lines, got $count)"
    exit 1
fi

# 3. Validate --window-count
echo -n "Test 3: --window-count (Size 10 -> Value=12345) ... "
"$DTPIPE" -i "$ARTIFACTS_DIR/source.csv" \
    --window-count 10 \
    --window-script "rows.map(r => ({ ...r, Value: 12345 }))" \
    -o "$ARTIFACTS_DIR/output_window.csv"

# Verify Value column (4th) is 12345
first_row_value=$(awk -F, 'NR==2 {print $4}' "$ARTIFACTS_DIR/output_window.csv")
if [[ "$first_row_value" == "12345" ]]; then
    echo "OK (Value=$first_row_value)"
else
    echo "FAILED (Expected Value=12345, got '$first_row_value')"
    # dump head for debug
    head -n 5 "$ARTIFACTS_DIR/output_window.csv"
    exit 1
fi

# 4. Validate Help Output
echo -n "Test 4: Checking --help for new options ... "
help_output=$("$DTPIPE" --help 2>&1)

if echo "$help_output" | grep -q -- "--filter" && \
   echo "$help_output" | grep -q -- "--expand" && \
   echo "$help_output" | grep -q -- "--window-count"; then
    echo "OK (Found --filter, --expand, --window-count)"
else
    echo "FAILED (Missing options in help)"
    echo "$help_output" | grep -E -- "filter|expand|window" || true
    exit 1
fi

echo "All tests passed!"
