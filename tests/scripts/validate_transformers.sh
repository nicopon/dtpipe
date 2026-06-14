#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
ARTIFACTS_DIR="$SCRIPT_DIR/artifacts"
mkdir -p "$ARTIFACTS_DIR"

DTPIPE="$PROJECT_ROOT/dist/release/dtpipe"
export DTPIPE_NO_TUI=1

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

pass() { echo -e "${GREEN}  OK: $1${NC}"; }
fail() { echo -e "${RED}  FAIL: $1${NC}"; exit 1; }

verify_baseline() {
    local actual_file=$1 name=$2
    local baseline_file="$SCRIPT_DIR/baselines/${name}.csv"
    
    mkdir -p "$SCRIPT_DIR/baselines"

    if [ "$UPDATE_BASELINES" = "1" ]; then
        cp "$actual_file" "$baseline_file"
        pass "$name: baseline updated"
    else
        if [ ! -f "$baseline_file" ]; then
            fail "$name: baseline not found at $baseline_file. Run with UPDATE_BASELINES=1 to generate."
            return
        fi
        
        if diff -u "$baseline_file" "$actual_file" > "$A/temp.diff"; then
            pass "$name: matches baseline"
            rm -f "$A/temp.diff"
        else
            echo -e "${RED}  FAIL: $name: mismatch with baseline! Diff:${NC}"
            cat "$A/temp.diff"
            rm -f "$A/temp.diff"
            fail "$name: mismatch with baseline"
        fi
    fi
}

echo "========================================"
echo "    DtPipe Transformer Validation"
echo "========================================"

if [ ! -f "$DTPIPE" ]; then
    echo "Building release..."
    "$PROJECT_ROOT/build.sh" > /dev/null
fi

if [ ! -f "$DTPIPE" ]; then
    echo "Error: binary not found at $DTPIPE" && exit 1
fi

cleanup() {
    rm -f "$ARTIFACTS_DIR"/trans_*.csv "$ARTIFACTS_DIR"/trans_*.arrow "$ARTIFACTS_DIR"/*.checksum
}
trap cleanup EXIT

A="$ARTIFACTS_DIR"

# ----------------------------------------
# Step 0: Generate reference source
# ----------------------------------------
echo "--- [Setup] Generating reference CSV ---"
"$DTPIPE" -i "generate:20" \
  --fake "Id:random.number" --fake-seed-row \
  --fake "Name:name.fullName" --fake-seed-row \
  --fake "Amount:finance.amount" --fake-seed-row \
  --fake "Secret:internet.password" --fake-seed-row \
  --drop "GenerateIndex" \
  -o "$A/trans_source.csv" --no-stats

# ----------------------------------------
# 1. Overwrite
# ----------------------------------------
echo "--- [1] Overwrite ---"
"$DTPIPE" -i "$A/trans_source.csv" \
  --overwrite "Secret:HIDDEN" \
  -o "$A/trans_overwrite.csv" --no-stats

verify_baseline "$A/trans_overwrite.csv" "Overwrite"

# ----------------------------------------
# 2. Null
# ----------------------------------------
echo "--- [2] Null ---"
"$DTPIPE" -i "$A/trans_source.csv" \
  --null "Amount" \
  -o "$A/trans_null.csv" --no-stats

verify_baseline "$A/trans_null.csv" "Null"

# ----------------------------------------
# 3. Mask
# ----------------------------------------
echo "--- [3] Mask ---"
"$DTPIPE" -i "$A/trans_source.csv" \
  --mask "Name" \
  -o "$A/trans_mask.csv" --no-stats

verify_baseline "$A/trans_mask.csv" "Mask"

# ----------------------------------------
# 4. Fake
# ----------------------------------------
echo "--- [4] Fake ---"
"$DTPIPE" -i "$A/trans_source.csv" \
  --fake "Name:name.firstName" --fake-seed-row \
  -o "$A/trans_fake.csv" --no-stats

verify_baseline "$A/trans_fake.csv" "Fake"

# ----------------------------------------
# 5. Format
# ----------------------------------------
echo "--- [5] Format ---"
"$DTPIPE" -i "generate:5" \
  --fake "First:name.firstName" --fake-seed-row \
  --fake "Last:name.lastName" --fake-seed-row \
  --drop "GenerateIndex" \
  --format "Full:{First} {Last}" \
  -o "$A/trans_format.csv" --no-stats

verify_baseline "$A/trans_format.csv" "Format"

# ----------------------------------------
# 6. Compute (JS expression)
# ----------------------------------------
echo "--- [6] Compute ---"
"$DTPIPE" -i "generate:5" \
  --fake "Val:random.number" --fake-seed-row \
  --drop "GenerateIndex" \
  --compute "Double:row.Val * 2" \
  -o "$A/trans_compute.csv" --no-stats

verify_baseline "$A/trans_compute.csv" "Compute"

# ----------------------------------------
# 7. Drop
# ----------------------------------------
echo "--- [7] Drop ---"
"$DTPIPE" -i "$A/trans_source.csv" \
  --drop "Secret" \
  -o "$A/trans_drop.csv" --no-stats

verify_baseline "$A/trans_drop.csv" "Drop"

# ----------------------------------------
# 8. Project
# ----------------------------------------
echo "--- [8] Project ---"
"$DTPIPE" -i "$A/trans_source.csv" \
  --project "Id,Name" \
  -o "$A/trans_project.csv" --no-stats

verify_baseline "$A/trans_project.csv" "Project"

# ----------------------------------------
# 9. Rename
# ----------------------------------------
echo "--- [9] Rename ---"
"$DTPIPE" -i "$A/trans_source.csv" \
  --rename "Name:FullName" \
  -o "$A/trans_rename.csv" --no-stats

verify_baseline "$A/trans_rename.csv" "Rename"

# ----------------------------------------
# 10. Filter (JS predicate)
# ----------------------------------------
echo "--- [10] Filter ---"
"$DTPIPE" -i "generate:100" \
  --fake "Id:random.number" --fake-seed-row \
  --drop "GenerateIndex" \
  --filter "row.Id % 2 == 0" \
  -o "$A/trans_filter.csv" --no-stats

verify_baseline "$A/trans_filter.csv" "Filter"

# ----------------------------------------
# 11. Expand (JS row explosion)
# ----------------------------------------
echo "--- [11] Expand ---"
"$DTPIPE" -i "generate:10" \
  --fake "Tags:['A','B','C']" --fake-seed-row \
  --drop "GenerateIndex" \
  --expand "JSON.parse(row.Tags.replace(/'/g, '\"')).map(t => ({ ...row, Tags: t }))" \
  -o "$A/trans_expand.csv" --no-stats

verify_baseline "$A/trans_expand.csv" "Expand"

# ----------------------------------------
# 12. Window
# ----------------------------------------
echo "--- [12] Window ---"
"$DTPIPE" -i "generate:20" \
  --fake "Val:random.number" --fake-seed-row \
  --drop "GenerateIndex" \
  --window-count 10 \
  --window-script "rows.map(r => ({ ...r, Val: 99999 }))" \
  -o "$A/trans_window.csv" --no-stats

verify_baseline "$A/trans_window.csv" "Window"

# ----------------------------------------
# 13. Transformer ordering (interleaved)
# ----------------------------------------
echo "--- [13] Transformer ordering ---"
"$DTPIPE" -i "generate:1" \
  --fake "A:lorem.word" --fake-seed-row \
  --fake "B:lorem.word" --fake-seed-row \
  --fake "C:lorem.word" --fake-seed-row \
  --drop "GenerateIndex" \
  --overwrite "A:Val1" \
  --format "B:{A}" \
  --overwrite "A:Val2" \
  --format "C:{A}" \
  -o "$A/trans_order.csv" --no-stats

verify_baseline "$A/trans_order.csv" "Ordering"

# ----------------------------------------
# 14. CLI --help sanity check
# ----------------------------------------
echo "--- [14] CLI help flags ---"
HELP=$("$DTPIPE" --help 2>&1)
for flag in --overwrite --null --mask --fake --format --compute --drop --project --rename --filter --expand --window-count; do
    echo "$HELP" | grep -q -- "$flag" || fail "Missing flag $flag in --help"
done
pass "All transformer flags present in --help"

echo ""
echo -e "${GREEN}All transformer tests passed!${NC}"
