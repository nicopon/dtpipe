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
    rm -f "$ARTIFACTS_DIR"/trans_*.csv "$ARTIFACTS_DIR"/trans_*.arrow
}
trap cleanup EXIT
cleanup

A="$ARTIFACTS_DIR"

# ----------------------------------------
# Step 0: Generate reference source
# ----------------------------------------
echo "--- [Setup] Generating reference CSV ---"
"$DTPIPE" -i "generate:20" \
  --fake "Id:random.number" \
  --fake "Name:name.fullName" \
  --fake "Amount:finance.amount" \
  --fake "Secret:internet.password" \
  --drop "GenerateIndex" \
  --fake-seed 42 \
  -o "$A/trans_source.csv" --no-stats

# ----------------------------------------
# 1. Overwrite
# ----------------------------------------
echo "--- [1] Overwrite ---"
"$DTPIPE" -i "$A/trans_source.csv" \
  --overwrite "Secret:HIDDEN" \
  -o "$A/trans_overwrite.csv" --no-stats

COUNT=$(grep -c "HIDDEN" "$A/trans_overwrite.csv" || true)
[ "$COUNT" -ge 20 ] && pass "Overwrite: found HIDDEN in $COUNT rows" || fail "Overwrite: expected >=20 HIDDEN, got $COUNT"

# ----------------------------------------
# 2. Null
# ----------------------------------------
echo "--- [2] Null ---"
"$DTPIPE" -i "$A/trans_source.csv" \
  --null "Amount" \
  -o "$A/trans_null.csv" --no-stats

# Amount is column 3; after header, no digit values expected
NON_EMPTY=$(cut -d',' -f3 "$A/trans_null.csv" | tail -n +2 | grep "[0-9]" | wc -l | tr -d ' ')
[ "$NON_EMPTY" -eq 0 ] && pass "Null: Amount column is empty" || fail "Null: found $NON_EMPTY non-empty Amount values"

# ----------------------------------------
# 3. Mask
# ----------------------------------------
echo "--- [3] Mask ---"
"$DTPIPE" -i "$A/trans_source.csv" \
  --mask "Name" \
  -o "$A/trans_mask.csv" --no-stats

# Original faked names should not appear verbatim
ORIG=$(head -2 "$A/trans_source.csv" | tail -1 | cut -d',' -f2)
MATCH=$(grep -c "$ORIG" "$A/trans_mask.csv" 2>/dev/null || true)
[ "$MATCH" -eq 0 ] && pass "Mask: original name not present" || fail "Mask: original name leaked ($ORIG found)"

# ----------------------------------------
# 4. Fake
# ----------------------------------------
echo "--- [4] Fake ---"
"$DTPIPE" -i "$A/trans_source.csv" \
  --fake "Name:name.firstName" \
  --fake-seed 99 \
  -o "$A/trans_fake.csv" --no-stats

ORIG_NAME=$(head -2 "$A/trans_source.csv" | tail -1 | cut -d',' -f2)
NEW_NAME=$(head -2 "$A/trans_fake.csv" | tail -1 | cut -d',' -f2)
[ "$ORIG_NAME" != "$NEW_NAME" ] && pass "Fake: Name replaced" || fail "Fake: Name unchanged"

# ----------------------------------------
# 5. Format
# ----------------------------------------
echo "--- [5] Format ---"
"$DTPIPE" -i "generate:5" \
  --fake "First:name.firstName" \
  --fake "Last:name.lastName" \
  --drop "GenerateIndex" \
  --format "Full:{First} {Last}" \
  -o "$A/trans_format.csv" --no-stats

# Full column (3rd) should contain a space
FIRST_FULL=$(awk -F',' 'NR==2 {print $3}' "$A/trans_format.csv" | tr -d '\r')
[[ "$FIRST_FULL" == *" "* ]] && pass "Format: 'Full' column has space" || fail "Format: unexpected value '$FIRST_FULL'"

# ----------------------------------------
# 6. Compute (JS expression)
# ----------------------------------------
echo "--- [6] Compute ---"
"$DTPIPE" -i "generate:5" \
  --fake "Val:random.number" \
  --drop "GenerateIndex" \
  --compute "Double:row.Val * 2" \
  -o "$A/trans_compute.csv" --no-stats

COUNT=$(wc -l < "$A/trans_compute.csv" | tr -d ' ')
[ "$COUNT" -gt 1 ] && pass "Compute: output has $COUNT lines" || fail "Compute: no output"

# ----------------------------------------
# 7. Drop
# ----------------------------------------
echo "--- [7] Drop ---"
"$DTPIPE" -i "$A/trans_source.csv" \
  --drop "Secret" \
  -o "$A/trans_drop.csv" --no-stats

HEADER=$(head -1 "$A/trans_drop.csv" | tr -d '\r')
[[ "$HEADER" != *"Secret"* ]] && pass "Drop: Secret column removed" || fail "Drop: Secret still present in header"

# ----------------------------------------
# 8. Project
# ----------------------------------------
echo "--- [8] Project ---"
"$DTPIPE" -i "$A/trans_source.csv" \
  --project "Id,Name" \
  -o "$A/trans_project.csv" --no-stats

HEADER=$(head -1 "$A/trans_project.csv" | tr -d '\r')
[[ "$HEADER" == "Id,Name" ]] && pass "Project: only Id,Name retained" || fail "Project: unexpected header '$HEADER'"

# ----------------------------------------
# 9. Rename
# ----------------------------------------
echo "--- [9] Rename ---"
"$DTPIPE" -i "$A/trans_source.csv" \
  --rename "Name:FullName" \
  -o "$A/trans_rename.csv" --no-stats

HEADER=$(head -1 "$A/trans_rename.csv" | tr -d '\r')
[[ "$HEADER" == *"FullName"* ]] && pass "Rename: FullName present" || fail "Rename: unexpected header '$HEADER'"

# ----------------------------------------
# 10. Filter (JS predicate)
# ----------------------------------------
echo "--- [10] Filter ---"
"$DTPIPE" -i "generate:100" \
  --fake "Id:random.number" \
  --drop "GenerateIndex" \
  --filter "row.Id % 2 == 0" \
  -o "$A/trans_filter.csv" --no-stats

COUNT=$(wc -l < "$A/trans_filter.csv" | tr -d ' ')
[ "$COUNT" -gt 1 ] && pass "Filter: output has $COUNT lines" || fail "Filter: no output"

# ----------------------------------------
# 11. Expand (JS row explosion)
# ----------------------------------------
echo "--- [11] Expand ---"
"$DTPIPE" -i "generate:10" \
  --fake "Tags:['A','B','C']" \
  --drop "GenerateIndex" \
  --expand "JSON.parse(row.Tags.replace(/'/g, '\"')).map(t => ({ ...row, Tags: t }))" \
  -o "$A/trans_expand.csv" --no-stats

# 10 rows * 3 tags = 30 + 1 header
COUNT=$(wc -l < "$A/trans_expand.csv" | tr -d ' ')
[ "$COUNT" -eq 31 ] && pass "Expand: 31 lines (10*3+header)" || fail "Expand: expected 31 lines, got $COUNT"

# ----------------------------------------
# 12. Window
# ----------------------------------------
echo "--- [12] Window ---"
"$DTPIPE" -i "generate:20" \
  --fake "Val:random.number" \
  --drop "GenerateIndex" \
  --window-count 10 \
  --window-script "rows.map(r => ({ ...r, Val: 99999 }))" \
  -o "$A/trans_window.csv" --no-stats

FIRST_VAL=$(awk -F',' 'NR==2 {print $1}' "$A/trans_window.csv" | tr -d '\r')
[ "$FIRST_VAL" -eq 99999 ] 2>/dev/null && pass "Window: Val=99999" || fail "Window: unexpected Val='$FIRST_VAL'"

# ----------------------------------------
# 13. Transformer ordering (interleaved)
# ----------------------------------------
echo "--- [13] Transformer ordering ---"
"$DTPIPE" -i "generate:1" \
  --fake "A:lorem.word" --fake "B:lorem.word" --fake "C:lorem.word" \
  --drop "GenerateIndex" \
  --overwrite "A:Val1" \
  --format "B:{A}" \
  --overwrite "A:Val2" \
  --format "C:{A}" \
  -o "$A/trans_order.csv" --no-stats

# B should be "Val1", C should be "Val2", A should be "Val2"
A_VAL=$(awk -F',' 'NR==2 {print $1}' "$A/trans_order.csv" | tr -d '\r')
B_VAL=$(awk -F',' 'NR==2 {print $2}' "$A/trans_order.csv" | tr -d '\r')
C_VAL=$(awk -F',' 'NR==2 {print $3}' "$A/trans_order.csv" | tr -d '\r')
[ "$A_VAL" = "Val2" ] && pass "Ordering: A=Val2" || fail "Ordering: A expected Val2, got '$A_VAL'"
[ "$B_VAL" = "Val1" ] && pass "Ordering: B=Val1" || fail "Ordering: B expected Val1, got '$B_VAL'"
[ "$C_VAL" = "Val2" ] && pass "Ordering: C=Val2" || fail "Ordering: C expected Val2, got '$C_VAL'"

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
