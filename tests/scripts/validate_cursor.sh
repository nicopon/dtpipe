#!/bin/bash
set -e

# validate_cursor.sh
# Tests --cursor, --state, and ${{cursor://...}} incremental loading.
# Uses SQLite as both source and target. No Docker required.

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
echo "    DtPipe Cursor Validation"
echo "========================================"

if [ ! -f "$DTPIPE" ]; then
    echo "Building release..."
    "$PROJECT_ROOT/build.sh" > /dev/null
fi

SRC_DB="$ARTIFACTS_DIR/cursor_src.db"
TGT_DB="$ARTIFACTS_DIR/cursor_tgt.db"
STATE_FILE="$ARTIFACTS_DIR/cursor_test.sync"

rm -f "$SRC_DB" "$TGT_DB" "$STATE_FILE"

# -------------------------------------------------------------
# Scenario 1: Full load (first run, no state file)
# -------------------------------------------------------------
echo "--- Scenario 1: Full load (no state file) ---"

# Create source table and insert 5 rows with increasing updated_at
sqlite3 "$SRC_DB" "CREATE TABLE source_table (id INTEGER PRIMARY KEY, name TEXT, updated_at TEXT);"
sqlite3 "$SRC_DB" "INSERT INTO source_table VALUES (1, 'Alice', '2026-06-16T10:00:00.000');"
sqlite3 "$SRC_DB" "INSERT INTO source_table VALUES (2, 'Bob', '2026-06-16T11:00:00.000');"
sqlite3 "$SRC_DB" "INSERT INTO source_table VALUES (3, 'Charlie', '2026-06-16T12:00:00.000');"
sqlite3 "$SRC_DB" "INSERT INTO source_table VALUES (4, 'David', '2026-06-16T13:00:00.000');"
sqlite3 "$SRC_DB" "INSERT INTO source_table VALUES (5, 'Eve', '2026-06-16T14:00:00.000');"

# Run dtpipe
"$DTPIPE" \
  -i "sqlite:Data Source=$SRC_DB" \
  --query "SELECT * FROM source_table WHERE updated_at >= '\${{cursor://$STATE_FILE|2026-06-16T00:00:00.000}}'" \
  -o "sqlite:Data Source=$TGT_DB" \
  --table "target_table" \
  --strategy Recreate \
  --key id \
  --cursor "updated_at" \
  --state "$STATE_FILE" \
  --no-stats

# Verify target has 5 rows
TGT_COUNT=$(sqlite3 "$TGT_DB" "SELECT COUNT(*) FROM target_table;")
[ "$TGT_COUNT" -eq 5 ] && pass "Scenario 1: target has 5 rows" || fail "Scenario 1: expected 5 rows, got $TGT_COUNT"

# Verify state file exists and has the correct value
[ -f "$STATE_FILE" ] && pass "Scenario 1: state file exists" || fail "Scenario 1: state file missing"
STATE_VAL=$(grep -o '"value": "[^"]*"' "$STATE_FILE" | head -n 1 | cut -d'"' -f4)
[ "$STATE_VAL" == "2026-06-16T14:00:00.000" ] && pass "Scenario 1: state file has correct max value" || fail "Scenario 1: expected 2026-06-16T14:00:00.000, got $STATE_VAL"

# -------------------------------------------------------------
# Scenario 2: Incremental load (second run)
# -------------------------------------------------------------
echo "--- Scenario 2: Incremental load ---"

# Add 3 new rows to source
sqlite3 "$SRC_DB" "INSERT INTO source_table VALUES (6, 'Frank', '2026-06-16T15:00:00.000');"
sqlite3 "$SRC_DB" "INSERT INTO source_table VALUES (7, 'Grace', '2026-06-16T16:00:00.000');"
sqlite3 "$SRC_DB" "INSERT INTO source_table VALUES (8, 'Heidi', '2026-06-16T17:00:00.000');"

# Run dtpipe incremental (query uses cursor, strategy Upsert)
"$DTPIPE" \
  -i "sqlite:Data Source=$SRC_DB" \
  --query "SELECT * FROM source_table WHERE updated_at > '\${{cursor://$STATE_FILE}}'" \
  -o "sqlite:Data Source=$TGT_DB" \
  --table "target_table" \
  --strategy Upsert \
  --key id \
  --cursor "updated_at" \
  --state "$STATE_FILE" \
  --no-stats

# Verify target has 8 rows
TGT_COUNT=$(sqlite3 "$TGT_DB" "SELECT COUNT(*) FROM target_table;")
[ "$TGT_COUNT" -eq 8 ] && pass "Scenario 2: target has 8 rows" || fail "Scenario 2: expected 8 rows, got $TGT_COUNT"

# Verify state file has been updated to the new max value
STATE_VAL=$(grep -o '"value": "[^"]*"' "$STATE_FILE" | head -n 1 | cut -d'"' -f4)
[ "$STATE_VAL" == "2026-06-16T17:00:00.000" ] && pass "Scenario 2: state file updated to 2026-06-16T17:00:00.000" || fail "Scenario 2: expected 2026-06-16T17:00:00.000, got $STATE_VAL"

# -------------------------------------------------------------
# Scenario 3: Override with --cursor-from
# -------------------------------------------------------------
echo "--- Scenario 3: Override with --cursor-from ---"

# Override cursor value to 2026-06-16T14:30:00.000 using --cursor-from.
# This should select 6, 7, and 8 again. Since strategy is Upsert, target count stays 8.
"$DTPIPE" \
  -i "sqlite:Data Source=$SRC_DB" \
  --query "SELECT * FROM source_table WHERE updated_at > '\${{cursor://$STATE_FILE}}'" \
  -o "sqlite:Data Source=$TGT_DB" \
  --table "target_table" \
  --strategy Upsert \
  --key id \
  --cursor "updated_at" \
  --state "$STATE_FILE" \
  --cursor-from "2026-06-16T14:30:00.000" \
  --no-stats

TGT_COUNT=$(sqlite3 "$TGT_DB" "SELECT COUNT(*) FROM target_table;")
[ "$TGT_COUNT" -eq 8 ] && pass "Scenario 3: target still has 8 rows" || fail "Scenario 3: expected 8 rows, got $TGT_COUNT"

# Verify state file has been updated to the new max value
STATE_VAL=$(grep -o '"value": "[^"]*"' "$STATE_FILE" | head -n 1 | cut -d'"' -f4)
[ "$STATE_VAL" == "2026-06-16T17:00:00.000" ] && pass "Scenario 3: state file still has correct max value" || fail "Scenario 3: expected 2026-06-16T17:00:00.000, got $STATE_VAL"

# -------------------------------------------------------------
# Scenario 4: Missing cursor column -> error
# -------------------------------------------------------------
echo "--- Scenario 4: Missing cursor column ---"

# Run with nonexistent column. Expect failure.
set +e
"$DTPIPE" \
  -i "sqlite:Data Source=$SRC_DB" \
  --query "SELECT * FROM source_table" \
  -o "sqlite:Data Source=$TGT_DB" \
  --table "target_table" \
  --strategy Upsert \
  --key id \
  --cursor "nonexistent_col" \
  --state "$STATE_FILE" \
  --no-stats > "$ARTIFACTS_DIR/error.log" 2>&1
EXIT_CODE=$?
set -e

[ "$EXIT_CODE" -ne 0 ] && pass "Scenario 4: nonexistent column fails with non-zero exit code" || fail "Scenario 4: expected failure, got success"
grep -iq "nonexistent_col" "$ARTIFACTS_DIR/error.log" && pass "Scenario 4: error message references column name" || fail "Scenario 4: error message does not reference nonexistent_col"

# -------------------------------------------------------------
# Scenario 5: No data -> state file unchanged
# -------------------------------------------------------------
echo "--- Scenario 5: No data, state file unchanged ---"

# Save modified time of state file
mtime_before=$(stat -f "%m" "$STATE_FILE")
sleep 1.2 # wait to make sure mtime would change if written

# Run query that matches 0 rows (WHERE updated_at > Heidi's time)
"$DTPIPE" \
  -i "sqlite:Data Source=$SRC_DB" \
  --query "SELECT * FROM source_table WHERE updated_at > '\${{cursor://$STATE_FILE}}'" \
  -o "sqlite:Data Source=$TGT_DB" \
  --table "target_table" \
  --strategy Upsert \
  --key id \
  --cursor "updated_at" \
  --state "$STATE_FILE" \
  --no-stats

mtime_after=$(stat -f "%m" "$STATE_FILE")
[ "$mtime_before" -eq "$mtime_after" ] && pass "Scenario 5: state file was not modified when 0 rows processed" || fail "Scenario 5: state file was modified on empty run"

# Cleanup
rm -f "$SRC_DB" "$TGT_DB" "$STATE_FILE" "$ARTIFACTS_DIR/error.log"

echo ""
echo -e "${GREEN}Cursor validation complete!${NC}"
