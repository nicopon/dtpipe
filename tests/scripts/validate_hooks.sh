#!/bin/bash
set -e

# validate_hooks.sh
# Tests --pre-exec, --post-exec, --finally-exec hooks using SQLite.
# No Docker required.

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
echo "    DtPipe Hook Validation"
echo "========================================"

if [ ! -f "$DTPIPE" ]; then
    echo "Building release..."
    "$PROJECT_ROOT/build.sh" > /dev/null
fi

SRC_DB="$ARTIFACTS_DIR/hooks_source.db"
TGT_DB="$ARTIFACTS_DIR/hooks_target.db"
PRE_SQL="$ARTIFACTS_DIR/hooks_pre.sql"
rm -f "$SRC_DB" "$TGT_DB" "$PRE_SQL"

# Setup source
sqlite3 "$SRC_DB" "CREATE TABLE source_table (id INTEGER PRIMARY KEY, name TEXT);"
sqlite3 "$SRC_DB" "INSERT INTO source_table VALUES (1, 'Alice');"
sqlite3 "$SRC_DB" "INSERT INTO source_table VALUES (2, 'Bob');"

# Setup target (hook log table)
sqlite3 "$TGT_DB" "CREATE TABLE hook_logs (message TEXT, created_at DATETIME DEFAULT CURRENT_TIMESTAMP);"

# Pre-exec SQL file
echo "INSERT INTO hook_logs (message) VALUES ('Pre-Exec File Run');" > "$PRE_SQL"

echo "--- Running pipeline with all three hooks ---"
"$DTPIPE" \
  -i "sqlite:Data Source=$SRC_DB" \
  --query "SELECT * FROM source_table" \
  -o "sqlite:Data Source=$TGT_DB;Cache=Shared" \
  --limit 2 \
  --table "target_table" \
  --strategy Recreate \
  --pre-exec "@$PRE_SQL" \
  --post-exec "INSERT INTO hook_logs (message) VALUES ('Post-Exec Run')" \
  --finally-exec "INSERT INTO hook_logs (message) VALUES ('Finally-Exec Run')" \
  --no-stats

LOGS=$(sqlite3 "$TGT_DB" "SELECT message FROM hook_logs ORDER BY created_at;")
echo "  Hook logs: $LOGS"

[[ "$LOGS" == *"Pre-Exec File Run"* ]]  && pass "pre-exec file executed"  || fail "pre-exec file not found in logs"
[[ "$LOGS" == *"Post-Exec Run"* ]]      && pass "post-exec executed"      || fail "post-exec not found in logs"
[[ "$LOGS" == *"Finally-Exec Run"* ]]   && pass "finally-exec executed"   || fail "finally-exec not found in logs"

DATA_COUNT=$(sqlite3 "$TGT_DB" "SELECT COUNT(*) FROM target_table;")
[ "$DATA_COUNT" -eq 2 ] && pass "Target table has 2 rows" || fail "Expected 2 rows, got $DATA_COUNT"

# Cleanup
rm -f "$SRC_DB" "$TGT_DB" "$PRE_SQL"

echo ""
echo -e "${GREEN}Hook validation complete!${NC}"
