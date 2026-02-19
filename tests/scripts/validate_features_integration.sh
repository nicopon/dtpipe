#!/bin/bash
set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
DIST_DIR="$PROJECT_ROOT/dist/release"
DTPIPE_BIN="$DIST_DIR/dtpipe"
ARTIFACTS_DIR="$SCRIPT_DIR/artifacts"
mkdir -p "$ARTIFACTS_DIR"

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

echo "========================================"
echo "    DtPipe Feature Integration Test"
echo "========================================"
echo "Testing: JS Transformers + Schema Validation + Auto-Migration + Metrics + Hooks"

# 1. Setup Source Data
SOURCE_CSV="$ARTIFACTS_DIR/integ_source.csv"
echo "Id,Name,Category,Tags" > "$SOURCE_CSV"
# Row 1: Should pass filter, expand to 2 rows
echo "2,Product A,Electronics,\"['New','Sale']\"" >> "$SOURCE_CSV"
# Row 2: Should be filtered out (Odd ID)
echo "3,Product B,Electronics,\"['Old']\"" >> "$SOURCE_CSV"
# Row 3: Should pass filter, expand to 1 row
echo "4,Product C,Home,\"['Regular']\"" >> "$SOURCE_CSV"

# 3. Setup Target DB (Schema mismatch for auto-migration test)
TARGET_DB="$ARTIFACTS_DIR/integ_target.db"
rm -f "$TARGET_DB"
sqlite3 "$TARGET_DB" "CREATE TABLE products (Id INTEGER, Name TEXT, Category TEXT); CREATE TABLE logs (message TEXT, created_at DATETIME DEFAULT CURRENT_TIMESTAMP);"
# Note: missing 'Tag' column which comes from Expand

# 3. Setup Metrics
METRICS_FILE="$ARTIFACTS_DIR/integ_metrics.json"
rm -f "$METRICS_FILE"

# 4. Run DtPipe
# - Filter: Id % 2 == 0
# - Expand: Tags -> split and create new rows with 'Tag' column
# - Auto-Migrate: Add 'Tag' column to target
# - Metrics: JSON output
# - Hooks: Log to table
echo "🚀 Running integration pipeline..."
$DTPIPE_BIN \
    --input "csv:$SOURCE_CSV" \
    --query "SELECT * FROM data" \
    --filter "row.Id % 2 == 0" \
    --expand "JSON.parse(row.Tags.replace(/'/g, '\"')).map(t => ({ ...row, Tag: t }))" \
    --output "sqlite:Data Source=$TARGET_DB" \
    --table "products" \
    --auto-migrate \
    --metrics-path "$METRICS_FILE" \
    --pre-exec "INSERT INTO logs (message) VALUES ('PRE_EXEC: Starting')" \
    --post-exec "INSERT INTO logs (message) VALUES ('POST_EXEC: Finished')" 

# 5. Verifications

ERRORS=0

# A. Data Count
# Row 2 (Product A) -> 2 tags -> 2 rows
# Row 4 (Product C) -> 1 tag -> 1 row
# Total expected: 3 rows
COUNT=$(sqlite3 "$TARGET_DB" "SELECT COUNT(*) FROM products;")
if [ "$COUNT" -eq 3 ]; then
    echo -e "${GREEN}✓ Correct row count (3)${NC}"
else
    echo -e "${RED}✗ Row count mismatch. Expected 3, got $COUNT${NC}"
    ERRORS=$((ERRORS + 1))
fi

# B. Auto-Migration (Column 'Tag' exists?)
SCHEMA=$(sqlite3 "$TARGET_DB" ".schema products")
if [[ "$SCHEMA" == *"Tag"* ]]; then
    echo -e "${GREEN}✓ Auto-migration added 'Tag' column${NC}"
else
    echo -e "${RED}✗ Auto-migration failed (Tag column missing)${NC}"
    ERRORS=$((ERRORS + 1))
fi

# C. Metrics File
if [ -f "$METRICS_FILE" ] && grep -q "WriteCount" "$METRICS_FILE"; then
    echo -e "${GREEN}✓ Metrics file created and valid${NC}"
else
    echo -e "${RED}✗ Metrics file missing or invalid${NC}"
    ERRORS=$((ERRORS + 1))
fi

# D. Hooks Log
LOG_COUNT=$(sqlite3 "$TARGET_DB" "SELECT COUNT(*) FROM logs WHERE message LIKE '%PRE_EXEC%' OR message LIKE '%POST_EXEC%';")
if [ "$LOG_COUNT" -eq 2 ]; then
    echo -e "${GREEN}✓ Hooks executed correctly (2 log entries found)${NC}"
else
    echo -e "${RED}✗ Hooks execution missing (Expected 2 log entries, got $LOG_COUNT)${NC}"
    ERRORS=$((ERRORS + 1))
fi

if [ "$ERRORS" -eq 0 ]; then
    echo -e "\n${GREEN}🎉 All Integrated Features Passed!${NC}"
    exit 0
else
    echo -e "\n${RED}✗ $ERRORS tests failed!${NC}"
    exit 1
fi
