#!/bin/bash
set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
DIST_DIR="$PROJECT_ROOT/dist/release"
DTPIPE_BIN="$DIST_DIR/dtpipe"
LOCKER_SCRIPT="$SCRIPT_DIR/tools/Locker.cs"
ARTIFACTS_DIR="$SCRIPT_DIR/artifacts"
mkdir -p "$ARTIFACTS_DIR"

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'
YELLOW='\033[1;33m'

echo "========================================"
echo "    DtPipe Resilience (Locking) Test"
echo "========================================"

# Check if Locker.cs is executable
if [ ! -x "$LOCKER_SCRIPT" ]; then
    echo "🔧 Making Locker.cs executable..."
    chmod +x "$LOCKER_SCRIPT"
fi

DB_PATH="$ARTIFACTS_DIR/resilience_lock.db"
SIGNAL_FILE="$DB_PATH.signal"
rm -f "$DB_PATH" "$SIGNAL_FILE"

# 1. Idempotent Initialization
echo "🧹 Initializing Database..."
sqlite3 "$DB_PATH" "DROP TABLE IF EXISTS sensitive_data; CREATE TABLE sensitive_data (id INTEGER PRIMARY KEY, content TEXT);"

# 2. Run Test
echo "Test 1: Write while DB is locked (Should Retry and Succeed)..."

# Run C# Script in background (Locks for 3000ms)
"$LOCKER_SCRIPT" "$DB_PATH" "3000" &
LOCKER_PID=$!

# Give it a moment to acquire lock
SIGNAL_FILE="$DB_PATH.signal"
echo "⏳ Waiting for lock signal..."
for i in {1..30}; do
    if [ -f "$SIGNAL_FILE" ]; then
        echo "🔒 Lock signal detected!"
        break
    fi
    sleep 0.5
done

if [ ! -f "$SIGNAL_FILE" ]; then
    echo "❌ Timeout waiting for locker."
    kill $LOCKER_PID || true
    exit 1
fi

# Run DtPipe
echo "🚀 [DtPipe] Starting export (expecting retries)..."
START_TIME=$(date +%s)
"$DTPIPE_BIN" --input "generate:10" \
    --fake "id:random.number" --fake "content:name.fullName" \
    --drop "GenerateIndex" \
    --output "sqlite:Data Source=$DB_PATH" \
    --table "sensitive_data" \
    --max-retries 5 \
    --retry-delay-ms 1000 \
    --strategy Append

EXIT_CODE=$?
END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

wait $LOCKER_PID

if [ $EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}✓ DtPipe succeeded!${NC}"
    echo "Duration: $DURATION seconds (should be > 3s)"
    
    # Verify data count (10 from dtpipe + 1 from locker = 11)
    COUNT=$(sqlite3 "$DB_PATH" "SELECT COUNT(*) FROM sensitive_data;")
    if [ "$COUNT" -eq 11 ]; then
         echo -e "${GREEN}✓ Data count correct (11 rows)${NC}"
    else
         echo -e "${RED}✗ Data count mismatch. Expected 11, got $COUNT${NC}"
         exit 1
    fi
else
    echo -e "${RED}✗ DtPipe Failed with exit code $EXIT_CODE${NC}"
    exit 1
fi
