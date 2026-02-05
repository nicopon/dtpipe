#!/bin/bash
set -e

# Find project root
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$( cd "$SCRIPT_DIR/../.." &> /dev/null && pwd )"
ARTIFACTS_DIR="/Users/PonsartNi/.gemini/antigravity/brain/66bdd720-b9da-4a69-9644-72c7513823fd"

cd "$ROOT_DIR"

# Setup Paths
SRC_DB="$ARTIFACTS_DIR/hooks_source.db"
TGT_DB="$ARTIFACTS_DIR/hooks_target.db"
PRE_EXEC_SQL="$ARTIFACTS_DIR/pre_exec.sql"
EXE="./src/DtPipe/bin/Debug/net10.0/DtPipe"

rm -f "$SRC_DB" "$TGT_DB" "$PRE_EXEC_SQL"

# Create Source Table
sqlite3 "$SRC_DB" "CREATE TABLE source_table (id INTEGER PRIMARY KEY, name TEXT);"
sqlite3 "$SRC_DB" "INSERT INTO source_table VALUES (1, 'Alice');"
sqlite3 "$SRC_DB" "INSERT INTO source_table VALUES (2, 'Bob');"

# Create Log Table in Target
sqlite3 "$TGT_DB" "CREATE TABLE hook_logs (message TEXT, created_at DATETIME DEFAULT CURRENT_TIMESTAMP);"

echo "Starting Hook Verification (Output to Artifacts)..."

# Create Hook File
echo "INSERT INTO hook_logs (message) VALUES ('Pre-Exec File Run');" > "$PRE_EXEC_SQL"

# Run DtPipe with Hooks
$EXE \
    --input "sqlite:Data Source=$SRC_DB" \
    --output "sqlite:Data Source=$TGT_DB;Cache=Shared" \
    --query "SELECT * FROM source_table" \
    --limit 2 \
    --sqlite-table "target_table" \
    --sqlite-strategy "Recreate" \
    --pre-exec "$PRE_EXEC_SQL" \
    --post-exec "INSERT INTO hook_logs (message) VALUES ('Post-Exec Run')" \
    --finally-exec "INSERT INTO hook_logs (message) VALUES ('Finally-Exec Run')"

echo "Job Completed."

# Verify Logs in Target
echo "Verifying Logs from $TGT_DB..."
LOGS=$(sqlite3 "$TGT_DB" "SELECT message FROM hook_logs ORDER BY created_at;")

echo "Logs Found:"
echo "$LOGS"

if [[ "$LOGS" == *"Pre-Exec File Run"* ]] && [[ "$LOGS" == *"Post-Exec Run"* ]] && [[ "$LOGS" == *"Finally-Exec Run"* ]]; then
    echo "SUCCESS: All hooks executed."
else
    echo "FAILURE: Missing hooks."
    exit 1
fi
