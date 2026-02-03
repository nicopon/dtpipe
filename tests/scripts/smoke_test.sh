#!/bin/bash
set -e

# Configuration
DIST_DIR="./dist/release"
DTPIPE_BIN="$DIST_DIR/dtpipe"
PG_CONN="pg:Host=localhost;Port=5433;Username=postgres;Password=password;Database=postgres"
DB_CONTAINER="dtpipe-smoke-postgres"
ARTIFACTS_DIR="tests/scripts/artifacts"
mkdir -p "$ARTIFACTS_DIR"

# Check binary
if [ ! -f "$DTPIPE_BIN" ]; then
    echo "Error: Binary not found at $DTPIPE_BIN. Run ./build.sh first."
    exit 1
fi

echo "--- DtPipe Smoke Test ---"

# 1. Setup Environment
echo "1. Setting up Postgres..."
docker rm -f $DB_CONTAINER 2>/dev/null || true
docker run --name $DB_CONTAINER -e POSTGRES_PASSWORD=password -p 5433:5432 -d postgres:15-alpine > /dev/null
echo "   Waiting for Postgres..."
sleep 5 # Give it a moment to init

# 2. Generate Source Data
echo "2. Generating Source Data..."
echo "Id,Name,Email,JoinDate" > "$ARTIFACTS_DIR/smoke_source.csv"
echo "1,Alice,alice@example.com,2023-01-01" >> "$ARTIFACTS_DIR/smoke_source.csv"
echo "2,Bob,bob@example.com,2023-02-15" >> "$ARTIFACTS_DIR/smoke_source.csv"
echo "3,Charlie,charlie@example.com,2023-03-20" >> "$ARTIFACTS_DIR/smoke_source.csv"

# 3. Import CSV -> Postgres
echo "3. Importing CSV to Postgres..."
$DTPIPE_BIN -i "$ARTIFACTS_DIR/smoke_source.csv" -o "$PG_CONN" --pg-table "smoke_users" --pg-strategy Recreate > /dev/null
echo "   Import completed."

# 4. Export Postgres -> CSV (Verification)
echo "4. Exporting Postgres to CSV..."
# Note: Ordering by Id to match source order
$DTPIPE_BIN -i "$PG_CONN" -q "SELECT * FROM smoke_users ORDER BY Id" -o "$ARTIFACTS_DIR/smoke_verify.csv" > /dev/null
echo "   Export completed."

# 5. Verify Content
echo "5. Verifying Data Integrity..."

# Normalize files for comparison:
# 1. Remove BOM (if any)
# 2. Lowercase headers (since Postgres normalizes to lowercase)
# 3. Sort (already sorted by query but good practice)

# Function to normalize CSV
normalize() {
    # Remove BOM, convert to lowercase, remove carriage returns
    sed 's/^\xEF\xBB\xBF//' "$1" | tr '[:upper:]' '[:lower:]' | tr -d '\r'
}

normalize "$ARTIFACTS_DIR/smoke_source.csv" > "$ARTIFACTS_DIR/source_norm.csv"
normalize "$ARTIFACTS_DIR/smoke_verify.csv" > "$ARTIFACTS_DIR/verify_norm.csv"

if diff "$ARTIFACTS_DIR/source_norm.csv" "$ARTIFACTS_DIR/verify_norm.csv" > /dev/null; then
    echo "-----------------------------------"
    echo "✅ SMOKE TEST PASSED: Data matches!"
    echo "-----------------------------------"
else
    echo "❌ SMOKE TEST FAILED: Content mismatch"
    diff "$ARTIFACTS_DIR/source_norm.csv" "$ARTIFACTS_DIR/verify_norm.csv"
    exit 1
fi

# Cleanup
rm "$ARTIFACTS_DIR"/smoke_source.csv "$ARTIFACTS_DIR"/smoke_verify.csv "$ARTIFACTS_DIR"/source_norm.csv "$ARTIFACTS_DIR"/verify_norm.csv
docker rm -f $DB_CONTAINER > /dev/null
