#!/bin/bash
set -e

# Resolve Project Root and Paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
INFRA_DIR="$PROJECT_ROOT/tests/infra"
OUTPUT_DIR="$SCRIPT_DIR/output"

# Path to binary (Release build)
DTPIPE="$PROJECT_ROOT/dist/release/dtpipe"

echo "========================================"
echo "    DtPipe Integration Testing"
echo "========================================"

# Always Build Release
echo "🔨 Building Release..."
"$PROJECT_ROOT/build.sh" > /dev/null

# Check if binary exists (Double check)
if [ ! -f "$DTPIPE" ]; then
    echo "❌ Error: Build failed or binary not found at $DTPIPE"
    exit 1
fi

# Cleanup Function
cleanup() {
    echo ""
    echo "🧹 Cleaning up..."
    docker-compose -f "$INFRA_DIR/docker-compose.yml" down 2>/dev/null || true
    rm -f "$OUTPUT_DIR/ref.hash" "$OUTPUT_DIR/final.hash" "$OUTPUT_DIR/test.parquet" "$OUTPUT_DIR/reference.csv" reference.db
}
trap cleanup EXIT


echo "🐳 Starting Infrastructure (Postgres, MSSQL, Oracle)..."
docker-compose -f "$INFRA_DIR/docker-compose.yml" up -d

echo "⏳ Waiting 60s for Databases to Initialize..."
sleep 60

echo "----------------------------------------"
echo "Step 0: Generate Reference Source (CSV)"
echo "----------------------------------------"
# Sample -> CSV (Immutable Source)
$DTPIPE --input "sample:100;Id=int;Amount=double;Created=date" \
           --query "SELECT * FROM dummy" \
           --output "$OUTPUT_DIR/reference.csv"

echo "----------------------------------------"
echo "Step 0b: Generate Reference Checksum"
echo "----------------------------------------"
# CSV -> Checksum
$DTPIPE --input "csv:$OUTPUT_DIR/reference.csv" \
           --query "SELECT * FROM data" \
           --output "checksum:$OUTPUT_DIR/ref.hash"
REF_HASH=$(cat "$OUTPUT_DIR/ref.hash")
echo "Reference Hash: $REF_HASH"

echo "----------------------------------------"
echo "Step 1: CSV -> Postgres"
echo "----------------------------------------"
PG_CONN="pg:Host=localhost;Port=5440;Database=integration;Username=postgres;Password=password"
CMD1="$DTPIPE --input \"csv:$OUTPUT_DIR/reference.csv\" --query \"SELECT * FROM data\" --output \"$PG_CONN\""
echo "Running: $CMD1"
eval $CMD1

echo "----------------------------------------"
echo "Step 2: Postgres -> MSSQL"
echo "----------------------------------------"
MSSQL_CONN="mssql:Server=localhost,1434;Database=master;User Id=sa;Password=Password123!;TrustServerCertificate=True"
$DTPIPE --input "$PG_CONN" \
           --query "SELECT * FROM \"Export\"" \
           --output "$MSSQL_CONN" \
           --mssql-table "ExportedData"

echo "----------------------------------------"
echo "Step 3: MSSQL -> Oracle"
echo "----------------------------------------"
# Wait for Oracle to be really ready (Healthcheck is just a weak test)
echo "Waiting extra time for Oracle..."
sleep 20 

ORACLE_CONN="ora:Data Source=localhost:1522/FREEPDB1;User Id=testuser;Password=password;Pooling=false"
$DTPIPE --input "$MSSQL_CONN" \
           --query "SELECT * FROM ExportedData" \
           --output "$ORACLE_CONN" \
           --ora-table "EXPORT_DATA" \
           --ora-strategy "Truncate" # Ensure clean table

echo "----------------------------------------"
echo "Step 4: Oracle -> Parquet"
echo "----------------------------------------"
$DTPIPE --input "$ORACLE_CONN" \
           --query "SELECT * FROM EXPORT_DATA" \
           --output "$OUTPUT_DIR/test.parquet"

echo "----------------------------------------"
echo "Step 5: Parquet -> Checksum"
echo "----------------------------------------"
$DTPIPE --input "parquet:$OUTPUT_DIR/test.parquet" \
           --query "SELECT * FROM data" \
           --output "checksum:$OUTPUT_DIR/final.hash"

FINAL_HASH=$(cat "$OUTPUT_DIR/final.hash")
echo "Final Hash:     $FINAL_HASH"


echo "----------------------------------------"
echo "Comparison"
echo "----------------------------------------"

if [ "$REF_HASH" == "$FINAL_HASH" ]; then
    echo "✅ SUCCESS: Hashes match!"
    exit 0
else
    echo "❌ FAILURE: Hashes mismatch!"
    echo "Expected: $REF_HASH"
    echo "Actual:   $FINAL_HASH"
    exit 1
fi
