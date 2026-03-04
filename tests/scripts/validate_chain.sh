#!/bin/bash
set -e

# Resolve Project Root and Paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
INFRA_DIR="$PROJECT_ROOT/tests/infra"
ARTIFACTS_DIR="$SCRIPT_DIR/artifacts"
mkdir -p "$ARTIFACTS_DIR"

# Path to binary (Release build)
DTPIPE="$PROJECT_ROOT/dist/release/dtpipe"

echo "========================================"
echo "    DtPipe Integration Testing"
echo "========================================"

# Always Build Release
echo "🔨 Building Release..."
"$PROJECT_ROOT/build.sh" > /dev/null || { echo "❌ Build Failed!"; exit 1; }

# Check if binary exists
if [ ! -f "$DTPIPE" ]; then
    echo "❌ Error: Build failed or binary not found at $DTPIPE"
    exit 1
fi

# Cleanup Function
cleanup() {
    if [ $? -eq 0 ]; then
        echo ""
        echo "🧹 Cleaning up..."
        rm -f "$ARTIFACTS_DIR"/ref.hash "$ARTIFACTS_DIR"/final.hash "$ARTIFACTS_DIR"/test.parquet "$ARTIFACTS_DIR"/reference.csv "$ARTIFACTS_DIR"/final.csv
    else
        echo ""
        echo "⚠️ Failure detected. Keeping artifacts in $ARTIFACTS_DIR for debugging."
    fi
}
trap cleanup EXIT


echo "🐳 Starting Infrastructure..."
"$INFRA_DIR/start_infra.sh"

echo "🧹 Explicit Cleanup of Target Databases..."
docker exec dtpipe-integ-postgres psql -U postgres -d integration -c "DROP TABLE IF EXISTS test_table CASCADE;" || true
docker exec dtpipe-integ-mssql-tools /opt/mssql-tools/bin/sqlcmd -S dtpipe-integ-mssql -U sa -P 'Password123!' -Q "IF OBJECT_ID('ExportedData', 'U') IS NOT NULL DROP TABLE ExportedData;" || true

# Oracle explicit cleanup
docker exec -i dtpipe-integ-oracle sqlplus testuser/password@localhost:1521/FREEPDB1 <<EOF
BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE EXPORT_DATA';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;
/
EXIT;
EOF


echo "----------------------------------------"
echo "Step 0: Generate Reference Source CSV"
echo "----------------------------------------"
$DTPIPE --input "generate:100" \
           --fake "seq_id:{GenerateIndex}" \
           --fake "val_id:random.number" \
           --fake "val_price:commerce.price" \
           --fake "val_date:date.past" \
           --fake-seed 42 \
           --fake-deterministic \
           --drop "GenerateIndex" \
           --output "$ARTIFACTS_DIR/reference.csv"

echo "----------------------------------------"
echo "Step 0b: Generate Reference Checksum (via CSV normalization)"
echo "----------------------------------------"
# Use CAST to ensure numerical sort even if CSV reader treats it as string
$DTPIPE --input "csv:$ARTIFACTS_DIR/reference.csv" \
           --query 'SELECT seq_id, val_id, val_price, val_date FROM data ORDER BY CAST(seq_id AS BIGINT)' \
           --output "checksum:$ARTIFACTS_DIR/ref.hash"
REF_HASH=$(cat "$ARTIFACTS_DIR/ref.hash")
echo "Reference Hash: $REF_HASH"

echo "----------------------------------------"
echo "Step 1: CSV -> Postgres"
echo "----------------------------------------"
PG_CONN="pg:Host=localhost;Port=5440;Database=integration;Username=postgres;Password=password"
CMD1="$DTPIPE --input \"csv:$ARTIFACTS_DIR/reference.csv\" --output \"$PG_CONN\" --pg-table \"test_table\" --pg-strategy \"Recreate\""
echo "Running: $CMD1"
eval $CMD1

echo "----------------------------------------"
echo "Step 2: Postgres -> MSSQL"
echo "----------------------------------------"
MSSQL_CONN="mssql:Server=localhost,1434;Database=master;User Id=sa;Password=Password123!;TrustServerCertificate=True"
$DTPIPE --input "$PG_CONN" \
           --query 'SELECT seq_id, val_id, val_price, val_date FROM test_table ORDER BY seq_id' \
           --output "$MSSQL_CONN" \
           --mssql-table "ExportedData" \
           --mssql-strategy "Recreate"

# Step 3: Oracle
ORACLE_CONN="ora:Data Source=localhost:1522/FREEPDB1;User Id=testuser;Password=password;Pooling=false"
$DTPIPE --input "$MSSQL_CONN" \
           --query 'SELECT seq_id, val_id, val_price, val_date FROM ExportedData ORDER BY seq_id' \
           --output "$ORACLE_CONN" \
           --ora-table "EXPORT_DATA" \
           --ora-strategy "Recreate" 

echo "----------------------------------------"
echo "Step 4: Oracle -> Parquet"
echo "----------------------------------------"
# Note: Oracle might return uppercase names, we use aliases to keep it consistent
$DTPIPE --input "$ORACLE_CONN" \
           --query 'SELECT seq_id AS seq_id, val_id AS val_id, val_price AS val_price, val_date AS val_date FROM EXPORT_DATA ORDER BY seq_id' \
           --output "$ARTIFACTS_DIR/test.parquet"

echo "----------------------------------------"
echo "Step 5: Parquet -> CSV -> Normalized Comparison"
echo "----------------------------------------"
# Convert final parquet to CSV
$DTPIPE --input "parquet:$ARTIFACTS_DIR/test.parquet" \
           --output "csv:$ARTIFACTS_DIR/final.csv"

# Normalize BOTH by:
# 1. Removing header (tail -n +2)
# 2. Sorting numerically by first column (sort -n -t, -k1)
# 3. Removing any potential trailing whitespace/newlines issues
tail -n +2 "$ARTIFACTS_DIR/reference.csv" | sort -n -t, -k1 > "$ARTIFACTS_DIR/ref_sorted.csv"
tail -n +2 "$ARTIFACTS_DIR/final.csv" | sort -n -t, -k1 > "$ARTIFACTS_DIR/final_sorted.csv"

# Calculate hashes of SORTED DATA ONLY
$DTPIPE --input "csv:$ARTIFACTS_DIR/ref_sorted.csv" --no-stats --output "checksum:$ARTIFACTS_DIR/ref_sorted.hash"
$DTPIPE --input "csv:$ARTIFACTS_DIR/final_sorted.csv" --no-stats --output "checksum:$ARTIFACTS_DIR/final_sorted.hash"

REF_HASH=$(cat "$ARTIFACTS_DIR/ref_sorted.hash")
FINAL_HASH=$(cat "$ARTIFACTS_DIR/final_sorted.hash")

echo "Reference Hash (sorted data): $REF_HASH"
echo "Final Hash (sorted data):     $FINAL_HASH"

echo "----------------------------------------"
echo "Comparison"
echo "----------------------------------------"

if [ "$REF_HASH" == "$FINAL_HASH" ]; then
    echo "✅ SUCCESS: Hashes match!"
    exit 0
else
    echo "❌ FAILURE: Data mismatch!"
    echo "Expected Hash: $REF_HASH"
    echo "Actual Hash:   $FINAL_HASH"
    echo ""
    echo "Showing differences between ref_sorted.csv and final_sorted.csv (first 20 lines):"
    echo "------------------------------------------------------"
    diff <(head -n 20 "$ARTIFACTS_DIR/ref_sorted.csv") <(head -n 20 "$ARTIFACTS_DIR/final_sorted.csv") || true
    echo "------------------------------------------------------"
    exit 1
fi
