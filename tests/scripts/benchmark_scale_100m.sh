#!/usr/bin/env bash
set -eo pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
ROOT_DIR="$DIR/../.."
ARTIFACTS_DIR="$DIR/artifacts"

DTPOLARS_CMD="$ROOT_DIR/dist/dtpolars"
DTFUSION_CMD="$ROOT_DIR/dist/dtfusion"
DTDUCK_CMD="$ROOT_DIR/dist/dtduck"
DTPIPE_CMD="$ROOT_DIR/dist/release/dtpipe"
MAIN_PARQUET="$ARTIFACTS_DIR/main.parquet"

echo "--- Benchmarking SQL Engines (100M rows + 2 JOINs) ---"
echo "Dataset: $MAIN_PARQUET"
echo ""

# Setup reference data 1 (10,000 rows)
REF_CSV="$ARTIFACTS_DIR/ref1_100m.csv"
echo "Id,Val" > "$REF_CSV"
for i in {1..10000}; do echo "$i,val_$i"; done >> "$REF_CSV"

# Setup reference data 2 (1,000 rows)
REF2_CSV="$ARTIFACTS_DIR/ref2_100m.csv"
echo "Id,Desc" > "$REF2_CSV"
for i in {1..1000}; do echo "$i,desc_$i"; done >> "$REF2_CSV"

ROWS=100000000
QUERY="SELECT COUNT(*) FROM main m JOIN ref r ON m.GenerateIndex = r.Id JOIN ref2 r2 ON m.GenerateIndex = r2.Id"
QUERY_FUSION="SELECT COUNT(*) FROM main m JOIN ref r ON m.GenerateIndex = CAST(r.Id AS BIGINT) JOIN ref2 r2 ON m.GenerateIndex = CAST(r2.Id AS BIGINT)"

echo "======================================"
echo "  1. DtPolars ($ROWS rows)"
echo "======================================"
/usr/bin/time -l "$DTPOLARS_CMD" \
  --in main="parquet:$MAIN_PARQUET" \
  --in ref="csv:$REF_CSV" \
  --in ref2="csv:$REF2_CSV" \
  --query "$QUERY" \
  --out "csv:$ARTIFACTS_DIR/temp_result.csv" || true

echo ""
echo "======================================"
echo "  2. DtFusion ($ROWS rows)"
echo "======================================"
/usr/bin/time -l "$DTFUSION_CMD" \
  --in main="parquet:$MAIN_PARQUET" \
  --in ref="csv:$REF_CSV" \
  --in ref2="csv:$REF2_CSV" \
  --query "$QUERY_FUSION" \
  --out "csv:$ARTIFACTS_DIR/temp_result.csv" || true

echo ""
echo "======================================"
echo "  3. DtDuck ($ROWS rows)"
echo "======================================"
/usr/bin/time -l "$DTDUCK_CMD" \
  --in main="parquet:$MAIN_PARQUET" \
  --in ref="csv:$REF_CSV" \
  --in ref2="csv:$REF2_CSV" \
  --query "$QUERY" \
  --out "csv:$ARTIFACTS_DIR/temp_result.csv" || true

echo ""
echo "======================================"
echo "  4. DtPipe duck-engine ($ROWS rows)"
echo "======================================"
/usr/bin/time -l "$DTPIPE_CMD" \
  --input "generate:0" --alias main \
  --input "generate:0" --alias ref \
  --input "generate:0" --alias ref2 \
  --xstreamer duck-engine \
  --main main --ref ref --ref ref2 \
  --src-main "parquet:$MAIN_PARQUET" \
  --src-ref "csv:$REF_CSV" \
  --src-ref "csv:$REF2_CSV" \
  --query "$QUERY" \
  --output null --no-stats || true

echo ""
echo "Benchmark finished."
