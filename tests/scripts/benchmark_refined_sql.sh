#!/usr/bin/env bash
set -eo pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
ROOT_DIR="$DIR/../.."
ARTIFACTS_DIR="$DIR/artifacts"

echo "--- Refined SQL Benchmarking (20M rows, Parquet Sources) ---"

DTPOLARS_CMD="$ROOT_DIR/dist/dtpolars"
DTFUSION_CMD="$ROOT_DIR/dist/dtfusion"
DTDUCK_CMD="$ROOT_DIR/dist/dtduck"
DTDUCKSHARP_CMD="$ROOT_DIR/dist/dtducksharp"
DTPIPE_CMD="$ROOT_DIR/src/DtPipe/bin/Release/net10.0/dtpipe"

MAIN_PQ="$ARTIFACTS_DIR/main.parquet"
REF1_PQ="$ARTIFACTS_DIR/ref1.parquet"
REF2_PQ="$ARTIFACTS_DIR/ref2.parquet"

QUERY="SELECT COUNT(*) FROM main m JOIN ref1 r1 ON (m.GenerateIndex % 10000 + 1) = r1.GenerateIndex JOIN ref2 r2 ON (m.GenerateIndex % 1000 + 1) = r2.GenerateIndex"

# 1. DtPolars
echo ""
echo "======================================"
echo "          1. DtPolars (Rust)          "
echo "======================================"
/usr/bin/time -l "$DTPOLARS_CMD" \
  --in main="proc:$DTPIPE_CMD -i parquet:$MAIN_PQ -o arrow:- --no-stats" \
  --in ref1="proc:$DTPIPE_CMD -i parquet:$REF1_PQ -o arrow:- --no-stats" \
  --in ref2="proc:$DTPIPE_CMD -i parquet:$REF2_PQ -o arrow:- --no-stats" \
  --query "$QUERY" \
  --out "csv:-" || true

# 2. DtFusion
echo ""
echo "======================================"
echo "          2. DtFusion (Rust)          "
echo "======================================"
/usr/bin/time -l "$DTFUSION_CMD" \
  --in main="proc:$DTPIPE_CMD -i parquet:$MAIN_PQ -o arrow:- --no-stats" \
  --in ref1="proc:$DTPIPE_CMD -i parquet:$REF1_PQ -o arrow:- --no-stats" \
  --in ref2="proc:$DTPIPE_CMD -i parquet:$REF2_PQ -o arrow:- --no-stats" \
  --query "$QUERY" \
  --out "csv:-" || true

# 3. DtDuck
echo ""
echo "======================================"
echo "          3. DtDuck (Rust)            "
echo "======================================"
/usr/bin/time -l "$DTDUCK_CMD" \
  --in main="proc:$DTPIPE_CMD -i parquet:$MAIN_PQ -o arrow:- --no-stats" \
  --in ref1="proc:$DTPIPE_CMD -i parquet:$REF1_PQ -o arrow:- --no-stats" \
  --in ref2="proc:$DTPIPE_CMD -i parquet:$REF2_PQ -o arrow:- --no-stats" \
  --query "$QUERY" \
  --out "csv:-" || true

# 4. DtDuckSharp (.NET)
echo ""
echo "======================================"
echo "          4. DtDuckSharp (.NET)       "
echo "======================================"
/usr/bin/time -l "$DTDUCKSHARP_CMD" \
  --in main="proc:$DTPIPE_CMD -i parquet:$MAIN_PQ -o arrow:- --no-stats" \
  --in ref1="proc:$DTPIPE_CMD -i parquet:$REF1_PQ -o arrow:- --no-stats" \
  --in ref2="proc:$DTPIPE_CMD -i parquet:$REF2_PQ -o arrow:- --no-stats" \
  --query "$QUERY" \
  --out "csv:-" || true

# 5. DuckXStreamer
echo ""
echo "======================================"
echo "          5. DuckXStreamer (DAG)      "
echo "======================================"
# DuckXStreamer is a DAG that reads Parquet files and joins them.
# We use -i parquet:PATH --alias X to feed the XStreamer.
/usr/bin/time -l "$DTPIPE_CMD" dag \
  -i "parquet:$MAIN_PQ" --alias main \
  -i "parquet:$REF1_PQ" --alias ref1 \
  -i "parquet:$REF2_PQ" --alias ref2 \
  -x duck-xstream --main main --ref "ref1,ref2" --query "$QUERY" \
  -o "null:-" || true

echo ""
echo "Benchmark finished."
