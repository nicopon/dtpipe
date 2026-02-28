#!/usr/bin/env bash
# =============================================================================
# FAIR BENCHMARK: SQL Engines — 100M rows, 2 JOINs, Arrow IPC streams only
#
# Each engine receives its data via Arrow IPC stream (proc: mode) produced by
# a dtpipe subprocess. No native parquet/csv reading by the engines themselves.
# This measures the engine's SQL + Arrow ingestion performance, not I/O speed.
# =============================================================================
set -eo pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
ROOT_DIR="$DIR/../.."
ARTIFACTS_DIR="$DIR/artifacts"

DTPOLARS_CMD="$ROOT_DIR/dist/dtpolars"
DTFUSION_CMD="$ROOT_DIR/dist/dtfusion"
DTDUCK_CMD="$ROOT_DIR/dist/dtduck"
DTPIPE_CMD="$ROOT_DIR/src/DtPipe/bin/Release/net10.0/dtpipe"
MAIN_PARQUET="$ARTIFACTS_DIR/main.parquet"

# Reference CSVs (pre-created in benchmark_100m.sh)
REF_CSV="$ARTIFACTS_DIR/ref1_100m.csv"
REF2_CSV="$ARTIFACTS_DIR/ref2_100m.csv"

ROWS=100000000
QUERY='SELECT COUNT(*) FROM main m JOIN ref r ON m.GenerateIndex = r.Id JOIN ref2 r2 ON m.GenerateIndex = r2.Id'
QUERY_FUSION='SELECT COUNT(*) FROM main m JOIN ref r ON m."GenerateIndex" = CAST(r."Id" AS BIGINT) JOIN ref2 r2 ON m."GenerateIndex" = CAST(r2."Id" AS BIGINT)'

# Arrow IPC stream producers (each outputs Arrow IPC stream to stdout)
MAIN_PROC="$DTPIPE_CMD -i parquet:$MAIN_PARQUET -o arrow:- --no-stats"
REF_PROC="$DTPIPE_CMD -i csv:$REF_CSV -o arrow:- --no-stats"
REF2_PROC="$DTPIPE_CMD -i csv:$REF2_CSV -o arrow:- --no-stats"

echo "=============================================================="
echo "  FAIR BENCHMARK — Arrow IPC stream inputs (proc: mode)"
echo "  Dataset: $ROWS rows, 2 JOINs"
echo "  All engines receive Arrow IPC streams via subprocess (dtpipe)"
echo "=============================================================="
echo ""

# ── 0. REFERENCE: Engines with native readers (parquet/csv) ────────────────

echo "[ REFERENCE — Native I/O (no Arrow IPC overhead) ]"
echo ""

echo "=============================="
echo "  REF-A. DtDuck native"
echo "=============================="
/usr/bin/time -l "$DTDUCK_CMD" \
  --in main="parquet:$MAIN_PARQUET" \
  --in ref="csv:$REF_CSV" \
  --in ref2="csv:$REF2_CSV" \
  --query "$QUERY" --out "csv:-" 2>&1 | grep -v "^\s*0 " | head -20 || true

echo ""
echo "=============================="
echo "  REF-B. DtPolars native"
echo "=============================="
/usr/bin/time -l "$DTPOLARS_CMD" \
  --in main="parquet:$MAIN_PARQUET" \
  --in ref="csv:$REF_CSV" \
  --in ref2="csv:$REF2_CSV" \
  --query "$QUERY" --out "csv:-" 2>&1 | grep -v "^\s*0 " | head -20 || true

echo ""
echo "=============================="
echo "  REF-C. DtFusion native"
echo "=============================="
/usr/bin/time -l "$DTFUSION_CMD" \
  --in main="parquet:$MAIN_PARQUET" \
  --in ref="csv:$REF_CSV" \
  --in ref2="csv:$REF2_CSV" \
  --query "$QUERY_FUSION" --out "csv:-" 2>&1 | grep -v "^\s*0 " | head -20 || true

# ── 1. FAIR: Engines with Arrow IPC stream inputs (proc: mode) ─────────────

echo ""
echo "[ FAIR — Arrow IPC stream inputs (proc: mode) ]"
echo ""

echo "=============================="
echo "  1. DtDuck (proc: Arrow IPC)"
echo "=============================="
/usr/bin/time -l "$DTDUCK_CMD" \
  --in "main=proc:$MAIN_PROC" \
  --in "ref=proc:$REF_PROC" \
  --in "ref2=proc:$REF2_PROC" \
  --query "$QUERY" --out "csv:-" || true

echo ""
echo "=============================="
echo "  2. DtPolars (proc: Arrow IPC)"
echo "=============================="
/usr/bin/time -l "$DTPOLARS_CMD" \
  --in "main=proc:$MAIN_PROC" \
  --in "ref=proc:$REF_PROC" \
  --in "ref2=proc:$REF2_PROC" \
  --query "$QUERY" --out "csv:-" || true

echo ""
echo "=============================="
echo "  3. DtFusion (proc: Arrow IPC)"
echo "=============================="
/usr/bin/time -l "$DTFUSION_CMD" \
  --in "main=proc:$MAIN_PROC" \
  --in "ref=proc:$REF_PROC" \
  --in "ref2=proc:$REF2_PROC" \
  --query "$QUERY_FUSION" --out "csv:-" || true

echo ""
echo "=============================="
echo "  4. DtPipe duck-engine (sub-proc Arrow IPC, Piste D)"
echo "=============================="
/usr/bin/time -l "$DTPIPE_CMD" dag \
  -i "generate:0" --alias main \
  -i "generate:0" --alias ref \
  -i "generate:0" --alias ref2 \
  -x duck-engine \
  --main main --ref ref --ref ref2 \
  --src-main "parquet:$MAIN_PARQUET" \
  --src-ref "csv:$REF_CSV" \
  --src-ref "csv:$REF2_CSV" \
  --query "$QUERY" \
  -o "null:" --no-stats || true

echo ""
echo "=============================="
echo "  REF-D. DtPipe fusion-engine native (parquet + csv direct)"
echo "=============================="
/usr/bin/time -l "$DTPIPE_CMD" dag \
  -i "generate:0" --alias main \
  -i "generate:0" --alias ref \
  -i "generate:0" --alias ref2 \
  -x fusion-engine \
  --main main --ref ref --ref ref2 \
  --src-main "parquet:$MAIN_PARQUET" \
  --src-ref "csv:$REF_CSV" \
  --src-ref "csv:$REF2_CSV" \
  --query "$QUERY_FUSION" \
  -o "null:" --no-stats || true

echo ""
echo "=============================="
echo "  5. DtPipe fusion-engine FAIR (Arrow memory channel from DAG branches)"
echo "=============================="
/usr/bin/time -l "$DTPIPE_CMD" dag \
  -i "parquet:$MAIN_PARQUET" --alias main \
  -i "csv:$REF_CSV"          --alias ref \
  -i "csv:$REF2_CSV"         --alias ref2 \
  -x fusion-engine \
  --main main --ref ref --ref ref2 \
  --query "$QUERY_FUSION" \
  -o "null:" --no-stats || true


echo ""
echo "Fair benchmark finished."
