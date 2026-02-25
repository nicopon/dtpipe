#!/usr/bin/env bash
set -eo pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
ROOT_DIR="$DIR/../.."

echo "--- Benchmarking SQL Engines (5M rows + 2 JOINs) ---"

# Build all just in case
echo "Building binaries..."
bash "$ROOT_DIR/build_dtpolars.sh" > /dev/null
bash "$ROOT_DIR/build_dtfusion.sh" > /dev/null
bash "$ROOT_DIR/build_dtduck.sh" > /dev/null
bash "$ROOT_DIR/build_dtducksharp.sh" > /dev/null
dotnet build "$ROOT_DIR/src/DtPipe/DtPipe.csproj" -c Release --nologo > /dev/null

DTPOLARS_CMD="$ROOT_DIR/dist/dtpolars"
DTFUSION_CMD="$ROOT_DIR/dist/dtfusion"
DTDUCK_CMD="$ROOT_DIR/dist/dtduck"
DTDUCKSHARP_CMD="$ROOT_DIR/dist/dtducksharp"
DTPIPE_CMD="$ROOT_DIR/src/DtPipe/bin/Release/net10.0/dtpipe"

# Setup reference data 1 (10,000 rows)
REF_CSV="$ROOT_DIR/tests/scripts/benchmark_ref.csv"
echo "Id,Val" > "$REF_CSV"
for i in {1..10000}; do
    echo "$i,val_$i" >> "$REF_CSV"
done

# Setup reference data 2 (1,000 rows)
REF2_CSV="$ROOT_DIR/tests/scripts/benchmark_ref2.csv"
echo "Id,Desc" > "$REF2_CSV"
for i in {1..1000}; do
    echo "$i,desc_$i" >> "$REF2_CSV"
done

ROWS=1000000
PIPE_CMD="$DTPIPE_CMD -i generate:$ROWS --fake Email:internet.email --fake Name:name.fullname --fake Amount:finance.amount --fake Country:address.countrycode -o arrow:-"

echo ""
echo "======================================"
echo "          1. DtPolars ($ROWS rows)    "
echo "======================================"
QUERY_POLARS="SELECT COUNT(*) FROM stream s JOIN ref r ON s.GenerateIndex = r.Id JOIN ref2 r2 ON s.GenerateIndex = r2.Id"
/usr/bin/time -l "$DTPOLARS_CMD" \
  --in stream="proc:$PIPE_CMD" \
  --in ref="csv:$REF_CSV" \
  --in ref2="csv:$REF2_CSV" \
  --query "$QUERY_POLARS" \
  --out "csv:-" || true

echo ""
echo "======================================"
echo "          2. DtFusion ($ROWS rows)    "
echo "======================================"
# DataFusion query (Needs CAST for CSV columns)
QUERY_FUSION="SELECT COUNT(*) FROM stream s JOIN ref r ON s.GenerateIndex = CAST(r.Id AS BIGINT) JOIN ref2 r2 ON s.GenerateIndex = CAST(r2.Id AS BIGINT)"
/usr/bin/time -l "$DTFUSION_CMD" \
  --in stream="proc:$PIPE_CMD" \
  --in ref="csv:$REF_CSV" \
  --in ref2="csv:$REF2_CSV" \
  --query "$QUERY_FUSION" \
  --out "csv:-" || true

echo ""
echo "======================================"
echo "          3. DtDuck   ($ROWS rows)    "
echo "======================================"
# DuckDB query
QUERY_DUCK="SELECT COUNT(*) FROM stream s JOIN ref r ON s.GenerateIndex = r.Id JOIN ref2 r2 ON s.GenerateIndex = r2.Id"
/usr/bin/time -l "$DTDUCK_CMD" \
  --in stream="proc:$PIPE_CMD" \
  --in ref="csv:$REF_CSV" \
  --in ref2="csv:$REF2_CSV" \
  --query "$QUERY_DUCK" \
  --out "csv:-" || true

echo ""
echo "======================================"
echo "          4. DtDuckSharp ($ROWS rows) "
echo "======================================"
# DuckDBSharp query
/usr/bin/time -l "$DTDUCKSHARP_CMD" \
  --in stream="proc:$PIPE_CMD" \
  --in ref="csv:$REF_CSV" \
  --in ref2="csv:$REF2_CSV" \
  --query "$QUERY_DUCK" \
  --out "csv:-" || true

echo ""
echo "======================================"
echo "          5. DtPipe Native Join ($ROWS rows) "
echo "======================================"
# DtPipe XStreamer Native Join Pipeline
/usr/bin/time -l "$DTPIPE_CMD" \
  -i "generate:$ROWS" --fake Email:internet.email --fake Name:name.fullname --fake Amount:finance.amount --fake Country:address.countrycode --alias stream \
  -i "csv:$REF_CSV" --alias ref \
  -i "csv:$REF2_CSV" --alias ref2 \
  -x native-join --main stream --ref ref --on "GenerateIndex=Id" --select "Val" --alias j1 \
  -x native-join --main j1 --ref ref2 --on "GenerateIndex=Id" --select "Desc" \
  -o "null.sha256" --no-stats || true

echo ""
echo "======================================"
echo "          6. DtPipe DuckXStreamer ($ROWS rows) "
echo "======================================"
# DtPipe XStreamer Duck Join Pipeline
# Note: DuckXStreamer can handle multiple JOINs in one SQL query
/usr/bin/time -l "$DTPIPE_CMD" dag \
  -i "generate:$ROWS" --fake Email:internet.email --fake Name:name.fullname --fake Amount:finance.amount --fake Country:address.countrycode --alias stream \
  -i "csv:$REF_CSV" --alias ref \
  -i "csv:$REF2_CSV" --alias ref2 \
  -x duck-xstream --main stream --ref "ref,ref2" --query "SELECT stream.*, ref.Val, ref2.Desc FROM stream JOIN ref ON stream.GenerateIndex = ref.Id JOIN ref2 ON stream.GenerateIndex = ref2.Id" \
  -o "null.sha256" --no-stats || true

rm "$REF_CSV" "$REF2_CSV" "null.sha256" || true
echo ""
echo "Benchmark finished."
