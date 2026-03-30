#!/bin/bash

# bench.sh
# Performance benchmarks for DtPipe.
# Usage:
#   ./bench.sh                  # standard pipeline benchmarks
#   ./bench.sh --sql            # SQL JOIN benchmarks (DataFusion, requires datasets)
#   ./bench.sh --all            # all benchmarks

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
ARTIFACTS_DIR="$SCRIPT_DIR/artifacts"
mkdir -p "$ARTIFACTS_DIR"

DTPIPE="$ROOT_DIR/dist/release/dtpipe"
export DTPIPE_NO_TUI=1

if [ ! -f "$DTPIPE" ]; then
    echo "Error: $DTPIPE not found. Run ./build.sh first."
    exit 1
fi

RUN_SQL=0
for arg in "$@"; do
    case "$arg" in
        --sql) RUN_SQL=1 ;;
        --all) RUN_SQL=1 ;;
    esac
done

timeit() {
    local label="$1"
    shift
    echo "  [$label] Running..."
    local start end duration ec
    start=$(date +%s%N)
    "$@"
    ec=$?
    end=$(date +%s%N)
    duration=$(( (end - start) / 1000000 ))
    if [ $ec -eq 0 ]; then
        echo "  [$label] Done in ${duration}ms"
    else
        echo "  [$label] FAILED (exit code $ec) in ${duration}ms" >&2
    fi
    return $ec
}

echo "======================================"
echo "  DtPipe Benchmark Suite"
echo "======================================"

# ----------------------------------------
# 1. Linear pipeline: generate → CSV
# ----------------------------------------
echo ""
echo "[1] Linear pipeline: generate:100k → CSV"
timeit "generate:100k→csv" "$DTPIPE" \
  -i "generate:100000" \
  --fake "Id:random.uuid" \
  --fake "Name:name.fullName" \
  --fake "Email:internet.email" \
  --drop "GenerateIndex" \
  -o "$ARTIFACTS_DIR/bench_linear.csv" --no-stats
ROWS=$(wc -l < "$ARTIFACTS_DIR/bench_linear.csv" | tr -d ' ')
echo "  Rows: $ROWS"

# ----------------------------------------
# 2. CSV → Parquet
# ----------------------------------------
echo ""
echo "[2] CSV → Parquet (100k rows)"
timeit "csv→parquet" "$DTPIPE" \
  -i "$ARTIFACTS_DIR/bench_linear.csv" \
  -o "$ARTIFACTS_DIR/bench_linear.parquet" --no-stats

# ----------------------------------------
# 3. Parquet → CSV with transformations
# ----------------------------------------
echo ""
echo "[3] Parquet → CSV with fake+overwrite+format (100k rows)"
timeit "parquet→csv+transforms" "$DTPIPE" \
  -i "$ARTIFACTS_DIR/bench_linear.parquet" \
  --fake "Name:name.firstName" \
  --overwrite "Email:anonymized@example.com" \
  --format "Display:{Name} <{Email}>" \
  -o "$ARTIFACTS_DIR/bench_transformed.csv" --no-stats

# ----------------------------------------
# 4. DuckDB integration (if available)
# ----------------------------------------
echo ""
echo "[4] generate:1M → DuckDB → Parquet"
DUCKDB_PATH="$ARTIFACTS_DIR/bench.duckdb"
rm -f "$DUCKDB_PATH"

timeit "generate:1M→duckdb" "$DTPIPE" \
  -i "generate:1000000" \
  --fake "Id:random.uuid" \
  --fake "Val:random.number" \
  --drop "GenerateIndex" \
  -o "duck:$DUCKDB_PATH" --table "bench_data" --strategy Recreate --no-stats

timeit "duckdb→parquet" "$DTPIPE" \
  -i "duck:$DUCKDB_PATH" \
  --query "SELECT * FROM bench_data" \
  -o "$ARTIFACTS_DIR/bench_from_duckdb.parquet" --no-stats

ROWS=$(python3 -c "import struct,sys; f=open('$ARTIFACTS_DIR/bench_from_duckdb.parquet','rb'); f.seek(-8,2); print(struct.unpack('<q',f.read(4))[0])" 2>/dev/null || echo "?")
echo "  Parquet rows: $ROWS"

# ----------------------------------------
# 5. SQL JOIN benchmarks (DataFusion)
# ----------------------------------------
if [ $RUN_SQL -eq 1 ]; then
    echo ""
    echo "[5] SQL JOIN benchmark (dual engine: DataFusion vs DuckDB)"

    MAIN_PARQUET="$ARTIFACTS_DIR/main.parquet"
    REF_CSV="$ARTIFACTS_DIR/ref1_10k.csv"
    REF2_CSV="$ARTIFACTS_DIR/ref2_10k.csv"

    if [ ! -f "$MAIN_PARQUET" ] || [ ! -f "$REF_CSV" ] || [ ! -f "$REF2_CSV" ]; then
        echo "  Generating benchmark datasets..."
        "$SCRIPT_DIR/generate_benchmark_datasets.sh"
    fi

    QUERY_FUSION='SELECT COUNT(*) FROM main m JOIN ref r ON m.GenerateIndex = CAST(r.Id AS BIGINT) JOIN ref2 r2 ON m.GenerateIndex = CAST(r2.Id AS BIGINT)'

    DTPIPE_DIR="$(dirname "$DTPIPE")"
    DATAFUSION_AVAILABLE=0
    if [ -f "$DTPIPE_DIR/libdtpipe_datafusion.dylib" ] || \
       [ -f "$DTPIPE_DIR/libdtpipe_datafusion.so" ] || \
       [ -f "$DTPIPE_DIR/dtpipe_datafusion.dll" ]; then
        DATAFUSION_AVAILABLE=1
    fi

    if [ $DATAFUSION_AVAILABLE -eq 1 ]; then
        echo "  DataFusion engine..."
        timeit "datafusion-dag" "$DTPIPE" \
          -i "parquet:$MAIN_PARQUET" --alias main \
          -i "csv:$REF_CSV"          --alias ref \
          -i "csv:$REF2_CSV"         --alias ref2 \
          --from main --ref ref --ref ref2 \
          --sql "$QUERY_FUSION" --sql-engine datafusion \
          -o null --no-stats
    else
        echo "  DataFusion engine: SKIPPED (native lib not found — run ./build_datafusion_bridge.sh first)"
    fi

    echo "  DuckDB engine..."
    timeit "duckdb-dag" "$DTPIPE" \
      -i "parquet:$MAIN_PARQUET" --alias main \
      -i "csv:$REF_CSV"          --alias ref \
      -i "csv:$REF2_CSV"         --alias ref2 \
      --from main --ref ref --ref ref2 \
      --sql "$QUERY_FUSION" --sql-engine duckdb \
      -o null --no-stats
      
fi

# ----------------------------------------
# Cleanup temp files
# ----------------------------------------
rm -f "$ARTIFACTS_DIR/bench_linear.csv" \
      "$ARTIFACTS_DIR/bench_linear.parquet" \
      "$ARTIFACTS_DIR/bench_transformed.csv" \
      "$ARTIFACTS_DIR/bench_from_duckdb.parquet" \
      "$ARTIFACTS_DIR/bench.duckdb"

echo ""
echo "======================================"
echo "  Benchmark complete."
echo "======================================"
