#!/usr/bin/env bash
set -eo pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
ROOT_DIR="$DIR/../.."
DTPIPE_CMD="$ROOT_DIR/src/DtPipe/bin/Release/net10.0/dtpipe"
ARTIFACTS_DIR="$DIR/artifacts"
mkdir -p "$ARTIFACTS_DIR"

OUTPUT_REPORT="$DIR/benchmark_50m_report.txt"
echo "=== DtPipe High-Volume Columnar Benchmark (50M Rows) ===" > "$OUTPUT_REPORT"
echo "Date: $(date)" >> "$OUTPUT_REPORT"
echo "OS: $(uname -a)" >> "$OUTPUT_REPORT"
echo "" >> "$OUTPUT_REPORT"

ROWS=50000000
FAKE_OPTS="--fake Email:internet.email --fake Name:name.fullname --fake Amount:finance.amount --fake Country:address.countrycode --fake-deterministic --fake-seed 42"

echo "Step 1: Baseline (Generate 10M -> Fake -> Null)"
echo "-----------------------------------------------" | tee -a "$OUTPUT_REPORT"
/usr/bin/time -l "$DTPIPE_CMD" --input "generate:10000000" $FAKE_OPTS --output "null:" --no-stats --batch-size 65536 2>&1 | tee -a "$OUTPUT_REPORT"

echo "" | tee -a "$OUTPUT_REPORT"
echo "Step 2: Parquet Persistence (Generate 50M -> Fake -> Parquet)"
echo "------------------------------------------------------------" | tee -a "$OUTPUT_REPORT"
PARQUET_FILE="$ARTIFACTS_DIR/bench_50m.parquet"
rm -f "$PARQUET_FILE"
/usr/bin/time -l "$DTPIPE_CMD" --input "generate:$ROWS" $FAKE_OPTS --output "parquet:$PARQUET_FILE" --no-stats --batch-size 65536 2>&1 | tee -a "$OUTPUT_REPORT"
echo "Parquet File Size: $(du -sh "$PARQUET_FILE")" >> "$OUTPUT_REPORT"

echo "" | tee -a "$OUTPUT_REPORT"
echo "Step 3: DuckDB Persistence (Generate 50M -> Fake -> DuckDB)"
echo "-----------------------------------------------------------" | tee -a "$OUTPUT_REPORT"
DUCKDB_FILE="$ARTIFACTS_DIR/bench_50m.duckdb"
rm -f "$DUCKDB_FILE"
/usr/bin/time -l "$DTPIPE_CMD" --input "generate:$ROWS" $FAKE_OPTS --output "duck:$DUCKDB_FILE" --table Benchmark50M --strategy Recreate --no-stats --batch-size 65536 2>&1 | tee -a "$OUTPUT_REPORT"
echo "DuckDB File Size: $(du -sh "$DUCKDB_FILE")" >> "$OUTPUT_REPORT"

echo "" | tee -a "$OUTPUT_REPORT"
echo "Step 4: Verification (Read back from DuckDB)"
echo "--------------------------------------------" | tee -a "$OUTPUT_REPORT"
"$DTPIPE_CMD" --input "duck:$DUCKDB_FILE" --query "SELECT COUNT(*), AVG(Amount) FROM Benchmark50M" --output "null:" --no-stats 2>&1 | tee -a "$OUTPUT_REPORT"

echo "" >> "$OUTPUT_REPORT"
echo "Benchmark completed." >> "$OUTPUT_REPORT"
echo "Report saved to $OUTPUT_REPORT"
