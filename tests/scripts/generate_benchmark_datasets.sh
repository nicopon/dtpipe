#!/usr/bin/env bash
set -eo pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
ROOT_DIR="$DIR/../.."
ARTIFACTS_DIR="$DIR/artifacts"
DTPIPE_CMD="$ROOT_DIR/dist/release/dtpipe"

mkdir -p "$ARTIFACTS_DIR"

echo "--- Generating 1M rows benchmark dataset (Parquet) ---"

rm -f "$ARTIFACTS_DIR/main.parquet"
rm -f "$ARTIFACTS_DIR/ref1_10m.csv"
rm -f "$ARTIFACTS_DIR/ref2_10m.csv"

# 1. Generate main dataset (100M rows)
echo "Generating main.parquet (100,000,000 rows)..."
"$DTPIPE_CMD" -i generate:100000000 \
  --fake Email:Internet.Email \
  --fake Name:Name.FullName \
  --fake Amount:Finance.Amount \
  --fake Country:Address.CountryCode \
  -o "parquet:$ARTIFACTS_DIR/main.parquet" --no-stats

# 2. Reference 1 (10,000 rows)
echo "Generating ref1_10m.csv (10,000 rows)..."
"$DTPIPE_CMD" -i generate:10000 --compute "Id:long:row.GenerateIndex" -o "csv:$ARTIFACTS_DIR/ref1_10m.csv" --no-stats

# 3. Reference 2 (10,000 rows)
echo "Generating ref2_10m.csv (10,000 rows)..."
"$DTPIPE_CMD" -i generate:10000 --compute "Id:long:row.GenerateIndex" -o "csv:$ARTIFACTS_DIR/ref2_10m.csv" --no-stats

echo "Dataset generation finished."
ls -lh "$ARTIFACTS_DIR"
