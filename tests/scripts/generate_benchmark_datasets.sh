#!/usr/bin/env bash
set -eo pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
ROOT_DIR="$DIR/../.."
ARTIFACTS_DIR="$DIR/artifacts"
DTPIPE_CMD="$ROOT_DIR/dist/release/dtpipe"

mkdir -p "$ARTIFACTS_DIR"

echo "--- Generating 1M rows benchmark dataset (Parquet) ---"

# 1. Main Stream (1M rows)
# We generate a large dataset with IDs and some dummy data
echo "Generating main.parquet (1,000,000 rows)..."
"$DTPIPE_CMD" -i "generate:1000000" \
  --fake Email:internet.email \
  --fake Name:name.fullname \
  --fake Amount:finance.amount \
  --fake Country:address.countrycode \
  -o "parquet:$ARTIFACTS_DIR/main.parquet" --no-stats

# 2. Reference 1 (10,000 rows)
echo "Generating ref1.parquet (10,000 rows)..."
"$DTPIPE_CMD" -i "generate:10000" \
  --fake Val:name.fullname \
  -o "parquet:$ARTIFACTS_DIR/ref1.parquet" --no-stats

# 3. Reference 2 (10,000 rows)
echo "Generating ref2.parquet (10,000 rows)..."
"$DTPIPE_CMD" -i "generate:10000" \
  --fake Desc:commerce.productName \
  -o "parquet:$ARTIFACTS_DIR/ref2.parquet" --no-stats

echo "Dataset generation finished."
ls -lh "$ARTIFACTS_DIR"
