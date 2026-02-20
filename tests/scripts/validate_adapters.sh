#!/bin/bash
set -e

# validate_adapters.sh - Demonstrate and validate Arrow and JsonL adapters
# This script ensures that JsonL and Arrow adapters can read/write data correctly using the CLI.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR/../.."
ARTIFACTS_DIR="$SCRIPT_DIR/artifacts"

mkdir -p "$ARTIFACTS_DIR"

# Build the project
dotnet build "$PROJECT_ROOT/src/DtPipe/DtPipe.csproj" -c Release > /dev/null

DTPIPE="dotnet run --project $PROJECT_ROOT/src/DtPipe/DtPipe.csproj -c Release --"

# Idempotency: cleanup previous artifacts
rm -f "$ARTIFACTS_DIR/data.jsonl" "$ARTIFACTS_DIR/data.arrowstream" "$ARTIFACTS_DIR/data.arrow"

echo "1. Generating data and saving to JsonL..."
$DTPIPE -i "generate:10" -o "jsonl:$ARTIFACTS_DIR/data.jsonl"
echo "--- data.jsonl (first 2 lines) ---"
head -n 2 "$ARTIFACTS_DIR/data.jsonl"

echo -e "\n2. Reading from JsonL and saving to Arrow (Stream)..."
$DTPIPE -i "jsonl:$ARTIFACTS_DIR/data.jsonl" -o "arrow:$ARTIFACTS_DIR/data.arrowstream"
echo "Arrow Stream file created (data.arrowstream)"

echo -e "\n3. Reading from Arrow (Stream) and converting to Arrow (File format)..."
$DTPIPE -i "arrow:$ARTIFACTS_DIR/data.arrowstream" -o "arrow:$ARTIFACTS_DIR/data.arrow"
echo "Arrow File created (data.arrow)"

echo -e "\n4. Verifying Arrow File by reading it and showing first 5 rows as CSV..."
$DTPIPE -i "arrow:$ARTIFACTS_DIR/data.arrow" -o "csv" --limit 5

echo -e "\nValidation completed successfully!"
