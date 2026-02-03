#!/bin/bash
set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
DTPIPE="$PROJECT_ROOT/dist/release/dtpipe"
OUTPUT_DIR="$SCRIPT_DIR/artifacts"

mkdir -p "$OUTPUT_DIR"

if [ ! -f "$DTPIPE" ]; then
    echo "Building..."
    "$PROJECT_ROOT/build.sh"
fi

echo "Generating data (CSV)..."
"$DTPIPE" --input "sample:100;Id=int;Name=string;Amount=double;Secret=string" \
            --output "$OUTPUT_DIR/ref.csv"

echo "Running failing step (Iterative - CSV)..."

for i in {1..5}; do
    echo "Run #$i"
    "$DTPIPE" --input "csv:$OUTPUT_DIR/ref.csv" \
              --output "$OUTPUT_DIR/out_$i.csv" \
              --drop "Name" \
              --project "Id, Secret"
done

echo "Done."
