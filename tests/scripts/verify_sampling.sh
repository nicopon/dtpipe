#!/bin/bash
set -e

# Setup
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
# Path to binary (Release build)
DTPIPE="$PROJECT_ROOT/dist/release/dtpipe"

source "$SCRIPT_DIR/common.sh" # Load helpers
OUTPUT_DIR="$SCRIPT_DIR/artifacts"
mkdir -p "$OUTPUT_DIR"

echo "========================================"
echo "    DtPipe Sampling Verification"
echo "========================================"

# Always build
"$PROJECT_ROOT/build.sh" > /dev/null

# Generate 100 rows with 10% sampling -> Expect ~10 rows
echo "Test 1: Sampling 10% of 100 rows (Sample Provider)"

OUTPUT_FILE="$OUTPUT_DIR/sampling_test.csv"

# Using run_via_yaml to ensure sampling works via YAML configuration as well
run_via_yaml --input "sample:100;Id=int;Name=string" \
             --query "SELECT * FROM data" \
             --output "csv:$OUTPUT_FILE" \
             --limit 100 \
             --sample-rate 0.1 \
             --sample-seed 12345

ROW_COUNT=$(wc -l < "$OUTPUT_FILE" | tr -d ' ')
# Remove header
ROW_COUNT=$((ROW_COUNT - 1))

echo "Rows gathered: $ROW_COUNT"

if [ "$ROW_COUNT" -gt 0 ] && [ "$ROW_COUNT" -lt 30 ]; then
    echo -e "${GREEN}✅ Sampling logic works (Got $ROW_COUNT rows, expected ~10)${NC}"
else
    echo -e "${RED}❌ Sampling logic failed (Got $ROW_COUNT rows)${NC}"
    exit 1
fi

# Clean up
rm -f "$OUTPUT_FILE"

echo -e "${GREEN}Sampling Verification Passed!${NC}"
