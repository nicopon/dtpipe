#!/bin/bash

# Configuration
SCRIPT_DIR="$(dirname "$0")"
PROJECT_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"
DIST_DIR="$PROJECT_ROOT/dist/release"
DTPIPE_BIN="$DIST_DIR/dtpipe"
ARTIFACTS_DIR="tests/scripts/artifacts"
mkdir -p "$ARTIFACTS_DIR"

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

echo "========================================"
echo "    DtPipe Metrics Validation"
echo "========================================"

# Cleanup
rm -f "$ARTIFACTS_DIR/metrics.json" "$ARTIFACTS_DIR/metrics_out.csv"

# 1. Run export with metrics enabled
echo "Test 1: Exporting with --metrics-path..."
$DTPIPE_BIN --input "generate:100" \
               --query "SELECT * FROM data" \
               --output "csv:$ARTIFACTS_DIR/metrics_out.csv" \
               --metrics-path "$ARTIFACTS_DIR/metrics.json" \
               --fake

# 2. Verify Metrics File Exists
if [ ! -f "$ARTIFACTS_DIR/metrics.json" ]; then
    echo -e "${RED}✗ Metrics file fail: metrics.json not created${NC}"
    exit 1
fi

# 3. Verify Metrics Content (Basic check via grep)
echo "Verifying metrics content..."
if grep -q "\"ReadCount\": 100" "$ARTIFACTS_DIR/metrics.json" && \
   grep -q "\"WriteCount\": 100" "$ARTIFACTS_DIR/metrics.json"; then
    echo -e "${GREEN}✓ Metrics Validation Passed${NC}"
else
    echo -e "${RED}✗ Metrics Validation Failed: Incorrect content in metrics.json${NC}"
    cat "$ARTIFACTS_DIR/metrics.json"
    exit 1
fi

exit 0
