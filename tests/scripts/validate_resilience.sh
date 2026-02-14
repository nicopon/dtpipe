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
echo "    DtPipe Resilience Validation"
echo "========================================"

# Cleanup
rm -f "$ARTIFACTS_DIR/resilience.yaml"

# 1. Export job with retry options to YAML
echo "Test 1: Exporting retry options to YAML..."
$DTPIPE_BIN --input "generate:10" \
               --query "SELECT * FROM data" \
               --output "csv:$ARTIFACTS_DIR/res_out.csv" \
               --max-retries 12 \
               --retry-delay-ms 450 \
               --export-job "$ARTIFACTS_DIR/resilience.yaml"

# 2. Verify YAML Content
if grep -iq "max-retries: 12" "$ARTIFACTS_DIR/resilience.yaml" && \
   grep -iq "retry-delay-ms: 450" "$ARTIFACTS_DIR/resilience.yaml"; then
    echo -e "${GREEN}✓ Resilience YAML Persistence Passed${NC}"
else
    echo -e "${RED}✗ Resilience YAML Persistence Failed${NC}"
    cat "$ARTIFACTS_DIR/resilience.yaml"
    exit 1
fi

# 3. Run from YAML
echo "Test 2: Running from YAML..."
$DTPIPE_BIN --job "$ARTIFACTS_DIR/resilience.yaml"

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Resilience Execution from YAML Passed${NC}"
else
    echo -e "${RED}✗ Resilience Execution from YAML Failed${NC}"
    exit 1
fi

exit 0
