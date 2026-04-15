#!/bin/bash

# validate_xml.sh
# Validates the XML adapter with a massive file.

SCRIPT_DIR=$(dirname "$0")
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
GENERATOR="$SCRIPT_DIR/generate_massive_xml.sh"
ARTIFACTS_DIR="$SCRIPT_DIR/artifacts"
XML_FILE="${ARTIFACTS_DIR}/test_data_massive.xml"
CLEANUP=0

# Detect binary
if [ -f "$PROJECT_ROOT/dist/release/dtpipe" ]; then
    DTPIPE="$PROJECT_ROOT/dist/release/dtpipe"
else
    DTPIPE="/usr/local/share/dotnet/dotnet $PROJECT_ROOT/src/DtPipe/bin/Debug/net10.0/DtPipe.dll"
fi

echo "=== XML Adapter Mass Validation ==="

# 1. Ensure XML file exists
if [ ! -f "$XML_FILE" ]; then
    echo "XML test artifact missing. Generating local temporary copy..."
    XML_FILE="${ARTIFACTS_DIR}/massive_test_data_tmp.xml"
    mkdir -p "$ARTIFACTS_DIR"
    bash "$GENERATOR" "$XML_FILE" 2
    CLEANUP=1
fi

if [ ! -f "$XML_FILE" ]; then
    echo "Error: XML file was not found or generated."
    exit 1
fi

echo "Running functional validation (XML -> JSONL)..."
START_TIME=$(date +%s)

# Use dtpipe to count rows by outputting to jsonl and using wc
ROW_COUNT=$($DTPIPE -i xml:"$XML_FILE" --xml-path //Record -o jsonl:- | wc -l)

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

echo "Functional validation finished in ${DURATION}s."
echo "Total rows processed: $ROW_COUNT"

# Basic assertion: for 2GB we expect around 5 million records
EXPECTED_MIN=5000000
if [ "$ROW_COUNT" -lt "$EXPECTED_MIN" ]; then
    echo "Error: Row count ($ROW_COUNT) is lower than expected ($EXPECTED_MIN)."
    exit 1
fi

echo ""
echo "Running pure ingestion benchmark (XML -> Null)..."
$DTPIPE -i xml:"$XML_FILE" --xml-path //Record -o null:-

echo ""
echo "Verifying first record structure..."
FIRST_RECORD=$($DTPIPE -i xml:"$XML_FILE" --xml-path //Record -o jsonl:- --limit 1)
echo "$FIRST_RECORD"
if echo "$FIRST_RECORD" | grep -q "\"_id\""; then
    echo "✅ Attribute prefix verification passed (_id found)."
else
    echo "❌ Attribute prefix verification FAILED (expected _id, found: $FIRST_RECORD)"
    exit 1
fi

echo "Success: XML streaming adapter handled massive file efficiently."

# Cleanup only if it was a temporary local file
if [ "$CLEANUP" -eq 1 ]; then
    rm "$XML_FILE"
fi
