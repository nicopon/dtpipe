#!/bin/bash
set -e

# Path to executables
QUERYDUMP="./dist/release/querydump"

echo "========================================"
echo "    QueryDump Sampling Verification"
echo "========================================"

# Generate 100 rows with 10% sampling -> Expect ~10 rows
echo "Test 1: Sampling 10% of 100 rows (Sample Provider)"
"$QUERYDUMP" --input "sample:100;Id=int;Name=string" \
             --query "SELECT * FROM data" \
             --output "csv:dist/sampling_test.csv" \
             --limit 100 \
             --sample-rate 0.1 \
             --sample-seed 12345

ROW_COUNT=$(wc -l < dist/sampling_test.csv | tr -d ' ')
# Remove header
ROW_COUNT=$((ROW_COUNT - 1))

echo "Rows gathered: $ROW_COUNT"

if [ "$ROW_COUNT" -gt 0 ] && [ "$ROW_COUNT" -lt 30 ]; then
    echo "✅ Sampling logic works (Got $ROW_COUNT rows, expected ~10)"
else
    echo "❌ Sampling logic failed (Got $ROW_COUNT rows)"
    exit 1
fi

# Clean up
rm dist/sampling_test.csv

echo "Sampling Verification Passed!"
