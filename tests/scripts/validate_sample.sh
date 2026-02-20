#!/bin/bash
# Validation script for the DtPipe programmatic sample

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR/../.."

cd "$PROJECT_ROOT"

echo "------------------------------------------------------"
echo "Validating DtPipe.Sample Project"
echo "------------------------------------------------------"

# Build the project
echo "Building DtPipe.Sample..."
dotnet build src/DtPipe.Sample/DtPipe.Sample.csproj -c Debug --nologo

# Run the project
echo "Running DtPipe.Sample..."
OUTPUT=$(dotnet run --project src/DtPipe.Sample/DtPipe.Sample.csproj -c Debug --no-build)

# Assertions
echo "$OUTPUT"

echo "Checking output for Scenario 1 (Full Pipeline)..."
if ! echo "$OUTPUT" | grep -q "Scenario 1: Full Pipeline Engine"; then
    echo "ERROR: Scenario 1 not found in output."
    exit 1
fi
if ! echo "$OUTPUT" | grep -q "Transferred 5 rows"; then
    echo "ERROR: Scenario 1 did not transfer 5 rows."
    exit 1
fi

echo "Checking output for Scenario 2 (Reader Only)..."
if ! echo "$OUTPUT" | grep -q "Scenario 2: Reader Only (Manual Consumption)"; then
    echo "ERROR: Scenario 2 not found in output."
    exit 1
fi
if ! echo "$OUTPUT" | grep -q "Read 3 rows"; then
    echo "ERROR: Scenario 2 did not read 3 rows."
    exit 1
fi

echo "Checking output for Scenario 3 (Writer Only)..."
if ! echo "$OUTPUT" | grep -q "Scenario 3: Writer Only (Manual Push)"; then
    echo "ERROR: Scenario 3 not found in output."
    exit 1
fi
# The writer should push the rows to stdout (CSV format)
if ! echo "$OUTPUT" | grep -q "101,Alice"; then
    echo "ERROR: Scenario 3 output '101,Alice' not found in stream."
    exit 1
fi
if ! echo "$OUTPUT" | grep -q "103,Charlie"; then
    echo "ERROR: Scenario 3 output '103,Charlie' not found in stream."
    exit 1
fi

echo "Checking output for Scenario 4 (DataFrame)..."
if ! echo "$OUTPUT" | grep -q "Scenario 4: DataFrame to Writer"; then
    echo "ERROR: Scenario 4 not found in output."
    exit 1
fi
if ! echo "$OUTPUT" | grep -q "2,b@test.com,false"; then
    echo "ERROR: Scenario 4 DataFrame row '2,b@test.com,false' not found in stream."
    exit 1
fi

echo "Checking output for Scenario 5 (Custom Transformer)..."
if ! echo "$OUTPUT" | grep -q "Scenario 5: Custom C# Transformer"; then
    echo "ERROR: Scenario 5 not found in output."
    exit 1
fi
if ! echo "$OUTPUT" | grep -q "Hello 1"; then
    echo "ERROR: Scenario 5 Transformer row 'Hello 1' not found in stream."
    exit 1
fi

echo "Checking output for Scenario 6 (LINQ Generator)..."
if ! echo "$OUTPUT" | grep -q "Scenario 6: Pure LINQ Object Generator"; then
    echo "ERROR: Scenario 6 not found in output."
    exit 1
fi
if ! echo "$OUTPUT" | grep -q "4;Product_4;43.96"; then
    echo "ERROR: Scenario 6 LINQ row '4;Product_4;43.96' not found in stream."
    exit 1
fi

echo ""
echo "✅ DtPipe.Sample validated successfully!"
exit 0
