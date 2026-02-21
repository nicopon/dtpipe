#!/usr/bin/env bash
set -euo pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
ROOT_DIR="$DIR/../.."

echo "--- Validating DtPolars Orchestration ---"

# Step 1: Ensure dtpipe is built
echo "1. Building dtpipe..."
dotnet build "$ROOT_DIR/src/DtPipe/DtPipe.csproj" -f net10.0 -c Debug --nologo

# Step 2: Ensure DtPolars is built
echo "2. Building DtPolars..."
bash "$ROOT_DIR/build_dtpolars.sh"

# Step 3: Setup test data
TEST_CSV="$ROOT_DIR/tests/scripts/dtpolars_ref.csv"
echo "Id,Name,Category" > "$TEST_CSV"
echo "1,Alice,A" >> "$TEST_CSV"
echo "2,Bob,B" >> "$TEST_CSV"
echo "5,Eve,A" >> "$TEST_CSV"

# Step 4: Run Orchestration
# We will use dtpipe to generate 5 rows (0, 1, 2, 3, 4) and join with the CSV.
# The expected join match on GenerateIndex == Id will be Id 1 and 2.
echo "3. Executing DtPolars DtPipe Join..."

DTPOLARS_CMD="$ROOT_DIR/dist/dtpolars"
DTPIPE_CMD="$ROOT_DIR/src/DtPipe/bin/Debug/net10.0/dtpipe"

"$DTPOLARS_CMD" \
  --in ref="csv:$TEST_CSV" \
  --in pipe_stream="proc:$DTPIPE_CMD -i generate:5 -o arrow:-" \
  --query "SELECT p.GenerateIndex, r.Name, r.Category FROM pipe_stream p JOIN ref r ON p.GenerateIndex = r.Id" \
  --out "csv:$ROOT_DIR/tests/scripts/dtpolars_out.csv"

# Step 5: Verify Output
echo "4. Verifying Output..."
cat "$ROOT_DIR/tests/scripts/dtpolars_out.csv" | grep "Alice" > /dev/null
cat "$ROOT_DIR/tests/scripts/dtpolars_out.csv" | grep "Bob" > /dev/null

if cat "$ROOT_DIR/tests/scripts/dtpolars_out.csv" | grep "Eve"; then
    echo "❌ Error: Eve should not be in the output!"
    exit 1
fi

rm "$TEST_CSV"
rm "$ROOT_DIR/tests/scripts/dtpolars_out.csv"

echo "✅ DtPolars Orchestration test passed!"
