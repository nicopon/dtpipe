#!/usr/bin/env bash
set -euo pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
ROOT_DIR="$DIR/../.."

echo "--- Validating DtFusion Orchestration ---"

# Step 1: Ensure dtpipe is built
echo "1. Building dtpipe..."
dotnet build "$ROOT_DIR/src/DtPipe/DtPipe.csproj" -f net10.0 -c Debug --nologo

# Step 2: Ensure DtFusion is built
echo "2. Building DtFusion..."
bash "$ROOT_DIR/build_dtfusion.sh"

# Step 3: Setup test data
TEST_CSV="$ROOT_DIR/tests/scripts/dtfusion_ref.csv"
echo "Id,Name,Category" > "$TEST_CSV"
echo "1,Alice,A" >> "$TEST_CSV"
echo "2,Bob,B" >> "$TEST_CSV"
echo "5,Eve,A" >> "$TEST_CSV"

# Step 4: Run Orchestration
# dtpipe generates 5 rows (GenerateIndex 0..4), join with CSV on GenerateIndex == Id.
# Expected matches: Id 1 (Alice) and Id 2 (Bob). Eve (Id 5) should NOT appear.
echo "3. Executing DtFusion DtPipe Join..."

DTFUSION_CMD="$ROOT_DIR/dist/dtfusion"
DTPIPE_CMD="$ROOT_DIR/src/DtPipe/bin/Debug/net10.0/dtpipe"

"$DTFUSION_CMD" \
  --in ref="csv:$TEST_CSV" \
  --in pipe_stream="proc:$DTPIPE_CMD -i generate:5 -o arrow:-" \
  --query "SELECT p.GenerateIndex, r.Name, r.Category FROM pipe_stream p JOIN ref r ON CAST(p.GenerateIndex AS INT) = r.Id" \
  --out "csv:$ROOT_DIR/tests/scripts/dtfusion_out.csv"

# Step 5: Verify Output
echo "4. Verifying Output..."
cat "$ROOT_DIR/tests/scripts/dtfusion_out.csv" | grep "Alice" > /dev/null
cat "$ROOT_DIR/tests/scripts/dtfusion_out.csv" | grep "Bob" > /dev/null

if cat "$ROOT_DIR/tests/scripts/dtfusion_out.csv" | grep "Eve"; then
    echo "❌ Error: Eve should not be in the output!"
    exit 1
fi

rm "$TEST_CSV"
rm "$ROOT_DIR/tests/scripts/dtfusion_out.csv"

echo "✅ DtFusion Orchestration test passed!"
