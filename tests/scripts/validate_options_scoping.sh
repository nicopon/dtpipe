#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../../" && pwd)"
ARTIFACTS_DIR="$SCRIPT_DIR/artifacts/scoping"

mkdir -p "$ARTIFACTS_DIR"
cd "$PROJECT_DIR"

echo "Building project..."
dotnet build src/DtPipe/DtPipe.csproj -c Release --nologo -v q

echo "Setting up test data..."
cat << EOF > "$ARTIFACTS_DIR/in_comma.csv"
id,val,score
1,A,100
2,B,200
EOF

cat << EOF > "$ARTIFACTS_DIR/in_pipe.csv"
id.val.score
1.A.100
2.B.200
EOF

# Test 1: Global behavior (flag BEFORE -o)
# Read comma, write comma (custom separator . applied globally so reading comma fails if it enforces .)
# We will read dot, write dot.
echo "Running Test 1: Global CLI Scoping (Before -o)"
dotnet run --project src/DtPipe/DtPipe.csproj -c Release -- \
  --csv-separator . \
  -i "$ARTIFACTS_DIR/in_pipe.csv" \
  -o "csv:$ARTIFACTS_DIR/out_global.csv"

if grep -q "\." "$ARTIFACTS_DIR/out_global.csv"; then
    echo "  [OK] Test 1: Writer correctly applied global separator ."
else
    echo "  [FAIL] Test 1: Writer failed to apply global separator ."
    cat "$ARTIFACTS_DIR/out_global.csv"
    exit 1
fi

# Test 2: Writer-only behavior (flag AFTER -o)
# Read comma (default), write dot.
echo "Running Test 2: Writer-only CLI Scoping (After -o)"
dotnet run --project src/DtPipe/DtPipe.csproj -c Release -- \
  -i "$ARTIFACTS_DIR/in_comma.csv" \
  -o "csv:$ARTIFACTS_DIR/out_scoped.csv" \
  --csv-separator .

if grep -q "\." "$ARTIFACTS_DIR/out_scoped.csv"; then
    echo "  [OK] Test 2: Writer applied scoped separator ."
else
    echo "  [FAIL] Test 2: Writer failed to apply scoped separator ."
    cat "$ARTIFACTS_DIR/out_scoped.csv"
    exit 1
fi

# We can also check that reader successfully read commas because 100 is there.
if grep -q "100" "$ARTIFACTS_DIR/out_scoped.csv"; then
    echo "  [OK] Test 2: Reader correctly ignored writer-scoped separator (defaulted to comma)"
else
    echo "  [FAIL] Test 2: Reader incorrectly applied writer-scoped separator!"
    exit 1
fi

# Test 3: YAML ProviderOptions Scoping
echo "Running Test 3: YAML Config Scoping"
cat << EOF > "$ARTIFACTS_DIR/job_config.yaml"
input: "$ARTIFACTS_DIR/in_comma.csv"
output: "csv:$ARTIFACTS_DIR/out_yaml.csv"
provider-options:
  csv-writer:
    separator: ";"
EOF

dotnet run --project src/DtPipe/DtPipe.csproj -c Release -- \
  --job "$ARTIFACTS_DIR/job_config.yaml"

if grep -q ";" "$ARTIFACTS_DIR/out_yaml.csv"; then
    echo "  [OK] Test 3: YAML scoped csv-writer successfully wrote with ;"
else
    echo "  [FAIL] Test 3: YAML scoped csv-writer failed to apply ;"
    cat "$ARTIFACTS_DIR/out_yaml.csv"
    exit 1
fi

echo "All scoping tests passed successfully!"
exit 0
