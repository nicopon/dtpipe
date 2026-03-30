#!/bin/bash
set -e

# validate_docs.sh
# 1. Verifies all --flags mentioned in README.md and COOKBOOK.md are registered in the binary.
# 2. Runs representative README examples to ensure they work end-to-end.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
DTPIPE="$PROJECT_ROOT/dist/release/dtpipe"
TMP_DIR="$SCRIPT_DIR/artifacts/docs_examples"
mkdir -p "$TMP_DIR"

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

pass() { echo -e "${GREEN}  OK: $1${NC}"; }
fail() { echo -e "${RED}  FAIL: $1${NC}"; exit 1; }

echo "========================================"
echo "    DtPipe Docs Validation"
echo "========================================"

if [ ! -f "$DTPIPE" ]; then
    echo "Building release..."
    "$PROJECT_ROOT/build.sh" > /dev/null
fi

# ----------------------------------------
# 1. Flag presence check (README + COOKBOOK)
# ----------------------------------------
echo "--- [1] Documented flags present in --help ---"

DOC_FILES=("$PROJECT_ROOT/README.md" "$PROJECT_ROOT/COOKBOOK.md")
"$DTPIPE" --help > "$TMP_DIR/help.tmp" 2>&1

# Flags valid but not enumerated in --help (dynamic provider options, subcommands, dotnet-tool flags)
ALLOW_LIST="--project --version --help --fake-list --secrets --columnar-fast-path --compute-types --linux-pipes --migration --install --table --strategy --key --sampling-rate --sampling-seed --export-job --auto-migrate --strict-schema --metrics-path --unsafe-query --insert-mode --sql-processors --no-schema-validation --pre-exec --post-exec --finally-exec --from --ref --alias"
FAILED=0

for doc in "${DOC_FILES[@]}"; do
    [ -f "$doc" ] || continue
    echo "  Checking $(basename "$doc")..."
    FLAGS=$(grep -oE '\-\-[a-z0-9\-]+' "$doc" | grep -vE "^(--|---)$" | sort -u)
    for flag in $FLAGS; do
        [[ $ALLOW_LIST =~ $flag ]] && continue
        if ! grep -qF -- "$flag" "$TMP_DIR/help.tmp"; then
            echo -e "${RED}    [FAIL] '$flag' in $(basename "$doc") not found in --help${NC}"
            FAILED=1
        fi
    done
done

rm -f "$TMP_DIR/help.tmp"
[ $FAILED -eq 0 ] && pass "All documented flags found in --help" || fail "Some flags missing from --help (see above)"

# ----------------------------------------
# 2. README example smoke tests
# ----------------------------------------
echo "--- [2] README example execution ---"

run_test() {
    local title=$1
    shift
    echo -n "  $title ... "
    if "$@" > /dev/null 2>&1; then
        echo -e "${GREEN}OK${NC}"
    else
        echo -e "${RED}FAIL${NC}"
        "$@" 2>&1 | head -20
        exit 1
    fi
}

# Shorthand: run a transformer test using generate: source + dry-run
run_transformer_test() {
    local title=$1
    shift
    run_test "$title" "$DTPIPE" \
        -i "generate:5" \
        --fake "FIRSTNAME:name.firstName" \
        --fake "LASTNAME:name.lastName" \
        --fake "EMAIL:internet.email" \
        --fake "PHONE:random.number" \
        --fake "STATUS:lorem.word" \
        --fake "FULL_NAME:name.fullName" \
        --fake "INTERNAL_ID:random.uuid" \
        --drop "GenerateIndex" \
        "$@" \
        -o "$TMP_DIR/test.csv" --no-stats
}

run_test "Quick start (DuckDB to CSV)" \
    "$DTPIPE" -i "duck::memory:" --query "SELECT 1 AS id" -o "$TMP_DIR/out.csv" --no-stats

run_test "Dry-run with sampling" \
    "$DTPIPE" -i "duck::memory:" --query "SELECT 1 AS id" -o "$TMP_DIR/out.csv" --sampling-rate 0.5 --dry-run --no-stats

run_test "Export with YAML job file" \
    "$DTPIPE" -i "duck::memory:" --query "SELECT 1 AS id" -o "$TMP_DIR/out.csv" \
    --fake "FIRSTNAME:name.firstName" --export-job "$TMP_DIR/job.yaml" --no-stats

run_transformer_test "Null transformer"       --null "INTERNAL_ID"
run_transformer_test "Overwrite transformer"  --overwrite "STATUS:anonymized"
run_transformer_test "Format transformer"     --format "DISPLAY_NAME:{FIRSTNAME} {LASTNAME}"
run_transformer_test "Mask transformer"       --mask "EMAIL:###****"
run_transformer_test "Compute (JS row)"       --compute "FULL_NAME:return row.FIRSTNAME + ' ' + row.LASTNAME;"
run_transformer_test "Compute (auto-return)"  --compute "FULL_NAME:row.FIRSTNAME.toUpperCase()"
run_transformer_test "Fake anonymization"     --fake "FULL_NAME:name.fullName" --fake "EMAIL:internet.email" --fake-locale fr
run_transformer_test "Fake seeding by column" --fake "FULL_NAME:name.fullName" --fake-seed-column FIRSTNAME
run_transformer_test "Project"                --project "FIRSTNAME,LASTNAME,EMAIL"
run_transformer_test "Drop column"            --drop "INTERNAL_ID"

# YAML job file test
cat > "$TMP_DIR/readme_job.yaml" <<EOF
input: "duck::memory:"
query: "SELECT 'Alice' AS name, 'test@example.com' AS email, '0612345678' AS phone, 1 AS id"
output: "$TMP_DIR/readme_out.parquet"
transformers:
  - null:
      mappings:
        phone: ~
  - fake:
      mappings:
        name: name.fullName
        email: internet.email
      options:
        locale: fr
        seed-column: id
EOF

run_test "YAML job file execution (dry-run)" \
    "$DTPIPE" --job "$TMP_DIR/readme_job.yaml" --dry-run

# ----------------------------------------
# Cleanup
# ----------------------------------------
rm -rf "$TMP_DIR"

echo ""
echo -e "${GREEN}Docs validation complete!${NC}"
