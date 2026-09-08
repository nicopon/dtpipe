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

# Build a "help universe" from the WHOLE command tree, not just the root.
# We walk the subcommand graph generically (no hardcoded names): the root help
# lists subcommands under "SUBCOMMANDS:"; each subcommand's help (System.CommandLine)
# lists further commands under "Commands:". A command path is expanded once by
# hashing its help output — a path that falls back to the root help (e.g. `secret
# set --help`) rehashes to the root's content and is skipped, so the walk terminates.
UNIVERSE="$TMP_DIR/help_universe.txt"; : > "$UNIVERSE"
SEEN="$TMP_DIR/seen_hashes.txt";     : > "$SEEN"
MAX_DEPTH=4
queue=("0|")
while [ ${#queue[@]} -gt 0 ]; do
    entry="${queue[0]}"; queue=("${queue[@]:1}")
    depth="${entry%%|*}"; path="${entry#*|}"
    out="$("$DTPIPE" $path --help 2>&1 || true)"
    h="$(printf '%s' "$out" | { command -v shasum >/dev/null 2>&1 && shasum || md5; } 2>/dev/null | awk '{print $1}')"
    [ -z "$h" ] && h="$$-$path"
    grep -qxF "$h" "$SEEN" && continue
    echo "$h" >> "$SEEN"
    printf '%s\n' "$out" >> "$UNIVERSE"
    [ "$depth" -ge "$MAX_DEPTH" ] && continue
    while IFS= read -r child; do
        [ -z "$child" ] && continue
        [ "$child" = "-" ] && continue
        queue+=("$((depth+1))|${path:+$path }$child")
    done < <(printf '%s\n' "$out" | awk '
        /^(SUBCOMMANDS:|Commands:)$/ { in_section = 1; next }
        /^[^ ]/   { in_section = 0 }
        /^$/      { in_section = 0 }
        in_section && NF { print $1 }')
done

# Tokens that are command names, not flags (--install/--uninstall are completion
# subcommands), plus dotnet-tool-only flags and dynamic provider options that the
# help intentionally does not enumerate.
ALLOW_LIST="--project --install --uninstall --fake-list --secrets --columnar-fast-path --linux-pipes --migration --sql-processors"
FAILED=0

for doc in "${DOC_FILES[@]}"; do
    [ -f "$doc" ] || continue
     echo "  Checking $(basename "$doc")..."
    # Markdown link targets carry "--" too: a heading like "A & B" yields the anchor
    # "#a--b", which is not a flag. Strip (...) link targets before extracting, or the check
    # reports a documentation cross-reference as an undocumented flag.
    FLAGS=$(sed -E 's/\]\([^)]*\)/]/g' "$doc" | grep -oE '\-\-[a-z0-9\-]+' | grep -vE "^(--|---)$" | sort -u)
    for flag in $FLAGS; do
        [[ $ALLOW_LIST =~ (^|[[:space:]])$flag([[:space:]]|$) ]] && continue
        if ! grep -qF -- "$flag" "$UNIVERSE"; then
             echo -e "${RED}     [FAIL] '$flag' in $(basename "$doc") not found in --help${NC}"
            FAILED=1
        fi
    done
done

rm -f "$UNIVERSE" "$SEEN"
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
    "$DTPIPE" -i "duck::memory:" --query "SELECT 1 AS id" \
    --fake "FIRSTNAME:name.firstName" -o "$TMP_DIR/out.csv" --export-job "$TMP_DIR/job.yaml" --no-stats

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
main:
  input: "duck::memory:"
  output: "$TMP_DIR/readme_out.parquet"
  provider-options:
    duck:
      query: "SELECT 'Alice' AS name, 'test@example.com' AS email, '0612345678' AS phone, 1 AS id"
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
# 3. MCP tool table matches the server
# ----------------------------------------
# The table in REFERENCE.md is what a reader is pointed at as the catalogue, and it is written by
# hand — it named 'register-yaml-job', which the server has never exposed, while omitting four
# tools it does. Ask the server instead of trusting the table.
echo "--- [3] REFERENCE.md MCP tool table matches 'dtpipe mcp' ---"

MCP_OUT="$TMP_DIR/mcp_tools.json"
MCP_IN="$TMP_DIR/mcp_in.fifo"
# `dtpipe mcp` serves STDIO until it is stopped: it does not exit when its input ends, so the
# request goes in through a fifo and the server is killed once the reply is on disk. Without the
# kill this check waits forever.
rm -f "$MCP_IN"; mkfifo "$MCP_IN"
"$DTPIPE" mcp < "$MCP_IN" > "$MCP_OUT" 2>/dev/null &
MCP_PID=$!
{ printf '%s\n' \
    '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"validate_docs","version":"1"}}}' \
    '{"jsonrpc":"2.0","method":"notifications/initialized"}' \
    '{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}'
  # The fifo is held open while the server answers; closing it early would end the session.
  for _ in $(seq 30); do grep -q '"id":2' "$MCP_OUT" 2>/dev/null && break; sleep 0.5; done
} > "$MCP_IN"
kill "$MCP_PID" 2>/dev/null || true
wait "$MCP_PID" 2>/dev/null || true
rm -f "$MCP_IN"

SERVED="$(python3 -c "
import json, sys
for line in open('$MCP_OUT'):
    line = line.strip()
    if not line: continue
    m = json.loads(line)
    if m.get('id') == 2:
        print('\n'.join(sorted(t['name'] for t in m['result']['tools'])))
        break
")"
[ -z "$SERVED" ] && fail "'dtpipe mcp' returned no tool list"

DOCUMENTED="$(sed -n '/^### Exposed MCP Tools/,/^---$/p' "$PROJECT_ROOT/REFERENCE.md" \
    | sed -n 's/^| `\([a-z-]*\)` |.*/\1/p' | sort)"

MISSING="$(comm -23 <(printf '%s\n' "$SERVED") <(printf '%s\n' "$DOCUMENTED") | tr '\n' ' ')"
EXTRA="$(comm -13 <(printf '%s\n' "$SERVED") <(printf '%s\n' "$DOCUMENTED") | tr '\n' ' ')"

[ -n "$(echo "$MISSING" | tr -d ' ')" ] && fail "MCP tools served but absent from REFERENCE.md: $MISSING"
[ -n "$(echo "$EXTRA" | tr -d ' ')" ] && fail "REFERENCE.md documents MCP tools the server does not expose: $EXTRA"
pass "$(printf '%s\n' "$SERVED" | wc -l | tr -d ' ') MCP tools, table and server agree"

# ----------------------------------------
# Cleanup
# ----------------------------------------
rm -rf "$TMP_DIR"

echo ""
echo -e "${GREEN}Docs validation complete!${NC}"
