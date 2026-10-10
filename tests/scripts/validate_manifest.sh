#!/bin/bash
set -e

# validate_manifest.sh
# F13 — every provider documented in REFERENCE.md is registered by the component
# catalog, and the catalog output stays in sync with the docs (drift guard).

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
ARTIFACTS_DIR="$SCRIPT_DIR/artifacts/manifest"
mkdir -p "$ARTIFACTS_DIR"

DTPIPE="$PROJECT_ROOT/dist/release/dtpipe"
export DTPIPE_NO_TUI=1

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

pass() { echo -e "  ${GREEN}OK: $1${NC}"; }

fail() {
    echo -e "  ${RED}FAIL: $1${NC}"
    exit 1
}

echo "========================================"
echo "    DtPipe Provider Manifest Validation"
echo "========================================"

if [ ! -f "$DTPIPE" ]; then
    echo "Building release..."
    "$PROJECT_ROOT/build.sh" > /dev/null
fi

EXPECTED="arrow arrow-memory checksum csv duck generate jsonl mem merge mssql mysql null ora parquet pg sql sqlite xml"

# The inventory is read from the command's own machine form, never from the table meant for a
# person: a table's colours and borders change with the terminal the runner believes it has.
ACTUAL=$("$DTPIPE" providers --format json | python3 -I -c 'import json, sys; print("\n".join(sorted({e["name"] for e in json.load(sys.stdin)})))')
for p in $EXPECTED; do
    echo "$ACTUAL" | grep -qx "$p" || fail "provider '$p' not registered"
done
pass "all expected providers registered"

# Docs drift, help -> docs. The loop must iterate the DISCOVERED set: walking a literal list here
# would only ever check the providers someone remembered to add to it, so a new one could never
# fail this gate.
#
# "mem" and "arrow-memory" are excluded on purpose: they are the DAG's inter-branch channels,
# addressed through --alias / --from rather than typed as a connection prefix. They are registered
# as components because the engine routes them like providers, not because a user ever writes one.
INTERNAL="mem arrow-memory"
for p in $ACTUAL; do
    case " $INTERNAL " in *" $p "*) continue ;; esac
    grep -q "$p" "$PROJECT_ROOT/REFERENCE.md" || fail "provider '$p' missing from REFERENCE.md"
done
pass "REFERENCE.md covers all user-facing registered providers"

echo ""
echo "All manifest checks passed."
