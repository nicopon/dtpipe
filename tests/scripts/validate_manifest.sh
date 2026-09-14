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
YELLOW='\033[1;33m'
NC='\033[0m'

pass() { echo -e "  ${GREEN}OK: $1${NC}"; }

# Diagnostic dump for a failure seen so far only on GitHub-hosted ubuntu-latest
# ("provider 'arrow' not registered"), reproducible on every real CI run but never
# locally: not in a clean Docker clone built with the exact CI publish flags, x64
# emulated or native arm64, 25 isolated runs, 25 sequential runs matching the
# validator order, POSIX or UTF-8 locale. Fires on the way to fail() so a passing
# run stays as quiet as before. Delete once the cause is found and fixed.
dump_manifest_diagnostics() {
    echo -e "  ${YELLOW}--- diagnostic dump (validate_manifest) ---${NC}"
    echo "  uname: $(uname -a)"
    echo "  locale:"; locale 2>&1 | sed 's/^/    /'
    echo "  dotnet: $(dotnet --version 2>&1)"
    echo "  binary: $(ls -la "$DTPIPE" 2>&1)"
    echo "  relevant env:"; env | grep -E '^(TERM|COLUMNS|LINES|LANG|LC_|DTPIPE_|CI|GITHUB_)' | sort | sed 's/^/    /'
    echo "  raw 'dtpipe providers' output:"
    echo "$RAW_OUTPUT" | sed 's/^/    /'
    echo "  parsed ACTUAL list: $(echo "$ACTUAL" | tr '\n' ',')"
    echo "  three more invocations, checking whether 'arrow' flickers:"
    local i retry actual_retry
    for i in 1 2 3; do
        retry=$("$DTPIPE" providers 2>&1)
        actual_retry=$(echo "$retry" | awk '/│/ {gsub(/│/,""); gsub(/^ +| +$/,"",$1); if ($1!="" && $1!="Provider") print $1}' | sort -u)
        if echo "$actual_retry" | grep -qx "arrow"; then
            echo "    retry $i: arrow present"
        else
            echo "    retry $i: arrow MISSING"
        fi
    done
    echo -e "  ${YELLOW}--- end diagnostic dump ---${NC}"
}

fail() {
    dump_manifest_diagnostics
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

# "Provider" is the table header, not a component.
RAW_OUTPUT=$("$DTPIPE" providers 2>&1)

# Strip SGR sequences before reading the cells. Spectre colours the table whenever it believes the
# sink renders ANSI, and "a CI provider is running me" is one of those beliefs: on GitHub Actions
# every cell arrives as "\033[38;5;14marrow\033[0m" even though stdout is a pipe here, while the
# same command redirected on a workstation is plain. Without this, $1 is the escape sequence, every
# provider reads as missing, and the script says "provider 'arrow' not registered" about a provider
# the dump above shows registered — which is exactly what it said, on CI only, for four days.
ANSI=$(printf '\033')
ACTUAL=$(echo "$RAW_OUTPUT" | sed "s/${ANSI}\[[0-9;]*m//g" | awk '/│/ {gsub(/│/,""); gsub(/^ +| +$/,"",$1); if ($1!="" && $1!="Provider") print $1}' | sort -u)
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
