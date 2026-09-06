#!/bin/bash
set -e

# validate_sample_safety.sh
# Sample-mode safety is a READ-side property, on the real binary.
#
# Neutralising the writer says nothing about a source that mutates on its way past:
# DELETE … RETURNING streams rows while the server destroys them, --duck-init runs arbitrary
# SQL before the read, and --limit bounds what the client reads, never what was deleted.
#
# Scope, honestly: this exercises the verb scan and the reporting, with no database container.
# The server-enforced half (SET TRANSACTION READ ONLY on PostgreSQL/Oracle/MySQL, PRAGMA
# query_only on SQLite) needs live infrastructure and is covered by SampleModeSafetyGateTests
# at the unit level; what cannot be checked without a server is the server's own refusal.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
DTPIPE="$PROJECT_ROOT/dist/release/dtpipe"

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

pass() { echo -e "  ${GREEN}OK: $1${NC}"; }
fail() { echo -e "  ${RED}FAIL: $1${NC}"; exit 1; }

echo "========================================"
echo "    DtPipe Sample Safety Validation"
echo "========================================"

[ -x "$DTPIPE" ] || fail "dist/release/dtpipe not built — run ./build.sh"

WORK="$(mktemp -d)"
export DTPIPE_STATE_HOME="$WORK/state"
trap 'rm -rf "$WORK"' EXIT
cd "$WORK"

# ── 1. A destructive statement on the source side is refused ──────────────────
OUT="$("$DTPIPE" -i "duck::memory:" --duck-init "DROP TABLE IF EXISTS victim" \
        --query "SELECT 1 AS x" -o csv:out.csv --dry-run 1 --no-stats 2>&1 || true)"
echo "$OUT" | grep -q "Sample mode refuses" \
    || { echo "$OUT" | tail -5; fail "a destructive --duck-init was not refused in sample mode"; }
pass "a source-side destructive statement is refused"

# ── 2. …and the target was never touched ──────────────────────────────────────
[ ! -f out.csv ] || fail "the target file was created by a refused run"
pass "the refused run created nothing"

# ── 3. An ordinary pipeline still runs ────────────────────────────────────────
OUT="$("$DTPIPE" -i "duck::memory:" --duck-init "CREATE TABLE t(x INT)" \
        --query "SELECT 1 AS x" -o csv:ok.csv --dry-run 1 --no-stats 2>&1)"
echo "$OUT" | grep -q "writer was neutralised" \
    || { echo "$OUT" | tail -5; fail "an ordinary sample run did not complete"; }
[ ! -f ok.csv ] || fail "sample mode created the target file"
pass "an ordinary sample run completes and writes nothing"

# ── 4. The report states which guarantee it actually had ──────────────────────
echo "$OUT" | grep -q "Source protection" \
    || fail "the run did not report what it could guarantee about the source"
pass "the report states the source guarantee rather than implying one"

echo ""
echo -e "${GREEN}Sample safety checks passed.${NC}"
