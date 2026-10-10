#!/usr/bin/env bash
# validate_arrow_roundtrip.sh — a stream written to `arrow:` is read back under the contract it left with.
#
# The writer declares the IPC schema from the first batch it receives, since only a batch carries a
# timestamp's zone and the metadata of an extension type (arrow.uuid). Declared from the columns
# instead, a DuckDB TIMESTAMPTZ would arrive as UTC and the contract hash would change. This
# compares the contract of a direct read with the one of a read through `arrow:-`, and checks that a
# stream with no row still carries its columns. The session zone is pinned away from UTC: a runner
# is on UTC, where a writer that drops the zone is indistinguishable from one that keeps it.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
DTPIPE="${DTPIPE:-$ROOT_DIR/dist/release/dtpipe}"
export DTPIPE_NO_TUI=1

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'
pass() { echo -e "  ${GREEN}OK: $1${NC}"; }
fail() { echo -e "  ${RED}FAIL: $1${NC}"; exit 1; }

echo "========================================"
echo "    DtPipe Arrow Round-Trip Contract"
echo "========================================"

if [ ! -f "$DTPIPE" ]; then
    echo "Building release..."
    "$ROOT_DIR/build.sh" > /dev/null
fi

QUERY="SELECT now() AS ts, uuid() AS u"
ZONE="Asia/Tokyo"
INIT="SET TimeZone='$ZONE'"
hash_of() { python3 -I -c 'import json, sys; print(json.load(open(sys.argv[1]))["hash"])' "$1"; }

"$DTPIPE" -i "duck:memory" --duck-init "$INIT" --query "$QUERY" --dry-run 1 --contract-save "$TMP_DIR/direct.json" -o null: > /dev/null 2>&1 \
    || fail "direct read did not run"
"$DTPIPE" -i "duck:memory" --duck-init "$INIT" --query "$QUERY" -o arrow:- 2> /dev/null \
    | "$DTPIPE" -i arrow:- --dry-run 1 --contract-save "$TMP_DIR/roundtrip.json" -o null: > /dev/null 2>&1 \
    || fail "read through arrow:- did not run"

python3 -I -c '
import json, sys
fields = {f["name"]: f for f in json.load(open(sys.argv[1]))["schema"]["fields"]}
sys.exit(0 if fields["ts"]["type"] == "timestamp:us:" + sys.argv[2] else 1)
' "$TMP_DIR/direct.json" "$ZONE" || fail "the direct read does not carry the pinned zone $ZONE"

DIRECT=$(hash_of "$TMP_DIR/direct.json")
ROUNDTRIP=$(hash_of "$TMP_DIR/roundtrip.json")
[ "$DIRECT" = "$ROUNDTRIP" ] || fail "contract changed through arrow:- ($DIRECT -> $ROUNDTRIP)"
pass "TIMESTAMPTZ and uuid keep their contract through arrow:-"

# A fragment that produced no row still has to tell the next one its columns.
"$DTPIPE" -i "duck:memory" --duck-init "$INIT" --query "$QUERY WHERE false" -o arrow:- 2> /dev/null \
    | "$DTPIPE" -i arrow:- --dry-run 1 --contract-save "$TMP_DIR/empty.json" -o null: > /dev/null 2>&1 \
    || fail "an empty stream through arrow:- did not run"
python3 -I -c '
import json, sys
names = [f["name"] for f in json.load(open(sys.argv[1]))["schema"]["fields"]]
sys.exit(0 if names == ["ts", "u"] else 1)
' "$TMP_DIR/empty.json" || fail "an empty stream lost its columns"
pass "an empty stream still declares its columns"

echo ""
echo "All Arrow round-trip checks passed."
