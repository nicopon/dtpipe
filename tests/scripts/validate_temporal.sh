#!/usr/bin/env bash
# validate_temporal.sh — timestamps must not depend on the machine's time zone.
#
# A DateTime with Kind=Unspecified — what every ADO driver returns for a zone-less column
# (PostgreSQL timestamp, MySQL datetime, SQL Server datetime2) — is a wall clock with no zone.
# Arrow stores an instant, so the write side must pick one; resolving it against the host's zone
# makes output differ between machines, and shifts the value on every columnar hop.
#
# TemporalNormalizationTests pins the conversion rule, but its assertions are machine-independent
# and so cannot detect a zone dependency on a UTC CI runner. This script can: it runs the real
# binary under two TZ values and compares. It is the only check that fails if the behaviour
# returns.
#
# Three separate code paths reach Arrow timestamps, so all three are covered:
#   - PostgreSQL binary COPY reader   (its own Arrow build path)
#   - ADO columnar reader             (MySQL / SQL Server / Oracle / SQLite, via Arrow.Ado)
#   - row -> columnar bridge          (any pipeline carrying a row-mode transformer)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Test infrastructure endpoints. Sourcing this is what declares that this script
# needs tests/infra running (see lib/test_connections.sh).
# shellcheck source=lib/test_connections.sh
source "$SCRIPT_DIR/lib/test_connections.sh"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
DTPIPE="${DTPIPE:-$ROOT_DIR/src/DtPipe/bin/Debug/net10.0/DtPipe}"
if [ ! -f "$DTPIPE" ]; then
    DTPIPE="$ROOT_DIR/dist/release/dtpipe"
fi
export DTPIPE_NO_TUI=1

TMP_DIR="$(mktemp -d)"
cleanup() { rm -rf "$TMP_DIR"; }
trap cleanup EXIT

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'
pass() { echo -e "  ${GREEN}OK: $1${NC}"; }
fail() { echo -e "  ${RED}FAIL: $1${NC}"; exit 1; }

MY_CONN="$MYSQL"
PG_EXEC=(docker exec dtpipe-integ-postgres psql -U postgres -d integration)

# Two zones on opposite sides of UTC, so a drift in either direction shows up. Tokyo is chosen
# for the 00:30 row: a westward shift there rolls the value onto the previous calendar day.
TZ_A="UTC"
TZ_B="Asia/Tokyo"

echo "========================================"
echo "    DtPipe Temporal Zone Invariance"
echo "========================================"

if ! nc -z localhost 5440 2>/dev/null; then
    echo "SKIP: PostgreSQL container (port 5440) not reachable. Start tests/infra."
    exit 0
fi

# Expected wall clocks, written out rather than derived, so the test states what is correct
# instead of merely checking that two runs agree with each other.
EXPECTED_TS_1="2026-08-28 09:30:00.000000"
EXPECTED_TS_2="2026-01-01 00:30:00.000000"

"${PG_EXEC[@]}" -q -c "
    DROP TABLE IF EXISTS temporal_zone_probe;
    CREATE TABLE temporal_zone_probe (id INT PRIMARY KEY, d DATE, ts TIMESTAMP, tstz TIMESTAMPTZ);
    INSERT INTO temporal_zone_probe VALUES
      (1,'2026-08-28','2026-08-28 09:30:00','2026-08-28 09:30:00+02'),
      (2,'2026-01-01','2026-01-01 00:30:00','2026-01-01 00:30:00+01');" >/dev/null 2>&1 \
    || fail "could not seed the PostgreSQL probe table"

# Runs the same pipeline under both zones and requires byte-identical output.
# shellcheck disable=SC2317
run_both_zones() { # label, out_prefix, args...
    local label="$1" prefix="$2"; shift 2
    TZ="$TZ_A" "$DTPIPE" "$@" -o "$TMP_DIR/${prefix}_a.csv" --no-stats >/dev/null 2>&1 \
        || fail "[$label] run under TZ=$TZ_A failed"
    TZ="$TZ_B" "$DTPIPE" "$@" -o "$TMP_DIR/${prefix}_b.csv" --no-stats >/dev/null 2>&1 \
        || fail "[$label] run under TZ=$TZ_B failed"
    diff -q "$TMP_DIR/${prefix}_a.csv" "$TMP_DIR/${prefix}_b.csv" >/dev/null \
        || fail "[$label] output differs between TZ=$TZ_A and TZ=$TZ_B:
$(diff "$TMP_DIR/${prefix}_a.csv" "$TMP_DIR/${prefix}_b.csv" | head -8)"
}

# ------------------------------------------------------------------------------
# 1. PostgreSQL binary COPY reader.
# ------------------------------------------------------------------------------
echo -e "\n--- Test 1: pg: reader (binary COPY path) ---"
run_both_zones "pg" "pg" -i "$PG" \
    --query "SELECT id, d, ts, tstz FROM temporal_zone_probe ORDER BY id"

grep -q "$EXPECTED_TS_1" "$TMP_DIR/pg_a.csv" || fail "[pg] expected wall clock '$EXPECTED_TS_1' absent:
$(cat "$TMP_DIR/pg_a.csv")"
grep -q "$EXPECTED_TS_2" "$TMP_DIR/pg_a.csv" || fail "[pg] expected wall clock '$EXPECTED_TS_2' absent:
$(cat "$TMP_DIR/pg_a.csv")"
grep -q "^1,2026-08-28," "$TMP_DIR/pg_a.csv" || fail "[pg] DATE column drifted off its calendar day"
pass "identical under both zones, wall clocks preserved (timestamp, timestamptz, date)"

# ------------------------------------------------------------------------------
# 2. Row -> columnar bridge: a row-mode transformer forces the pipeline off the
#    pure columnar path, through a separate append implementation.
# ------------------------------------------------------------------------------
echo -e "\n--- Test 2: row-mode bridge (--compute forces row mode) ---"
run_both_zones "rowmode" "rowmode" -i "$PG" \
    --query "SELECT id, d, ts, tstz FROM temporal_zone_probe ORDER BY id" \
    --compute "Tag:'x'"

grep -q "$EXPECTED_TS_1" "$TMP_DIR/rowmode_a.csv" || fail "[rowmode] expected wall clock '$EXPECTED_TS_1' absent:
$(cat "$TMP_DIR/rowmode_a.csv")"
pass "identical under both zones through the row-mode bridge"

# ------------------------------------------------------------------------------
# 3. Full database round trip: exercises the write side as well as the read side.
# ------------------------------------------------------------------------------
echo -e "\n--- Test 3: pg: -> pg: round trip ---"
for tz in "$TZ_A" "$TZ_B"; do
    "${PG_EXEC[@]}" -q -c "DROP TABLE IF EXISTS temporal_zone_back;" >/dev/null 2>&1
    TZ="$tz" "$DTPIPE" -i "$PG" \
        --query "SELECT id, d, ts, tstz FROM temporal_zone_probe ORDER BY id" \
        -o "$PG" --table temporal_zone_back --strategy Recreate --key id --no-stats >/dev/null 2>&1 \
        || fail "[roundtrip] write under TZ=$tz failed"

    drift=$("${PG_EXEC[@]}" -t -A -c "
        SELECT COUNT(*) FROM temporal_zone_probe s JOIN temporal_zone_back b USING (id)
        WHERE s.ts IS DISTINCT FROM b.ts OR s.d IS DISTINCT FROM b.d;" 2>/dev/null | tr -d ' \r')
    [ "$drift" = "0" ] || fail "[roundtrip] $drift row(s) drifted under TZ=$tz"
done
pass "pg -> pg preserves timestamp and date under both zones"

# ------------------------------------------------------------------------------
# 4. ADO columnar reader — a different Arrow build path from PostgreSQL's.
# ------------------------------------------------------------------------------
echo -e "\n--- Test 4: mysql: reader (ADO columnar path) ---"
if nc -z 127.0.0.1 3306 2>/dev/null; then
    docker exec dtpipe-integ-mysql mysql -u testuser -ppassword integration -e \
        "DROP TABLE IF EXISTS temporal_zone_probe;
         CREATE TABLE temporal_zone_probe (id INT PRIMARY KEY, d DATE, ts DATETIME(6));
         INSERT INTO temporal_zone_probe VALUES
           (1,'2026-08-28','2026-08-28 09:30:00'),
           (2,'2026-01-01','2026-01-01 00:30:00');" >/dev/null 2>&1 \
        || fail "could not seed the MySQL probe table"

    run_both_zones "mysql" "mysql" -i "$MY_CONN" \
        --query "SELECT id, d, ts FROM temporal_zone_probe ORDER BY id"

    grep -q "$EXPECTED_TS_1" "$TMP_DIR/mysql_a.csv" || fail "[mysql] expected wall clock '$EXPECTED_TS_1' absent:
$(cat "$TMP_DIR/mysql_a.csv")"
    grep -q "$EXPECTED_TS_2" "$TMP_DIR/mysql_a.csv" || fail "[mysql] expected wall clock '$EXPECTED_TS_2' absent:
$(cat "$TMP_DIR/mysql_a.csv")"
    pass "identical under both zones, wall clocks preserved"
else
    echo "SKIP: MySQL container (port 3306) not reachable."
fi

echo ""
echo "All temporal zone-invariance checks passed."
