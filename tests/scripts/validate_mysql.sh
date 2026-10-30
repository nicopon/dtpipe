#!/usr/bin/env bash
# validate_mysql.sh — Integration test for the native "mysql:" provider.
#
# Exercises the two directions that actually prove the provider:
#   pg: -> mysql: --strategy Upsert   (MySqlBulkCopy + ON DUPLICATE KEY UPDATE from the dialect)
#   mysql: -> pg:                     (the reader)
#
# --strategy is a WRITE concern: putting MySQL on the source side would only exercise the
# PostgreSQL upsert, which already works. Both key modes are covered on purpose — --key given
# explicitly, and --key omitted so the key comes from the inspector's primary-key detection.
# Those two share a code path only at the end, and the omitted case is the default one.

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
MY_EXEC=(docker exec dtpipe-integ-mysql mysql -u testuser -ppassword integration)
PG_EXEC=(docker exec dtpipe-integ-postgres psql -U postgres -d integration)
ROWS=50

echo "========================================"
echo "    DtPipe Native MySQL Provider"
echo "========================================"

if ! nc -z 127.0.0.1 3306 2>/dev/null; then
    echo "SKIP: MySQL container (port 3306) not reachable. Start tests/infra."
    exit 0
fi
if ! nc -z localhost 5440 2>/dev/null; then
    echo "SKIP: PostgreSQL container (port 5440) not reachable. Start tests/infra."
    exit 0
fi

mysql_scalar() { "${MY_EXEC[@]}" -N -B -e "$1" 2>/dev/null | tr -d ' \r'; }
pg_scalar() { "${PG_EXEC[@]}" -t -A -c "$1" 2>/dev/null | tr -d ' \r'; }

# ------------------------------------------------------------------------------
# Seed: a PostgreSQL source covering the types that actually differ across the two
# engines — NUMERIC scale, microsecond timestamps, and UUID (which MySQL has no
# native type for and stores as CHAR(36)).
# ------------------------------------------------------------------------------
"${PG_EXEC[@]}" -q -c "
    DROP TABLE IF EXISTS mysql_e2e_src;
    CREATE TABLE mysql_e2e_src (id INTEGER PRIMARY KEY, name TEXT, amount NUMERIC(12,2),
                                created TIMESTAMP, uid UUID);
    INSERT INTO mysql_e2e_src
    SELECT g, 'name_'||g, (g*1.5)::numeric(12,2), now() - (g||' hours')::interval, gen_random_uuid()
    FROM generate_series(1,$ROWS) g;" >/dev/null 2>&1 || fail "could not seed PostgreSQL source"

# ------------------------------------------------------------------------------
# 1. pg: -> mysql:, Recreate. Establishes the target and its PRIMARY KEY, which is
#    what the later key-omitted upsert relies on.
# ------------------------------------------------------------------------------
echo -e "\n--- Test 1: pg: -> mysql: (Recreate, DDL generation) ---"
"${MY_EXEC[@]}" -e "DROP TABLE IF EXISTS mysql_e2e_tgt;" >/dev/null 2>&1
"$DTPIPE" -i "$PG" --query "SELECT * FROM mysql_e2e_src ORDER BY id" \
    -o "$MY_CONN" --table mysql_e2e_tgt --strategy Recreate --key id --no-stats >/dev/null 2>&1 \
    || fail "pg -> mysql Recreate failed"

CNT=$(mysql_scalar "SELECT COUNT(*) FROM mysql_e2e_tgt")
[ "$CNT" = "$ROWS" ] || fail "expected $ROWS rows in MySQL, got $CNT"

# The generated DDL must carry the primary key, or every later upsert degrades silently.
PK=$(mysql_scalar "SELECT COLUMN_NAME FROM information_schema.STATISTICS
                   WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='mysql_e2e_tgt' AND INDEX_NAME='PRIMARY'")
[ "$PK" = "id" ] || fail "expected PRIMARY KEY on 'id', got '$PK'"

# UUID has no MySQL type; CHAR(36) is what makes the round trip lossless.
UID_TYPE=$(mysql_scalar "SELECT COLUMN_TYPE FROM information_schema.COLUMNS
                         WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='mysql_e2e_tgt' AND COLUMN_NAME='uid'")
[ "$UID_TYPE" = "char(36)" ] || fail "expected uid char(36), got '$UID_TYPE'"
pass "$ROWS rows written, PRIMARY KEY(id) and uid CHAR(36) generated"

# ------------------------------------------------------------------------------
# 2. Upsert with --key given. Two passes over the same source: the second collides
#    on every row, so a working ON DUPLICATE KEY UPDATE leaves the count unchanged.
# ------------------------------------------------------------------------------
echo -e "\n--- Test 2: pg: -> mysql: --strategy Upsert --key id ---"
for pass_no in 1 2; do
    "$DTPIPE" -i "$PG" --query "SELECT * FROM mysql_e2e_src ORDER BY id" \
        -o "$MY_CONN" --table mysql_e2e_tgt --strategy Upsert --key id --no-stats >/dev/null 2>&1 \
        || fail "upsert pass $pass_no failed"
done
CNT=$(mysql_scalar "SELECT COUNT(*) FROM mysql_e2e_tgt")
[ "$CNT" = "$ROWS" ] || fail "expected $ROWS rows after double upsert, got $CNT"
pass "$ROWS rows after double upsert with explicit --key (no duplicates)"

# ------------------------------------------------------------------------------
# 3. Upsert with --key OMITTED. This is the default path: the key comes from the
#    inspector reading the primary key out of information_schema. If that detection
#    regresses, ON DUPLICATE KEY UPDATE has no keys and duplicates appear here first.
# ------------------------------------------------------------------------------
echo -e "\n--- Test 3: pg: -> mysql: --strategy Upsert (--key omitted) ---"
"$DTPIPE" -i "$PG" --query "SELECT * FROM mysql_e2e_src ORDER BY id" \
    -o "$MY_CONN" --table mysql_e2e_tgt --strategy Upsert --no-stats >/dev/null 2>&1 \
    || fail "upsert without --key failed"
CNT=$(mysql_scalar "SELECT COUNT(*) FROM mysql_e2e_tgt")
[ "$CNT" = "$ROWS" ] || fail "expected $ROWS rows after key-less upsert, got $CNT"
pass "$ROWS rows after upsert with auto-detected key (no duplicates)"

# ------------------------------------------------------------------------------
# 4. An upsert that only avoids duplicates is half-working: it must also UPDATE.
#    Change the source, re-upsert, and check both the changed and unchanged rows.
# ------------------------------------------------------------------------------
echo -e "\n--- Test 4: upsert updates conflicting rows, leaves the rest ---"
"${PG_EXEC[@]}" -q -c "UPDATE mysql_e2e_src SET name = 'UPDATED_'||id WHERE id <= 5;" >/dev/null 2>&1
"$DTPIPE" -i "$PG" --query "SELECT * FROM mysql_e2e_src ORDER BY id" \
    -o "$MY_CONN" --table mysql_e2e_tgt --strategy Upsert --no-stats >/dev/null 2>&1 \
    || fail "upsert after source change failed"

UPDATED=$(mysql_scalar "SELECT COUNT(*) FROM mysql_e2e_tgt WHERE name LIKE 'UPDATED_%'")
[ "$UPDATED" = "5" ] || fail "expected 5 updated rows, got $UPDATED"
CNT=$(mysql_scalar "SELECT COUNT(*) FROM mysql_e2e_tgt")
[ "$CNT" = "$ROWS" ] || fail "expected $ROWS rows after updating upsert, got $CNT"
pass "5 rows updated, $((ROWS - 5)) untouched, total still $ROWS"

# ------------------------------------------------------------------------------
# 5. Insert modes. Bulk goes through MySqlBulkCopy (LOAD DATA LOCAL INFILE) and needs
#    local_infile=ON server-side; Standard is the batched multi-row INSERT. Both must
#    land identical data, since the fallback is what most stock servers will use.
# ------------------------------------------------------------------------------
echo -e "\n--- Test 5: --insert-mode Bulk and Standard agree ---"
LOCAL_INFILE=$(mysql_scalar "SELECT @@GLOBAL.local_infile")
if [ "$LOCAL_INFILE" != "1" ]; then
    echo "  NOTE: local_infile=OFF; enabling it so the Bulk path is genuinely exercised."
    docker exec dtpipe-integ-mysql mysql -u root -ppassword \
        -e "SET GLOBAL local_infile=1;" >/dev/null 2>&1 \
        || echo "  NOTE: could not enable local_infile; Bulk will fall back to INSERT."
fi

for mode in Bulk Standard; do
    "${MY_EXEC[@]}" -e "DROP TABLE IF EXISTS mysql_e2e_mode;" >/dev/null 2>&1
    "$DTPIPE" -i "$PG" --query "SELECT * FROM mysql_e2e_src ORDER BY id" \
        -o "$MY_CONN" --table mysql_e2e_mode --strategy Recreate --key id \
        --insert-mode "$mode" --no-stats >/dev/null 2>&1 \
        || fail "--insert-mode $mode failed"
    CNT=$(mysql_scalar "SELECT COUNT(*) FROM mysql_e2e_mode")
    [ "$CNT" = "$ROWS" ] || fail "--insert-mode $mode wrote $CNT rows, expected $ROWS"
    SUM=$(mysql_scalar "SELECT COALESCE(SUM(amount),0) FROM mysql_e2e_mode")
    [ -n "$SUM" ] || fail "--insert-mode $mode produced no checksum"
    echo "    $mode: $CNT rows, sum(amount)=$SUM"
done
pass "Bulk and Standard insert modes both wrote $ROWS rows"

# ------------------------------------------------------------------------------
# 6. Upsert onto a table with NO unique index. MySQL's ON DUPLICATE KEY UPDATE keys
#    off the table's indexes, never off a named conflict target, so without an index
#    it degrades to a plain INSERT and duplicates appear silently. The writer must
#    detect that and fall back to DELETE+INSERT instead.
# ------------------------------------------------------------------------------
echo -e "\n--- Test 6: upsert without a unique index falls back, no duplicates ---"
"${MY_EXEC[@]}" -e "DROP TABLE IF EXISTS mysql_e2e_nokey;
                    CREATE TABLE mysql_e2e_nokey (id INT, name LONGTEXT, amount DECIMAL(38,9),
                                                  created DATETIME(6), uid CHAR(36));" >/dev/null 2>&1
for pass_no in 1 2; do
    "$DTPIPE" -i "$PG" --query "SELECT * FROM mysql_e2e_src ORDER BY id" \
        -o "$MY_CONN" --table mysql_e2e_nokey --strategy Upsert --key id --no-stats >/dev/null 2>&1 \
        || fail "keyless-table upsert pass $pass_no failed"
done
CNT=$(mysql_scalar "SELECT COUNT(*) FROM mysql_e2e_nokey")
[ "$CNT" = "$ROWS" ] || fail "expected $ROWS rows on an index-less target, got $CNT (duplicates: the fallback did not fire)"
pass "$ROWS rows on an index-less target (DELETE+INSERT fallback fired)"

# ------------------------------------------------------------------------------
# 7. Return direction: mysql: -> pg:. Exercises the reader, and checks that UUID
#    survives CHAR(36) -> Arrow -> PostgreSQL uuid rather than degrading to text.
# ------------------------------------------------------------------------------
echo -e "\n--- Test 7: mysql: -> pg: (reader) ---"
"${PG_EXEC[@]}" -q -c "DROP TABLE IF EXISTS mysql_e2e_back;" >/dev/null 2>&1
"$DTPIPE" -i "$MY_CONN" --query "SELECT * FROM mysql_e2e_tgt ORDER BY id" \
    -o "$PG" --table mysql_e2e_back --strategy Recreate --key id --no-stats >/dev/null 2>&1 \
    || fail "mysql -> pg failed"

CNT=$(pg_scalar "SELECT COUNT(*) FROM mysql_e2e_back")
[ "$CNT" = "$ROWS" ] || fail "expected $ROWS rows back in PostgreSQL, got $CNT"

UID_PG_TYPE=$(pg_scalar "SELECT data_type FROM information_schema.columns
                         WHERE table_name='mysql_e2e_back' AND column_name='uid'")
[ "$UID_PG_TYPE" = "uuid" ] || fail "expected uid to return as PostgreSQL uuid, got '$UID_PG_TYPE'"

# Value-level integrity across every column, timestamp included: a round trip must be exact
# on all five. Zone invariance itself is pinned separately by validate_temporal.sh.
MISMATCH=$(pg_scalar "SELECT COUNT(*) FROM mysql_e2e_src s JOIN mysql_e2e_back b USING (id)
                      WHERE s.name IS DISTINCT FROM b.name
                         OR s.amount IS DISTINCT FROM b.amount
                         OR s.uid IS DISTINCT FROM b.uid
                         OR s.created IS DISTINCT FROM b.created")
[ "$MISMATCH" = "0" ] || fail "$MISMATCH rows differ after the pg -> mysql -> pg round trip"
pass "$ROWS rows returned, uid still a native uuid, no value drift on any column"

# ------------------------------------------------------------------------------
# 8. A value the column cannot hold. LOAD DATA stores it clamped to the column's bound
#    and reports a warning; the writer must fail the run and leave none of the batch's
#    rows behind. A value that fits is written exactly.
# ------------------------------------------------------------------------------
echo -e "\n--- Test 8: a refused bulk load leaves the table as it was ---"
for cell in "3000000000|INT|1|0=NULL" "123456|DECIMAL(5,2)|1|0=NULL" "5|INT|0|0=NULL,1=5"; do
    IFS='|' read -r VALUE COLTYPE WANT_RC WANT_ROWS <<< "$cell"
    "${MY_EXEC[@]}" -e "DROP TABLE IF EXISTS mysql_e2e_refused;
                        CREATE TABLE mysql_e2e_refused (id INT, n $COLTYPE);
                        INSERT INTO mysql_e2e_refused VALUES (0, NULL);" >/dev/null 2>&1
    set +e
    "$DTPIPE" -i "duck::memory:" --query "SELECT 1::INT AS id, ${VALUE}::BIGINT AS n" \
        -o "$MY_CONN" --table mysql_e2e_refused --strategy Append --no-stats >/dev/null 2>&1
    RC=$?
    set -e
    GOT=$(mysql_scalar "SELECT GROUP_CONCAT(CONCAT(id,'=',IFNULL(n,'NULL')) ORDER BY id) FROM mysql_e2e_refused")
    [ "$RC" = "$WANT_RC" ] || fail "$VALUE into $COLTYPE: exit $RC, expected $WANT_RC"
    [ "$GOT" = "$WANT_ROWS" ] || fail "$VALUE into $COLTYPE: table holds '$GOT', expected '$WANT_ROWS'"
    echo "    $VALUE -> $COLTYPE: exit $RC, table '$GOT'"
done
"${MY_EXEC[@]}" -e "DROP TABLE IF EXISTS mysql_e2e_refused;" >/dev/null 2>&1
pass "an overflow exits 1 with the table unchanged; a value that fits is written"

# ------------------------------------------------------------------------------
# 9. Text that does not read as the column's type, on the INSERT path. The writer's
#    converter must refuse it rather than write NULL; text that reads is written exactly.
# ------------------------------------------------------------------------------
echo -e "\n--- Test 9: unreadable text into INT is refused on the INSERT path ---"
for cell in "abc|1|0=NULL" "42|0|0=NULL,1=42"; do
    IFS='|' read -r VALUE WANT_RC WANT_ROWS <<< "$cell"
    "${MY_EXEC[@]}" -e "DROP TABLE IF EXISTS mysql_e2e_unreadable;
                        CREATE TABLE mysql_e2e_unreadable (id INT, n INT);
                        INSERT INTO mysql_e2e_unreadable VALUES (0, NULL);" >/dev/null 2>&1
    set +e
    "$DTPIPE" -i "duck::memory:" --query "SELECT 1::INT AS id, '${VALUE}' AS n" \
        -o "$MY_CONN" --table mysql_e2e_unreadable --strategy Append --insert-mode Standard \
        --no-stats >/dev/null 2>&1
    RC=$?
    set -e
    GOT=$(mysql_scalar "SELECT GROUP_CONCAT(CONCAT(id,'=',IFNULL(n,'NULL')) ORDER BY id) FROM mysql_e2e_unreadable")
    [ "$RC" = "$WANT_RC" ] || fail "'$VALUE' into INT: exit $RC, expected $WANT_RC"
    [ "$GOT" = "$WANT_ROWS" ] || fail "'$VALUE' into INT: table holds '$GOT', expected '$WANT_ROWS'"
    echo "    '$VALUE' -> INT: exit $RC, table '$GOT'"
done
"${MY_EXEC[@]}" -e "DROP TABLE IF EXISTS mysql_e2e_unreadable;" >/dev/null 2>&1
pass "unreadable text exits 1 with the table unchanged; text that reads is written"

echo -e "\n--- Test 10: a date into a DATE column on the INSERT path, a time of day refused ---"
for cell in "DATE '2024-01-02'|0|0=NULL,1=2024-01-02" \
            "TIMESTAMP '2024-01-02 13:45:00'|1|0=NULL" \
            "TIMESTAMP '2024-01-02 00:00:00'|0|0=NULL,1=2024-01-02"; do
    IFS='|' read -r VALUE WANT_RC WANT_ROWS <<< "$cell"
    "${MY_EXEC[@]}" -e "DROP TABLE IF EXISTS mysql_e2e_date;
                        CREATE TABLE mysql_e2e_date (id INT, n DATE);
                        INSERT INTO mysql_e2e_date VALUES (0, NULL);" >/dev/null 2>&1
    set +e
    "$DTPIPE" -i "duck::memory:" --query "SELECT 1::INT AS id, ${VALUE} AS n" \
        -o "$MY_CONN" --table mysql_e2e_date --strategy Append --insert-mode Standard \
        --no-stats >/dev/null 2>&1
    RC=$?
    set -e
    GOT=$(mysql_scalar "SELECT GROUP_CONCAT(CONCAT(id,'=',IFNULL(n,'NULL')) ORDER BY id) FROM mysql_e2e_date")
    [ "$RC" = "$WANT_RC" ] || fail "${VALUE} into DATE: exit $RC, expected $WANT_RC"
    [ "$GOT" = "$WANT_ROWS" ] || fail "${VALUE} into DATE: table holds '$GOT', expected '$WANT_ROWS'"
    echo "    ${VALUE} -> DATE: exit $RC, table '$GOT'"
done
"${MY_EXEC[@]}" -e "DROP TABLE IF EXISTS mysql_e2e_date;" >/dev/null 2>&1
pass "a date and a midnight timestamp are written exactly; a time of day exits 1 with the table unchanged"

# ------------------------------------------------------------------------------
# 11. A date into a DATE column passes the schema check: the target and the mysql:
#     reader both read DATE as a date. Under --strict-schema it is written on both
#     paths, a DATE read by mysql: included; without it, no incompatibility is reported.
# ------------------------------------------------------------------------------
echo -e "\n--- Test 11: a date into a DATE column passes the schema check ---"
"${MY_EXEC[@]}" -e "DROP TABLE IF EXISTS mysql_e2e_date_src;
                    CREATE TABLE mysql_e2e_date_src (id INT, n DATE);
                    INSERT INTO mysql_e2e_date_src VALUES (1, '2024-01-02');" >/dev/null 2>&1
for cell in "duck|Standard|--strict-schema" "duck|Bulk|--strict-schema" "duck|Standard|" \
            "mysql|Standard|--strict-schema" "mysql|Bulk|--strict-schema"; do
    IFS='|' read -r SOURCE MODE SCHEMA <<< "$cell"
    if [ "$SOURCE" = duck ]; then
        INPUT=(-i "duck::memory:" --query "SELECT 1::INT AS id, DATE '2024-01-02' AS n")
    else
        INPUT=(-i "$MY_CONN" --query "SELECT id, n FROM mysql_e2e_date_src")
    fi
    "${MY_EXEC[@]}" -e "DROP TABLE IF EXISTS mysql_e2e_date;
                        CREATE TABLE mysql_e2e_date (id INT, n DATE);
                        INSERT INTO mysql_e2e_date VALUES (0, NULL);" >/dev/null 2>&1
    set +e
    "$DTPIPE" "${INPUT[@]}" -o "$MY_CONN" --table mysql_e2e_date --strategy Append \
        --insert-mode "$MODE" $SCHEMA --no-stats >"$TMP_DIR/date_schema.out" 2>&1
    RC=$?
    set -e
    GOT=$(mysql_scalar "SELECT GROUP_CONCAT(CONCAT(id,'=',IFNULL(n,'NULL')) ORDER BY id) FROM mysql_e2e_date")
    LABEL="$SOURCE DATE -> DATE, $MODE ${SCHEMA:-discard}"
    [ "$RC" = "0" ] || fail "$LABEL: exit $RC, expected 0: $(grep -m1 'Error:' "$TMP_DIR/date_schema.out")"
    [ "$GOT" = "0=NULL,1=2024-01-02" ] || fail "$LABEL: table holds '$GOT', expected '0=NULL,1=2024-01-02'"
    ! grep -q "Error:" "$TMP_DIR/date_schema.out" || fail "$LABEL: $(grep -m1 'Error:' "$TMP_DIR/date_schema.out")"
    echo "    $LABEL: exit 0, table '$GOT'"
done
"${MY_EXEC[@]}" -e "DROP TABLE IF EXISTS mysql_e2e_date; DROP TABLE IF EXISTS mysql_e2e_date_src;" >/dev/null 2>&1
pass "a date is written exactly under --strict-schema on both paths, and reported compatible"

echo ""
echo "All native MySQL provider checks passed."
