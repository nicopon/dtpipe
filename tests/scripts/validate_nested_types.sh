#!/bin/bash
set -e

# validate_nested_types.sh — collection columns (Postgres arrays, DuckDB LIST) end to end.
#
# The point of this suite is the difference between "unsupported" and "wrong". Every case here
# either moves the data intact or fails with a non-zero exit: a column that silently becomes
# whitespace, a type name, or an empty field is a failure, not a limitation.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Test infrastructure endpoints. Sourcing this is what declares that this script
# needs tests/infra running (see lib/test_connections.sh).
# shellcheck source=lib/test_connections.sh
source "$SCRIPT_DIR/lib/test_connections.sh"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
ARTIFACTS_DIR="$SCRIPT_DIR/artifacts"
DTPIPE="$ROOT_DIR/dist/release/dtpipe"
export DTPIPE_NO_TUI=1

GREEN='\033[0;32m'; RED='\033[0;31m'; NC='\033[0m'
FAILED=0

mkdir -p "$ARTIFACTS_DIR"

pass() { echo -e "  ${GREEN}OK${NC}: $1"; }
fail() { echo -e "  ${RED}FAIL${NC}: $1"; FAILED=1; }

# CSV lines end in CRLF; compare on the text, not on the line terminator.
expect_eq() {
    local got="${2%$'\r'}"
    if [ "$got" = "$3" ]; then pass "$1"; else fail "$1 (expected '$3', got '$got')"; fi
}

echo "===================================================="
echo "   Nested / collection column validation"
echo "===================================================="

if [ ! -x "$DTPIPE" ]; then echo "Binary not found: $DTPIPE — run ./build.sh"; exit 1; fi

# ── 1. DuckDB LIST → CSV ────────────────────────────────────────────────────────
echo ""
echo "--- [1] DuckDB LIST -> CSV ---"
OUT="$ARTIFACTS_DIR/nested_duck.csv"
rm -f "$OUT"
"$DTPIPE" -i "duck::memory:" \
    --duck-init "CREATE TABLE t AS SELECT 1 AS id, [10,20,30]::INTEGER[] AS tags;" \
    -q "SELECT * FROM t" -o "$OUT" --no-stats > /dev/null 2>&1
expect_eq "LIST rendered as JSON, not as a CLR type name" "$(tail -1 "$OUT")" '1,"[10,20,30]"'

# ── 2. DuckDB LIST → Parquet, read back by DuckDB itself ────────────────────────
echo ""
echo "--- [2] DuckDB LIST -> Parquet -> read back ---"
PQ="$ARTIFACTS_DIR/nested_duck.parquet"
RT="$ARTIFACTS_DIR/nested_duck_rt.csv"
rm -f "$PQ" "$RT"
"$DTPIPE" -i "duck::memory:" \
    --duck-init "CREATE TABLE t AS SELECT * FROM (VALUES (1,[10,20,30]::INTEGER[]),(2,NULL),(3,[]::INTEGER[]),(4,[7]::INTEGER[])) v(id,tags);" \
    -q "SELECT * FROM t ORDER BY id" -o "$PQ" --no-stats > /dev/null 2>&1
"$DTPIPE" -i "duck::memory:" \
    --duck-init "CREATE TABLE r AS SELECT id, to_json(tags)::VARCHAR AS j, tags IS NULL AS isnull, len(tags) AS n FROM read_parquet('$PQ');" \
    -q "SELECT * FROM r ORDER BY id" -o "$RT" --no-stats > /dev/null 2>&1

expect_eq "values survive the round trip"        "$(sed -n '2p' "$RT")" '1,"[10,20,30]",false,3'
# A NULL list and an empty list are distinct in Parquet's three-level encoding; collapsing
# them is the classic definition-level mistake, so both are asserted.
expect_eq "NULL list stays NULL"                 "$(sed -n '3p' "$RT")" '2,,true,'
expect_eq "empty list stays empty, not NULL"     "$(sed -n '4p' "$RT")" '3,[],false,0'
expect_eq "single-element list"                  "$(sed -n '5p' "$RT")" '4,[7],false,1'

# ── 3. Postgres int[] → CSV ─────────────────────────────────────────────────────
echo ""
echo "--- [3] Postgres int[] -> CSV ---"
docker exec dtpipe-integ-postgres psql -U postgres -d integration -c \
    "DROP TABLE IF EXISTS nested_t; CREATE TABLE nested_t(id int, tags int[], words text[]);
     INSERT INTO nested_t VALUES (1,'{10,20,30}','{a,b}'),(2,NULL,NULL),(3,'{}','{}'),(4,'{1,NULL,3}','{z}');" \
    > /dev/null 2>&1

OUT="$ARTIFACTS_DIR/nested_pg.csv"
rm -f "$OUT"
"$DTPIPE" -i "$PG" -q "SELECT id, tags FROM nested_t ORDER BY id" -o "$OUT" --no-stats > /dev/null 2>&1
expect_eq "int[] rendered as JSON"      "$(sed -n '2p' "$OUT")" '1,"[10,20,30]"'
expect_eq "NULL array is an empty field" "$(sed -n '3p' "$OUT")" '2,'
expect_eq "empty array is not NULL"      "$(sed -n '4p' "$OUT")" '3,[]'

# ── 4. Postgres int[] → Parquet, including a NULL element ───────────────────────
#
# Read back TWICE, and the second half is the point. Reading only through DuckDB's read_parquet
# proves the file is well formed and says nothing about dtpipe's own Parquet reader — which could
# not read back a single list file dtpipe had written, for as long as this section existed.
echo ""
echo "--- [4] Postgres int[] -> Parquet -> read back ---"
PQ="$ARTIFACTS_DIR/nested_pg.parquet"
RT="$ARTIFACTS_DIR/nested_pg_rt.csv"
RT_SELF="$ARTIFACTS_DIR/nested_pg_rt_self.csv"
rm -f "$PQ" "$RT" "$RT_SELF"
"$DTPIPE" -i "$PG" -q "SELECT id, tags FROM nested_t ORDER BY id" -o "$PQ" --no-stats > /dev/null 2>&1
"$DTPIPE" -i "duck::memory:" \
    --duck-init "CREATE TABLE r AS SELECT id, to_json(tags)::VARCHAR AS j FROM read_parquet('$PQ');" \
    -q "SELECT * FROM r ORDER BY id" -o "$RT" --no-stats > /dev/null 2>&1
expect_eq "values survive Postgres -> Parquet"   "$(sed -n '2p' "$RT")" '1,"[10,20,30]"'
expect_eq "NULL array stays NULL"                "$(sed -n '3p' "$RT")" '2,'
expect_eq "empty array stays empty"              "$(sed -n '4p' "$RT")" '3,[]'
# Exercises the "element is NULL" definition level, which the DuckDB driver cannot produce.
expect_eq "NULL *inside* a list keeps its slot"  "$(sed -n '5p' "$RT")" '4,"[1,null,3]"'

"$DTPIPE" -i "$PQ" -o "$RT_SELF" --no-stats > /dev/null 2>&1
expect_eq "dtpipe reads its own Parquet list"    "$(sed -n '2p' "$RT_SELF")" '1,"[10,20,30]"'
expect_eq "and a NULL list, through its reader"  "$(sed -n '3p' "$RT_SELF")" '2,'
expect_eq "and an empty one, still distinct"     "$(sed -n '4p' "$RT_SELF")" '3,[]'
expect_eq "and a NULL element, in its slot"      "$(sed -n '5p' "$RT_SELF")" '4,"[1,null,3]"'

# ── 4bis. DuckDB STRUCT and MAP, which have no Arrow form ───────────────────────
#
# A LIST maps to an Arrow ListType and must stay one; a STRUCT and a MAP arrive from the driver as
# a Dictionary, which has none — the reader declares those as text and renders the value as JSON,
# the same answer the writers give a composite a target cannot hold. Before that, building the
# schema threw before a single row was read, on types the --sql processor handles.
echo ""
echo "--- [4bis] DuckDB STRUCT / MAP / LIST through the duck: reader ---"
RT="$ARTIFACTS_DIR/nested_duck_reader.csv"

rm -f "$RT"
"$DTPIPE" -i "duck::memory:" -q "SELECT 1 AS id, {'sev':'info','n':2} AS p" -o "$RT" --no-stats > /dev/null 2>&1
expect_eq "a STRUCT reads back as JSON" "$(sed -n '2p' "$RT")" '1,"{""sev"":""info"",""n"":2}"'

rm -f "$RT"
"$DTPIPE" -i "duck::memory:" -q "SELECT 1 AS id, MAP{'a':1} AS m" -o "$RT" --no-stats > /dev/null 2>&1
expect_eq "a MAP reads back as JSON"    "$(sed -n '2p' "$RT")" '1,"{""a"":1}"'

rm -f "$RT"
"$DTPIPE" -i "duck::memory:" -q "SELECT 1 AS id, [10,20,30]::INTEGER[] AS vals" -o "$RT" --no-stats > /dev/null 2>&1
expect_eq "a LIST stays a list"         "$(sed -n '2p' "$RT")" '1,"[10,20,30]"'

# ── 5. Unsupported combinations must fail, not corrupt ──────────────────────────
echo ""
echo "--- [5] Unsupported combinations fail closed ---"
if "$DTPIPE" -i "$PG" -q "SELECT id, words FROM nested_t ORDER BY id" \
        -o "$ARTIFACTS_DIR/nested_text.parquet" --no-stats > /dev/null 2>&1; then
    fail "a Parquet list of text should be refused while definition levels are unavailable for it"
else
    pass "Parquet list of text refused with a non-zero exit"
fi

# text[] has no such limitation on the CSV side.
OUT="$ARTIFACTS_DIR/nested_text.csv"
rm -f "$OUT"
"$DTPIPE" -i "$PG" -q "SELECT id, words FROM nested_t ORDER BY id" -o "$OUT" --no-stats > /dev/null 2>&1
expect_eq "text[] still renders to CSV" "$(sed -n '2p' "$OUT")" '1,"[""a"",""b""]"'

# A Postgres type with no text rendering must be refused rather than written as a type name.
if "$DTPIPE" -i "$PG" -q "SELECT '(1,2)'::point AS p" \
        -o "$ARTIFACTS_DIR/nested_point.csv" --no-stats > /dev/null 2>&1; then
    fail "a type dtpipe cannot represent should not produce a file"
else
    pass "unrepresentable type refused with a non-zero exit"
fi

docker exec dtpipe-integ-postgres psql -U postgres -d integration \
    -c "DROP TABLE IF EXISTS nested_t;" > /dev/null 2>&1

echo ""
if [ $FAILED -ne 0 ]; then
    echo -e "${RED}Nested type validation FAILED.${NC}"
    exit 1
fi
echo -e "${GREEN}All nested type checks passed.${NC}"
