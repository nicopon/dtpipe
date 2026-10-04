#!/usr/bin/env bash
# validate_declared_decimals.sh — a decimal column the source declares a width for must reach the
# output at that width; one that declares none keeps the default scale.
#
# The ADO readers share one type resolver. A reader that dropped the declared precision wrote every
# NUMBER(p,s) / DECIMAL(p,s) with 18 decimals; Oracle also reads a small NUMBER(p,s) as a double.
# Both chains (columnar and row) are checked, because they take different roads to the same cell.
# Needs tests/infra (Oracle, MySQL) — sources lib/test_connections.sh.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/test_connections.sh
source "$SCRIPT_DIR/lib/test_connections.sh"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
DTPIPE="${DTPIPE:-$ROOT_DIR/src/DtPipe/bin/Debug/net10.0/DtPipe}"
[ -f "$DTPIPE" ] || DTPIPE="$ROOT_DIR/dist/release/dtpipe"
export DTPIPE_NO_TUI=1

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

GREEN='\033[0;32m' RED='\033[0;31m' NC='\033[0m'
FAILED=0
ok()   { echo -e "${GREEN}OK: $1${NC}"; }
fail() { echo -e "${RED}FAIL: $1${NC}"; FAILED=1; }

ORA_SYS="ora:Data Source=localhost:1522/FREEPDB1;User Id=system;Password=password;Connection Timeout=${TEST_CONNECT_TIMEOUT}"

docker exec -i dtpipe-integ-oracle sqlplus -s system/password@//localhost:1521/FREEPDB1 >/dev/null <<'SQL'
whenever sqlerror continue
drop table probe_declared_dec purge;
create table probe_declared_dec(id number(10) primary key, bare number, n385 number(38,5),
  n380 number(38,0), n102 number(10,2), n189 number(18,9), bd binary_double);
insert into probe_declared_dec values (1, 7, 12345.12345, 12345678901234567890, 12345678.12, 12345.123456789, 2.5);
commit;
SQL

docker exec -i dtpipe-integ-mysql mysql -utestuser -ppassword integration >/dev/null 2>&1 <<'SQL'
drop table if exists probe_declared_dec;
create table probe_declared_dec(id int primary key, d189 decimal(18,9), d102 decimal(10,2), d200 decimal(20,0));
insert into probe_declared_dec values (1, 12345.123456789, 12345678.12, 12345678901234567890);
SQL

MSSQL_SETUP="$TMP_DIR/mssql_setup.sql"
cat > "$MSSQL_SETUP" <<'SQL'
drop table if exists master.dbo.probe_declared_dec;
GO
create table master.dbo.probe_declared_dec(id int primary key, d189 decimal(18,9), d102 decimal(10,2), d200 decimal(20,0));
GO
insert into master.dbo.probe_declared_dec values (1, 12345.123456789, 12345678.12, 12345678901234567890);
GO
SQL
docker cp "$MSSQL_SETUP" dtpipe-integ-mssql-tools:/tmp/probe_declared_dec.sql >/dev/null
docker exec dtpipe-integ-mssql-tools bash -c '/opt/mssql-tools*/bin/sqlcmd -C -S dtpipe-integ-mssql -U sa -P "Password123!" -i /tmp/probe_declared_dec.sql' >/dev/null 2>&1

# SQL Server compiles a batch before running it, so each statement gets its own (GO): an INSERT in the
# same batch as the CREATE is compiled against the table the previous run left behind.
# expect <label> <connection> <expected CSV data row> [extra dtpipe args...]
expect() {
  local label="$1" conn="$2" want="$3"; shift 3
  local out="$TMP_DIR/out.csv"
  rm -f "$out"
  if ! "$DTPIPE" -i "$conn" --query "select * from probe_declared_dec" "$@" -o "csv:$out" --no-stats >/dev/null 2>&1; then
    fail "$label: dtpipe failed"; return
  fi
  local got
  got="$(sed -n 2p "$out" | tr -d '\r')"
  if [ "$got" = "$want" ]; then ok "$label"; else fail "$label: expected '$want', got '$got'"; fi
}

# BARE (NUMBER with no constraint) and BD (BINARY_DOUBLE) are not declared decimals: the default scale
# and a double respectively. The row chain renders the decimal without trailing zeros, so its
# undeclared cell differs by design; only the declared columns are compared there.
expect "oracle columnar" "$ORA_SYS" \
  "1,7.000000000000000000,12345.12345,12345678901234567890,12345678.12,12345.123456789,2.5"
expect "oracle row" "$ORA_SYS" \
  "1,7,12345.12345,12345678901234567890,12345678.12,12345.123456789,2.5" --compute "ID:row.ID"

expect "sqlserver columnar" "$MSSQL" "1,12345.123456789,12345678.12,12345678901234567890"
expect "sqlserver row" "$MSSQL" "1,12345.123456789,12345678.12,12345678901234567890" --compute "id:row.id"

expect "mysql columnar" "$MYSQL" "1,12345.123456789,12345678.12,12345678901234567890"
expect "mysql row" "$MYSQL" "1,12345.123456789,12345678.12,12345678901234567890" --compute "id:row.id"

# Write side: a column declared DECIMAL(18,9) is created at that width on every fixed-point engine and
# reads back exact. SQLite stores a float whatever is declared, so it is not part of this check.
WRITE_Q="select 1 as id, 12345.123456789::DECIMAL(18,9) as d, 0.000000001::DECIMAL(18,9) as tiny"
for target in "postgres|$PG" "mysql|$MYSQL" "sqlserver|$MSSQL" "oracle|$ORA" "duckdb|duck:$TMP_DIR/w.duckdb"; do
  label="${target%%|*}"; conn="${target#*|}"
  out="$TMP_DIR/w_$label.csv"
  if ! "$DTPIPE" -i "duck::memory:" --query "$WRITE_Q" -o "$conn" --table probe_declared_dec_w --strategy Recreate --no-stats >/dev/null 2>&1 \
     || ! "$DTPIPE" -i "$conn" --query "select * from probe_declared_dec_w" -o "csv:$out" --no-stats >/dev/null 2>&1; then
    fail "$label write: dtpipe failed"; continue
  fi
  got="$(sed -n 2p "$out" | tr -d '\r')"
  want="1,12345.123456789,0.000000001"
  if [ "$got" = "$want" ]; then ok "$label write keeps the declared width"; else fail "$label write: expected '$want', got '$got'"; fi
done

# Through a DAG memory channel: a decimal that declared no width creates the engine's default, as it does
# without a channel, and one that declared (10,2) keeps it. Read from MySQL's catalog, where the default
# (38,9) tells itself apart from the (38,18) an Arrow schema would otherwise report.
mysql_shape() {
  docker exec dtpipe-integ-mysql mysql -N -utestuser -ppassword integration -e \
    "select concat(numeric_precision, ',', numeric_scale) from information_schema.columns where table_schema='integration' and table_name='$1' and column_name='amount'" 2>/dev/null
}
"$DTPIPE" -i "generate:2" --fake "amount:finance.amount" --fake-seed 1 --alias src --from src \
  -o "$MYSQL" --table probe_declared_dec_chan_none --strategy Recreate --no-stats >/dev/null 2>&1 || fail "channel undeclared: dtpipe failed"
"$DTPIPE" -i "duck::memory:" --query "select 1.25::DECIMAL(10,2) as amount" --alias src --from src \
  -o "$MYSQL" --table probe_declared_dec_chan_decl --strategy Recreate --no-stats >/dev/null 2>&1 || fail "channel declared: dtpipe failed"
got="$(mysql_shape probe_declared_dec_chan_none)"
if [ "$got" = "38,9" ]; then ok "channel: undeclared decimal takes the engine default"; else fail "channel undeclared: expected '38,9', got '$got'"; fi
got="$(mysql_shape probe_declared_dec_chan_decl)"
if [ "$got" = "10,2" ]; then ok "channel: declared decimal keeps its width"; else fail "channel declared: expected '10,2', got '$got'"; fi
docker exec dtpipe-integ-mysql mysql -utestuser -ppassword integration -e "drop table if exists probe_declared_dec_chan_none; drop table if exists probe_declared_dec_chan_decl" >/dev/null 2>&1 || true

docker exec dtpipe-integ-postgres psql -U postgres -d integration -c "drop table if exists probe_declared_dec_w" >/dev/null 2>&1 || true
docker exec dtpipe-integ-mysql mysql -utestuser -ppassword integration -e "drop table if exists probe_declared_dec_w" >/dev/null 2>&1 || true
docker exec dtpipe-integ-mssql-tools bash -c '/opt/mssql-tools*/bin/sqlcmd -C -S dtpipe-integ-mssql -U sa -P "Password123!" -Q "drop table if exists master.dbo.probe_declared_dec_w; drop table if exists master.dbo.probe_declared_dec"' >/dev/null 2>&1 || true
docker exec -i dtpipe-integ-oracle sqlplus -s system/password@//localhost:1521/FREEPDB1 >/dev/null 2>&1 <<'SQL' || true
drop table testuser.probe_declared_dec_w purge;
SQL

docker exec -i dtpipe-integ-oracle sqlplus -s system/password@//localhost:1521/FREEPDB1 >/dev/null <<'SQL' || true
drop table probe_declared_dec purge;
SQL
docker exec dtpipe-integ-mysql mysql -utestuser -ppassword integration -e "drop table if exists probe_declared_dec" >/dev/null 2>&1 || true

exit "$FAILED"
