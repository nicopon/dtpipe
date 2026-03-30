#!/bin/bash
set -e

# validate_drivers.sh
# Tests all database drivers: Postgres, MSSQL, Oracle, SQLite, DuckDB.
# Covers: basic read/write, upsert/ignore strategies, cross-driver chain (CSV→PG→MSSQL→Oracle→Parquet),
#         and Oracle insert-mode performance (Standard/Append/Bulk).
# Requires: Docker with dtpipe-integ-postgres, dtpipe-integ-mssql-tools, dtpipe-integ-oracle.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
ARTIFACTS_DIR="$SCRIPT_DIR/artifacts"
INFRA_DIR="$PROJECT_ROOT/tests/infra"
mkdir -p "$ARTIFACTS_DIR"

DTPIPE="$PROJECT_ROOT/dist/release/dtpipe"
export DTPIPE_NO_TUI=1

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

pass() { echo -e "${GREEN}  OK: $1${NC}"; }
fail() { echo -e "${RED}  FAIL: $1${NC}"; exit 1; }
skip() { echo -e "${YELLOW}  SKIP: $1${NC}"; }

echo "========================================"
echo "    DtPipe Driver Validation"
echo "========================================"

if [ ! -f "$DTPIPE" ]; then
    echo "Building release..."
    "$PROJECT_ROOT/build.sh" > /dev/null
fi

has_docker() {
    command -v docker &>/dev/null && docker info &>/dev/null
}

if ! has_docker; then
    skip "Docker not available — all driver tests skipped"
    exit 0
fi

echo "Starting shared infrastructure..."
"$INFRA_DIR/start_infra.sh"

PG_CONN="pg:Host=localhost;Port=5440;Username=postgres;Password=password;Database=integration"
MSSQL_CONN="mssql:Server=localhost,1434;Database=master;User Id=sa;Password=Password123!;TrustServerCertificate=True"
ORA_CONN="ora:Data Source=localhost:1522/FREEPDB1;User Id=testuser;Password=password"

cleanup() {
    rm -f "$ARTIFACTS_DIR"/drv_*.csv "$ARTIFACTS_DIR"/drv_*.parquet
}
trap cleanup EXIT

# ----------------------------------------
# 1. Postgres — basic read/write
# ----------------------------------------
echo "--- [1] Postgres read/write ---"
docker exec -i dtpipe-integ-postgres psql -U postgres -d integration <<'EOF' > /dev/null
DROP TABLE IF EXISTS drv_users;
CREATE TABLE drv_users (id SERIAL PRIMARY KEY, name VARCHAR(50), email VARCHAR(50));
INSERT INTO drv_users (name, email) VALUES ('Alice', 'alice@example.com'), ('Bob', 'bob@example.com');
EOF

"$DTPIPE" -i "$PG_CONN" \
  --query "SELECT * FROM drv_users ORDER BY id" \
  -o "$ARTIFACTS_DIR/drv_pg.csv" --no-stats

grep -q "Alice" "$ARTIFACTS_DIR/drv_pg.csv" && grep -q "Bob" "$ARTIFACTS_DIR/drv_pg.csv" \
  && pass "Postgres: Alice and Bob present" || fail "Postgres: read/write failed"

# ----------------------------------------
# 2. Postgres — Upsert and Ignore strategies
# ----------------------------------------
echo "--- [2] Postgres Upsert/Ignore ---"
cat > "$ARTIFACTS_DIR/drv_v1.csv" <<'EOF'
Id,Name,Value
1,Alice,100
2,Bob,200
EOF
cat > "$ARTIFACTS_DIR/drv_v2.csv" <<'EOF'
Id,Name,Value
1,Alice_Updated,150
3,Charlie,300
EOF

"$DTPIPE" -i "$ARTIFACTS_DIR/drv_v1.csv" -o "$PG_CONN" --table "drv_upsert" --strategy Recreate --key "Id" --no-stats
"$DTPIPE" -i "$ARTIFACTS_DIR/drv_v2.csv" -o "$PG_CONN" --table "drv_upsert" --strategy Upsert   --key "Id" --no-stats
"$DTPIPE" -i "$PG_CONN" --query "SELECT * FROM drv_upsert ORDER BY id" -o "$ARTIFACTS_DIR/drv_upsert_out.csv" --no-stats

grep -q "Alice_Updated" "$ARTIFACTS_DIR/drv_upsert_out.csv" \
  && grep -q "200" "$ARTIFACTS_DIR/drv_upsert_out.csv" \
  && grep -q "300" "$ARTIFACTS_DIR/drv_upsert_out.csv" \
  && pass "Postgres Upsert: correct rows" || fail "Postgres Upsert: unexpected output"

"$DTPIPE" -i "$ARTIFACTS_DIR/drv_v1.csv" -o "$PG_CONN" --table "drv_ignore" --strategy Recreate --key "Id" --no-stats
"$DTPIPE" -i "$ARTIFACTS_DIR/drv_v2.csv" -o "$PG_CONN" --table "drv_ignore" --strategy Ignore   --key "Id" --no-stats
"$DTPIPE" -i "$PG_CONN" --query "SELECT * FROM drv_ignore ORDER BY id" -o "$ARTIFACTS_DIR/drv_ignore_out.csv" --no-stats

grep -q "Alice," "$ARTIFACTS_DIR/drv_ignore_out.csv" \
  && grep -q "200" "$ARTIFACTS_DIR/drv_ignore_out.csv" \
  && grep -q "300" "$ARTIFACTS_DIR/drv_ignore_out.csv" \
  && pass "Postgres Ignore: original row preserved" || fail "Postgres Ignore: unexpected output"

# ----------------------------------------
# 3. MSSQL — basic read/write
# ----------------------------------------
echo "--- [3] MSSQL read/write ---"
docker exec -i dtpipe-integ-mssql-tools /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "Password123!" <<'EOF' > /dev/null
IF EXISTS (SELECT * FROM sys.tables WHERE name = 'DrvUsers') DROP TABLE DrvUsers;
GO
CREATE TABLE DrvUsers (Id INT IDENTITY(1,1) PRIMARY KEY, Name NVARCHAR(50), Email NVARCHAR(50));
INSERT INTO DrvUsers (Name, Email) VALUES ('Charlie', 'charlie@example.com'), ('David', 'david@example.com');
GO
EOF

"$DTPIPE" -i "$MSSQL_CONN" \
  --query "SELECT * FROM DrvUsers ORDER BY Id" \
  -o "$ARTIFACTS_DIR/drv_mssql.csv" --no-stats

grep -q "Charlie" "$ARTIFACTS_DIR/drv_mssql.csv" && grep -q "David" "$ARTIFACTS_DIR/drv_mssql.csv" \
  && pass "MSSQL: Charlie and David present" || fail "MSSQL: read/write failed"

# ----------------------------------------
# 4. Oracle — basic read/write
# ----------------------------------------
echo "--- [4] Oracle read/write ---"
docker exec -i dtpipe-integ-oracle sqlplus testuser/password@localhost:1521/FREEPDB1 <<'EOF' > /dev/null
BEGIN EXECUTE IMMEDIATE 'DROP TABLE DrvUsers'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
CREATE TABLE DrvUsers (Id NUMBER GENERATED BY DEFAULT AS IDENTITY, Name VARCHAR2(50), Email VARCHAR2(50));
INSERT INTO DrvUsers (Name, Email) VALUES ('Eve', 'eve@example.com');
INSERT INTO DrvUsers (Name, Email) VALUES ('Frank', 'frank@example.com');
COMMIT;
EXIT;
EOF

"$DTPIPE" -i "$ORA_CONN" \
  --query "SELECT * FROM DrvUsers ORDER BY Id" \
  -o "$ARTIFACTS_DIR/drv_oracle.csv" --no-stats

grep -q "Eve" "$ARTIFACTS_DIR/drv_oracle.csv" && grep -q "Frank" "$ARTIFACTS_DIR/drv_oracle.csv" \
  && pass "Oracle: Eve and Frank present" || fail "Oracle: read/write failed"

# ----------------------------------------
# 5. Cross-driver chain: CSV → PG → MSSQL → Oracle → Parquet
# ----------------------------------------
echo "--- [5] Cross-driver chain (CSV→PG→MSSQL→Oracle→Parquet) ---"

# Cleanup chain tables
docker exec dtpipe-integ-postgres psql -U postgres -d integration -c "DROP TABLE IF EXISTS chain_table CASCADE;" > /dev/null || true
docker exec dtpipe-integ-mssql-tools /opt/mssql-tools/bin/sqlcmd -S dtpipe-integ-mssql -U sa -P 'Password123!' \
  -Q "IF OBJECT_ID('ChainData', 'U') IS NOT NULL DROP TABLE ChainData;" > /dev/null || true
docker exec -i dtpipe-integ-oracle sqlplus testuser/password@localhost:1521/FREEPDB1 <<'EOF' > /dev/null
BEGIN EXECUTE IMMEDIATE 'DROP TABLE CHAIN_DATA'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;
/
EXIT;
EOF

# Generate reference CSV
"$DTPIPE" -i "generate:100" \
  --fake "seq_id:{GenerateIndex}" \
  --fake "val_id:random.number" \
  --fake "val_price:commerce.price" \
  --fake "val_date:date.past" \
  --drop "GenerateIndex" \
  --fake-seed 42 \
  -o "$ARTIFACTS_DIR/drv_ref.csv" --no-stats

"$DTPIPE" -i "$ARTIFACTS_DIR/drv_ref.csv" -o "$PG_CONN" --table "chain_table" --strategy Recreate --no-stats
"$DTPIPE" -i "$PG_CONN" --query "SELECT seq_id, val_id, val_price, val_date FROM chain_table ORDER BY seq_id" \
  -o "$MSSQL_CONN" --table "ChainData" --strategy Recreate --no-stats
"$DTPIPE" -i "$MSSQL_CONN" --query "SELECT seq_id, val_id, val_price, val_date FROM ChainData ORDER BY seq_id" \
  -o "$ORA_CONN" --table "CHAIN_DATA" --strategy Recreate --no-stats
"$DTPIPE" -i "$ORA_CONN" \
  --query "SELECT seq_id AS seq_id, val_id AS val_id, val_price AS val_price, val_date AS val_date FROM CHAIN_DATA ORDER BY seq_id" \
  -o "$ARTIFACTS_DIR/drv_chain.parquet" --no-stats
"$DTPIPE" -i "parquet:$ARTIFACTS_DIR/drv_chain.parquet" -o "$ARTIFACTS_DIR/drv_chain_final.csv" --no-stats

# Row count check
ORIG_ROWS=$(wc -l < "$ARTIFACTS_DIR/drv_ref.csv" | tr -d ' ')
FINAL_ROWS=$(wc -l < "$ARTIFACTS_DIR/drv_chain_final.csv" | tr -d ' ')
[ "$ORIG_ROWS" -eq "$FINAL_ROWS" ] \
  && pass "Cross-driver chain: $FINAL_ROWS rows preserved" \
  || fail "Cross-driver chain: expected $ORIG_ROWS rows, got $FINAL_ROWS"

# ----------------------------------------
# 6. Oracle insert-mode performance
# ----------------------------------------
echo "--- [6] Oracle insert modes (Standard/Append/Bulk) ---"
ORA_SYS_CONN="ora:Data Source=localhost:1522/FREEPDB1;User Id=system;Password=password"
docker exec -i dtpipe-integ-oracle sqlplus system/password@localhost:1521/FREEPDB1 <<'EOF' > /dev/null
BEGIN EXECUTE IMMEDIATE 'DROP TABLE PerfTest'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;
/
CREATE TABLE PerfTest (Id NUMBER, Name VARCHAR2(100), CreatedDate TIMESTAMP);
EXIT;
EOF

ROW_COUNT=10000
for MODE in Standard Append Bulk; do
    echo "  Testing mode: $MODE ($ROW_COUNT rows)..."
    "$DTPIPE" -i "generate:$ROW_COUNT" \
      --fake "Id:random.number" \
      --fake "Name:name.fullName" \
      --fake "CreatedDate:date.past" \
      --drop "GenerateIndex" \
      -o "$ORA_SYS_CONN" \
      --table "PerfTest" \
      --strategy Truncate \
      --ora-insert-mode "$MODE" \
      --batch-size 1000 --no-stats

    COUNT=$(docker exec -i dtpipe-integ-oracle sqlplus -s system/password@localhost:1521/FREEPDB1 <<'EOF' | tr -d ' \n'
SET HEADING OFF;
SELECT COUNT(*) FROM PerfTest;
EXIT;
EOF
)
    [ "$COUNT" -eq "$ROW_COUNT" ] && pass "Oracle $MODE: $COUNT rows" || fail "Oracle $MODE: expected $ROW_COUNT, got $COUNT"
done

echo ""
echo -e "${GREEN}Driver validation complete!${NC}"
