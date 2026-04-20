#!/bin/bash
set -e

# smoke.sh — Golden smoke test (vicious edge cases + all DB drivers + 1M volume)
# Requires: Docker with dtpipe-integ-postgres, dtpipe-integ-mssql-tools, dtpipe-integ-oracle.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
INFRA_DIR="$PROJECT_ROOT/tests/infra"
INPUT_DIR="$SCRIPT_DIR/artifacts"
mkdir -p "$INPUT_DIR"

DTPIPE="$PROJECT_ROOT/dist/release/dtpipe"
export DTPIPE_NO_TUI=1

if [ ! -f "$DTPIPE" ]; then
    echo "Error: Binary not found at $DTPIPE. Run ./build.sh first."
    exit 1
fi

echo "=========================================="
echo "    DtPipe GOLDEN SMOKE TEST (VICIOUS)    "
echo "=========================================="

cleanup() {
    rm -f "$INPUT_DIR"/vicious_source.csv \
          "$INPUT_DIR"/vicious.parquet \
          "$INPUT_DIR"/vicious.db \
          "$INPUT_DIR"/query.sql \
          "$INPUT_DIR"/result_*.csv \
          "$INPUT_DIR"/high_volume.csv \
          "$INPUT_DIR"/high_volume.parquet \
          "$INPUT_DIR"/high_volume.db \
          "$INPUT_DIR"/composite_*.csv \
          "$INPUT_DIR"/comp_*.csv \
          "$INPUT_DIR"/vicious_inc.csv \
          "$INPUT_DIR"/composite.db \
          "$INPUT_DIR"/composite.duckdb
    # Drop typed tables from previous runs: Recreate preserves existing schema, so a stale
    # table with old column types must be purged when column types change across runs.
    docker exec dtpipe-integ-postgres psql -U postgres -d integration \
        -c "DROP TABLE IF EXISTS voldata, comp_users" 2>/dev/null || true
    docker exec dtpipe-integ-mssql-tools /opt/mssql-tools/bin/sqlcmd \
        -S localhost -U sa -P 'Password123!' \
        -Q "IF OBJECT_ID('VolData') IS NOT NULL DROP TABLE VolData; IF OBJECT_ID('CompUsers') IS NOT NULL DROP TABLE CompUsers;" \
        2>/dev/null || true
    printf 'DROP TABLE VOL_DATA PURGE;\nDROP TABLE COMP_USERS PURGE;\nEXIT\n' | \
        docker exec -i dtpipe-integ-oracle sqlplus -s system/password@FREEPDB1 2>/dev/null || true
}
cleanup

# ----------------------------------------
# 1. Infrastructure
# ----------------------------------------
echo "[1/9] Starting Infrastructure (Postgres, MSSQL, Oracle)..."
"$INFRA_DIR/start_infra.sh"

PG_CONN="pg:Host=localhost;Port=5440;Database=integration;Username=postgres;Password=password"
MSSQL_CONN="mssql:Server=localhost,1434;Database=master;User Id=sa;Password=Password123!;TrustServerCertificate=True"
ORA_CONN="ora:Data Source=localhost:1522/FREEPDB1;User Id=system;Password=password;Pooling=false"

# ----------------------------------------
# 2. Generate VICIOUS source (CSV edge cases)
# ----------------------------------------
echo "[2/9] Generating Vicious Source Data..."
cat > "$INPUT_DIR/vicious_source.csv" <<'EOF'
Id,Name,Email,Details,Amount,IsActive,JoinDate
1,Alice,alice@example.com,"Standard User",100.50,true,2023-01-01
2,Bob O'Connor,bob@example.com,"Contains, Commas",200.00,false,2023-02-15
3,Charlie "The Boss",charlie@example.com,"Contains ""Quotes""",300.99,true,2023-03-20
4,D'Artagnan,darian@example.com,NULL,0,false,
5,Élise,elise@example.com,"UTF-8: Ümlaut",-50.5,true,2023-12-31
6,Frank,,Empty Email,,1,,2024-01-01
7,Robert'); DROP TABLE Students;--,hack@example.com,SQL Injection Attempt,9999,true,2023-01-01
EOF
echo "      Generated 7 rows with edge cases."

# ----------------------------------------
# 3. High volume (1M rows on all DBs)
# ----------------------------------------
echo "[3/9] Running High Volume Test (1,000,000 rows on all DBs)..."
echo "Id,Guid,Number" > "$INPUT_DIR/high_volume.csv"
awk 'BEGIN { for(i=1; i<=1000000; i++) print i ",uuid-" i "," rand() }' >> "$INPUT_DIR/high_volume.csv"

echo "      Converting CSV → Parquet..."
"$DTPIPE" -i "$INPUT_DIR/high_volume.csv" \
  --column-types "Id:int64" \
  -o "parquet:$INPUT_DIR/high_volume.parquet" --no-stats > /dev/null

"$DTPIPE" -i "parquet:$INPUT_DIR/high_volume.parquet" -o "$INPUT_DIR/result_vol.csv" --no-stats > /dev/null
VOL_LINES=$(wc -l < "$INPUT_DIR/result_vol.csv" | tr -d ' ')
[ "$VOL_LINES" = "1000001" ] && echo "✅ Parquet Volume Verified (1M rows)." \
  || { echo "❌ FAILED: Parquet Volume Mismatch (Expected 1000001, Got $VOL_LINES)"; exit 1; }
rm "$INPUT_DIR"/result_vol.csv

# SQLite (1M)
echo "      SQLite 1M..."
"$DTPIPE" -i "parquet:$INPUT_DIR/high_volume.parquet" -o "sqlite:$INPUT_DIR/high_volume.db" \
  --table "vol_data" --strategy Recreate --no-stats > /dev/null
"$DTPIPE" -i "sqlite:$INPUT_DIR/high_volume.db" --query "SELECT COUNT(*) FROM vol_data" \
  -o csv --no-stats > "$INPUT_DIR/result_vol_sqlite.csv"
COUNT=$(tail -n 1 "$INPUT_DIR/result_vol_sqlite.csv" | tr -d '\r')
[ "$COUNT" = "1000000" ] && echo "✅ SQLite 1M Load Verified." \
  || { echo "❌ FAILED: SQLite Volume Mismatch ($COUNT)"; exit 1; }

# Postgres (1M)
echo "      Postgres 1M..."
"$DTPIPE" -i "parquet:$INPUT_DIR/high_volume.parquet" -o "$PG_CONN" \
  --table "voldata" --strategy Recreate --no-stats > /dev/null
"$DTPIPE" -i "$PG_CONN" --query "SELECT COUNT(*) FROM voldata" \
  -o csv --no-stats > "$INPUT_DIR/result_vol_pg.csv"
COUNT=$(tail -n 1 "$INPUT_DIR/result_vol_pg.csv" | tr -d '\r')
[ "$COUNT" = "1000000" ] && echo "✅ Postgres 1M Load Verified." \
  || { echo "❌ FAILED: PG Volume Mismatch ($COUNT)"; exit 1; }

# MSSQL (1M)
echo "      MSSQL 1M..."
MSSQL_CONN_EXT="mssql:Server=localhost,1434;Database=master;User Id=sa;Password=Password123!;TrustServerCertificate=True;Encrypt=False;MultipleActiveResultSets=True"
"$DTPIPE" -i "parquet:$INPUT_DIR/high_volume.parquet" -o "$MSSQL_CONN_EXT" \
  --table "VolData" --strategy Recreate --no-stats > /dev/null
"$DTPIPE" -i "$MSSQL_CONN" --query "SELECT COUNT(*) FROM VolData" \
  -o csv --no-stats > "$INPUT_DIR/result_vol_mssql.csv"
COUNT=$(tail -n 1 "$INPUT_DIR/result_vol_mssql.csv" | tr -d '\r')
[ "$COUNT" = "1000000" ] && echo "✅ MSSQL 1M Load Verified." \
  || { echo "❌ FAILED: MSSQL Volume Mismatch ($COUNT)"; exit 1; }

# Oracle (1M)
echo "      Oracle 1M..."
"$DTPIPE" -i "parquet:$INPUT_DIR/high_volume.parquet" -o "$ORA_CONN" \
  --table "VOL_DATA" --strategy Recreate --no-stats > /dev/null
"$DTPIPE" -i "$ORA_CONN" --query "SELECT COUNT(*) FROM VOL_DATA" \
  -o csv --no-stats > "$INPUT_DIR/result_vol_ora.csv"
COUNT=$(tail -n 1 "$INPUT_DIR/result_vol_ora.csv" | tr -d '\r')
[ "$COUNT" = "1000000" ] && echo "✅ Oracle 1M Load Verified." \
  || { echo "❌ FAILED: Oracle Volume Mismatch ($COUNT)"; exit 1; }

rm -f "$INPUT_DIR"/high_volume.* "$INPUT_DIR"/result_vol_*.csv

# ----------------------------------------
# 4. Pipeline 1: CSV → Parquet (anonymize + mask)
# ----------------------------------------
echo "[4/9] Pipeline 1: CSV → Parquet (Anonymize & Mask)..."
"$DTPIPE" -i "$INPUT_DIR/vicious_source.csv" -o "parquet:$INPUT_DIR/vicious.parquet" \
  --fake "Email:internet.email" \
  --fake-seed-column "Id" \
  --null "Details" \
  --mask "Name:?************" \
  --no-stats > /dev/null

"$DTPIPE" -i "parquet:$INPUT_DIR/vicious.parquet" -o "$INPUT_DIR/result_step1.csv" --no-stats > /dev/null
grep -q "alice@example.com" "$INPUT_DIR/result_step1.csv" \
  && { echo "❌ FAILED: Anonymization failed"; exit 1; }
grep -q "@" "$INPUT_DIR/result_step1.csv" \
  || { echo "❌ FAILED: Faking failed"; exit 1; }
echo "✅ Anonymization Verified."

# ----------------------------------------
# 5. Pipeline 2: Parquet → SQLite
# ----------------------------------------
echo "[5/9] Pipeline 2: Parquet → SQLite..."
"$DTPIPE" -i "parquet:$INPUT_DIR/vicious.parquet" -o "sqlite:$INPUT_DIR/vicious.db" \
  --table "users" --strategy Recreate --no-stats > /dev/null
"$DTPIPE" -i "sqlite:$INPUT_DIR/vicious.db" --query "SELECT COUNT(*) FROM users" \
  -o csv --no-stats > "$INPUT_DIR/result_count.csv"
COUNT=$(tail -n 1 "$INPUT_DIR/result_count.csv" | tr -d '\r')
[ "$COUNT" = "7" ] || { echo "❌ FAILED: SQLite Count Mismatch ($COUNT)"; exit 1; }
echo "✅ SQLite Count Verified."
rm "$INPUT_DIR"/result_count.csv

# ----------------------------------------
# 6. Pipeline 3: SQLite → Postgres (Upsert)
# ----------------------------------------
echo "[6/9] Pipeline 3: SQLite → Postgres (Upsert)..."
"$DTPIPE" -i "sqlite:$INPUT_DIR/vicious.db" --query "SELECT * FROM users" \
  -o "$PG_CONN" --table "vicious_users" --strategy Recreate --key "Id" --no-stats > /dev/null

cat > "$INPUT_DIR/vicious_inc.csv" <<'EOF'
Id,Name,Email,Details,Amount,IsActive,JoinDate
1,Alice Updated,newalice@example.com,Updated,999,true,2023-01-01
8,New User,new@example.com,New,0,true,2024-01-01
EOF

"$DTPIPE" -i "$INPUT_DIR/vicious_inc.csv" -o "$PG_CONN" \
  --table "vicious_users" --strategy Upsert --key "Id" --no-stats > /dev/null
"$DTPIPE" -i "$PG_CONN" --query "SELECT name FROM vicious_users WHERE id = '1'" \
  -o "$INPUT_DIR/result_pg_check.csv" --no-stats > /dev/null
grep -q "Alice Updated" "$INPUT_DIR/result_pg_check.csv" \
  || { echo "❌ FAILED: PG Upsert failed"; exit 1; }
echo "✅ Postgres Upsert Verified."

# ----------------------------------------
# 7. Query from file
# ----------------------------------------
echo "[7/9] Testing Query from File..."
echo "SELECT * FROM vicious_users WHERE CAST(NULLIF(amount, '') AS NUMERIC) > 100 ORDER BY id" \
  > "$INPUT_DIR/query.sql"
"$DTPIPE" -i "$PG_CONN" --query "$INPUT_DIR/query.sql" \
  -o "$INPUT_DIR/result_final.csv" --no-stats > /dev/null
LINE_COUNT=$(wc -l < "$INPUT_DIR/result_final.csv" | tr -d ' ')
[ "$LINE_COUNT" = "5" ] || { echo "❌ FAILED: Query File Mismatch (Expected 5, Got $LINE_COUNT)"; exit 1; }
echo "✅ Query File Verified."

# ----------------------------------------
# 8. SQL Server
# ----------------------------------------
echo "[8/9] Testing SQL Server..."
"$DTPIPE" -i "$INPUT_DIR/vicious_source.csv" -o "$MSSQL_CONN" \
  --table "ViciousUsers" --strategy Recreate --no-stats > /dev/null
"$DTPIPE" -i "$MSSQL_CONN" --query "SELECT COUNT(*) FROM ViciousUsers" \
  -o csv --no-stats > "$INPUT_DIR/result_mssql.csv"
COUNT=$(tail -n 1 "$INPUT_DIR/result_mssql.csv" | tr -d '\r')
[ "$COUNT" = "7" ] || { echo "❌ FAILED: MSSQL Count Mismatch (Expected 7, Got $COUNT)"; exit 1; }
echo "✅ SQL Server Initial Load Verified."

"$DTPIPE" -i "$INPUT_DIR/vicious_inc.csv" -o "$MSSQL_CONN" \
  --table "ViciousUsers" --strategy Upsert --key "Id" --no-stats > /dev/null
"$DTPIPE" -i "$MSSQL_CONN" --query "SELECT Name FROM ViciousUsers WHERE Id = '1'" \
  -o "$INPUT_DIR/result_mssql_check.csv" --no-stats > /dev/null
grep -q "Alice Updated" "$INPUT_DIR/result_mssql_check.csv" \
  || { echo "❌ FAILED: MSSQL Upsert failed"; exit 1; }
echo "✅ SQL Server Upsert Verified."

# ----------------------------------------
# 9. Oracle
# ----------------------------------------
echo "[9/9] Testing Oracle..."
"$DTPIPE" -i "parquet:$INPUT_DIR/vicious.parquet" -o "$ORA_CONN" \
  --table "VICIOUS_USERS" --strategy Recreate --no-stats > /dev/null
"$DTPIPE" -i "$ORA_CONN" --query "SELECT COUNT(*) FROM VICIOUS_USERS" \
  -o csv --no-stats > "$INPUT_DIR/result_ora.csv"
COUNT=$(tail -n 1 "$INPUT_DIR/result_ora.csv" | tr -d '\r')
[ "$COUNT" = "7" ] || { echo "❌ FAILED: Oracle Count Mismatch (Expected 7, Got $COUNT)"; exit 1; }
echo "✅ Oracle Initial Load Verified."

"$DTPIPE" -i "$INPUT_DIR/vicious_inc.csv" -o "$ORA_CONN" \
  --table "VICIOUS_USERS" --strategy Upsert --key "Id" --no-stats > /dev/null
"$DTPIPE" -i "$ORA_CONN" --query "SELECT Name FROM VICIOUS_USERS WHERE Id = 1" \
  -o "$INPUT_DIR/result_ora_check.csv" --no-stats > /dev/null
grep -q "Alice Updated" "$INPUT_DIR/result_ora_check.csv" \
  || { echo "❌ FAILED: Oracle Upsert failed"; exit 1; }
echo "✅ Oracle Upsert Verified."

# ----------------------------------------
# 10. Composite key upsert (all DBs)
# ----------------------------------------
echo "[10/10] Composite Key Upsert (Region, Branch)..."

cat > "$INPUT_DIR/composite_source.csv" <<'EOF'
Region,Branch,Target
EU,Paris,100
EU,Berlin,200
US,NY,500
EOF
cat > "$INPUT_DIR/composite_inc.csv" <<'EOF'
Region,Branch,Target
EU,Paris,150
EU,Madrid,300
EOF

verify_composite() {
    local file=$1 name=$2
    if grep -q "EU,Paris,150" "$file" && grep -q "EU,Madrid,300" "$file" && grep -q "EU,Berlin,200" "$file"; then
        echo "✅ $name Composite Verified."
    else
        echo "❌ FAILED: $name Composite Key Test Failed"
        cat "$file"
        exit 1
    fi
}

echo "      DuckDB..."
"$DTPIPE" -i "$INPUT_DIR/composite_source.csv" --column-types "Target:int32" \
  -o "duck:$INPUT_DIR/composite.duckdb" \
  --table "comp_users" --strategy Recreate --key "Region,Branch" --no-stats > /dev/null
"$DTPIPE" -i "$INPUT_DIR/composite_inc.csv" --column-types "Target:int32" \
  -o "duck:$INPUT_DIR/composite.duckdb" \
  --table "comp_users" --strategy Upsert   --key "Region,Branch" --no-stats > /dev/null
"$DTPIPE" -i "duck:$INPUT_DIR/composite.duckdb" \
  --query "SELECT * FROM comp_users ORDER BY Region, Branch" \
  -o "$INPUT_DIR/comp_duck.csv" --no-stats > /dev/null
verify_composite "$INPUT_DIR/comp_duck.csv" "DuckDB"

echo "      SQLite..."
"$DTPIPE" -i "$INPUT_DIR/composite_source.csv" --column-types "Target:int32" \
  -o "sqlite:$INPUT_DIR/composite.db" \
  --table "comp_users" --strategy Recreate --key "Region,Branch" --no-stats > /dev/null
"$DTPIPE" -i "$INPUT_DIR/composite_inc.csv" --column-types "Target:int32" \
  -o "sqlite:$INPUT_DIR/composite.db" \
  --table "comp_users" --strategy Upsert   --key "Region,Branch" --no-stats > /dev/null
"$DTPIPE" -i "sqlite:$INPUT_DIR/composite.db" \
  --query "SELECT * FROM comp_users ORDER BY Region, Branch" \
  -o "$INPUT_DIR/comp_sqlite.csv" --no-stats > /dev/null
verify_composite "$INPUT_DIR/comp_sqlite.csv" "SQLite"

echo "      Postgres..."
"$DTPIPE" -i "$INPUT_DIR/composite_source.csv" --column-types "Target:int32" \
  -o "$PG_CONN" \
  --table "comp_users" --strategy Recreate --key "Region,Branch" --no-stats > /dev/null
"$DTPIPE" -i "$INPUT_DIR/composite_inc.csv" --column-types "Target:int32" \
  -o "$PG_CONN" \
  --table "comp_users" --strategy Upsert   --key "Region,Branch" --no-stats > /dev/null
"$DTPIPE" -i "$PG_CONN" \
  --query "SELECT * FROM comp_users ORDER BY region, branch" \
  -o "$INPUT_DIR/comp_pg.csv" --no-stats > /dev/null
verify_composite "$INPUT_DIR/comp_pg.csv" "Postgres"

echo "      MSSQL..."
"$DTPIPE" -i "$INPUT_DIR/composite_source.csv" --column-types "Target:int32" \
  -o "$MSSQL_CONN" \
  --table "CompUsers" --strategy Recreate --key "Region,Branch" --no-stats > /dev/null
"$DTPIPE" -i "$INPUT_DIR/composite_inc.csv" --column-types "Target:int32" \
  -o "$MSSQL_CONN" \
  --table "CompUsers" --strategy Upsert   --key "Region,Branch" --no-stats > /dev/null
"$DTPIPE" -i "$MSSQL_CONN" \
  --query "SELECT * FROM CompUsers ORDER BY Region, Branch" \
  -o "$INPUT_DIR/comp_mssql.csv" --no-stats > /dev/null
verify_composite "$INPUT_DIR/comp_mssql.csv" "MSSQL"

echo "      Oracle..."
"$DTPIPE" -i "$INPUT_DIR/composite_source.csv" --column-types "Target:int32" \
  -o "$ORA_CONN" \
  --table "COMP_USERS" --strategy Recreate --key "Region,Branch" --no-stats > /dev/null
"$DTPIPE" -i "$INPUT_DIR/composite_inc.csv" --column-types "Target:int32" \
  -o "$ORA_CONN" \
  --table "COMP_USERS" --strategy Upsert   --key "Region,Branch" --no-stats > /dev/null
"$DTPIPE" -i "$ORA_CONN" \
  --query "SELECT * FROM COMP_USERS ORDER BY Region, Branch" \
  -o "$INPUT_DIR/comp_ora.csv" --no-stats > /dev/null
verify_composite "$INPUT_DIR/comp_ora.csv" "Oracle"

echo "--------------------------------------------------"
echo "🎉 GOLDEN SMOKE TEST PASSED: All Systems Nominal"
echo "--------------------------------------------------"

cleanup
