#!/bin/bash
set -e

# Configuration
DIST_DIR="./dist/release"
DTPIPE_BIN="$DIST_DIR/dtpipe"
INPUT_DIR="tests/scripts/artifacts"
mkdir -p "$INPUT_DIR"

PG_CONTAINER="dtpipe-golden-pg"
MSSQL_CONTAINER="dtpipe-golden-mssql"
ORA_CONTAINER="dtpipe-golden-oracle"
PG_CONN="pg:Host=localhost;Port=5434;Username=postgres;Password=password;Database=postgres"

# Check binary
if [ ! -f "$DTPIPE_BIN" ]; then
    echo "Error: Binary not found at $DTPIPE_BIN. Run ./build.sh first."
    exit 1
fi

echo "=========================================="
echo "    DtPipe GOLDEN SMOKE TEST (VICIOUS)    "
echo "=========================================="

# Cleanup Setup
cleanup() {
    rm -f "$INPUT_DIR"/vicious_source.csv "$INPUT_DIR"/vicious.parquet "$INPUT_DIR"/vicious.db "$INPUT_DIR"/query.sql "$INPUT_DIR"/result_*.csv "$INPUT_DIR"/high_volume.csv "$INPUT_DIR"/high_volume.parquet "$INPUT_DIR"/composite_*.csv "$INPUT_DIR"/comp_*.csv "$INPUT_DIR"/vicious_inc.csv
    # Also clean locally just in case
    rm -f result_*.csv high_volume.csv high_volume.parquet
    docker rm -f $PG_CONTAINER $MSSQL_CONTAINER $ORA_CONTAINER > /dev/null 2>&1 || true
}
cleanup # Pre-clean

# ---------------------------------------------------------
# 1. Environment Setup
# ---------------------------------------------------------
# ---------------------------------------------------------
# 1. Environment Setup (Parallel Startup)
# ---------------------------------------------------------
echo "[1/9] Starting Containers (Postgres, MSSQL, Oracle)..."
docker run --name $PG_CONTAINER -e POSTGRES_PASSWORD=password -p 5434:5432 -d postgres:15-alpine > /dev/null

# MSSQL
MSSQL_CONTAINER="dtpipe-golden-mssql"
docker run --name $MSSQL_CONTAINER -e "ACCEPT_EULA=Y" -e "MSSQL_SA_PASSWORD=MySecretPassword123!" -p 1434:1433 -d mcr.microsoft.com/azure-sql-edge:latest > /dev/null

# Oracle
ORA_CONTAINER="dtpipe-golden-oracle"
docker run --name $ORA_CONTAINER -e ORACLE_PASSWORD=MySecretPassword123! -p 1522:1521 -d gvenzl/oracle-free:slim > /dev/null

echo "      Waiting for containers to initialize..."

# Wait Loop Helper
wait_for_db() {
    local name=$1
    local conn=$2
    local query=$3
    local max_retries=20
    local i=0
    
    echo -n "      Waiting for $name..."
    until $DTPIPE_BIN -i "$conn" -q "$query" -o "csv" > /dev/null 2>&1; do
        i=$((i + 1))
        if [ $i -ge $max_retries ]; then
            echo " FAILED (Timeout)"
            docker logs $name | tail -n 10
            exit 1
        fi
        echo -n "."
        sleep 5
    done
    echo " OK"
}

# MSSQL Connection for Check
MSSQL_CONN_CHECK="mssql:Server=localhost,1434;Database=master;User Id=sa;Password=MySecretPassword123!;TrustServerCertificate=True;Encrypt=False"
# Oracle Connection for Check
ORA_CONN_CHECK="ora:Data Source=localhost:1522/FREEPDB1;User Id=system;Password=MySecretPassword123!;"

# Perform Checks
wait_for_db "Postgres" "$PG_CONN" "SELECT 1"
wait_for_db "MSSQL" "$MSSQL_CONN_CHECK" "SELECT 1"
wait_for_db "Oracle" "$ORA_CONN_CHECK" "SELECT 1 FROM DUAL"

cleanup() {
    rm -f vicious_source.csv vicious.parquet vicious.db query.sql result_*.csv high_volume.csv high_volume.parquet
    docker rm -f $PG_CONTAINER $MSSQL_CONTAINER $ORA_CONTAINER > /dev/null 2>&1 || true
}

# ---------------------------------------------------------
# 2. Generate VICIOUS Source Data (CSV)
# ---------------------------------------------------------
echo "[2/9] Generating Vicious Source Data..."
cat <<EOF > "$INPUT_DIR/vicious_source.csv"
Id,Name,Email,Details,Amount,IsActive,JoinDate
1,Alice,alice@example.com,"Standard User",100.50,true,2023-01-01
2,Bob O'Connor,bob@example.com,"Contains, Commas",200.00,false,2023-02-15
3,Charlie "The Boss",charlie@example.com,"Contains ""Quotes""",300.99,true,2023-03-20
4,D'Artagnan,darian@example.com,NULL,0,false,
5,Élise,elise@example.com,"UTF-8: 🚀 Ümlaut",-50.5,true,2023-12-31
6,Frank,,Empty Email,,1,,2024-01-01
7,Robert'); DROP TABLE Students;--,hack@example.com,SQL Injection Attempt,9999,true,2023-01-01
EOF
echo "      Generated 7 rows with edge cases."

# ---------------------------------------------------------
# 3. High Volume Test (1M Rows on ALL DBs)
# ---------------------------------------------------------
echo "[3/9] Running High Volume Test (1,000,000 rows on all DBs)..."
# Generate 1M rows
echo "Id,Guid,Number" > "$INPUT_DIR/high_volume.csv"
# awk is fast enough for 1M simple rows
awk 'BEGIN { for(i=1; i<=1000000; i++) print i ",uuid-" i "," rand() }' >> "$INPUT_DIR/high_volume.csv"

echo "      Converting CSV -> Parquet..."
$DTPIPE_BIN -i "$INPUT_DIR/high_volume.csv" -o "parquet:$INPUT_DIR/high_volume.parquet" > /dev/null

# Verify Parquet Count
$DTPIPE_BIN -i "parquet:$INPUT_DIR/high_volume.parquet" -o "$INPUT_DIR/result_vol.csv" > /dev/null
VOL_LINES=$(wc -l < "$INPUT_DIR/result_vol.csv" | tr -d ' ')
if [ "$VOL_LINES" != "1000001" ]; then
    echo "❌ FAILED: Parquet Volume Mismatch (Expected 1000001 lines, Got $VOL_LINES)"
    exit 1
fi
echo "✅ Parquet Volume Verified (1M rows)."
rm "$INPUT_DIR"/result_vol.csv

# SQLite Load (1M)
echo "      Loading 1M rows into SQLite..."
$DTPIPE_BIN -i "parquet:$INPUT_DIR/high_volume.parquet" -o "sqlite:$INPUT_DIR/high_volume.db" --sqlite-table "vol_data" --sqlite-strategy Recreate > /dev/null
$DTPIPE_BIN -i "sqlite:$INPUT_DIR/high_volume.db" -q "SELECT COUNT(*) FROM vol_data" -o "csv" > "$INPUT_DIR/result_vol_sqlite.csv"
COUNT=$(tail -n 1 "$INPUT_DIR/result_vol_sqlite.csv" | tr -d '\r')
if [ "$COUNT" != "1000000" ]; then echo "❌ FAILED: SQLite Volume Mismatch ($COUNT)"; exit 1; fi
echo "✅ SQLite 1M Load Verified."

# Postgres Load (1M)
echo "      Loading 1M rows into Postgres..."
$DTPIPE_BIN -i "parquet:$INPUT_DIR/high_volume.parquet" -o "$PG_CONN" --pg-table "voldata" --pg-strategy Recreate > /dev/null
$DTPIPE_BIN -i "$PG_CONN" -q "SELECT COUNT(*) FROM voldata" -o "csv" > "$INPUT_DIR/result_vol_pg.csv"
COUNT=$(tail -n 1 "$INPUT_DIR/result_vol_pg.csv" | tr -d '\r')
if [ "$COUNT" != "1000000" ]; then echo "❌ FAILED: PG Volume Mismatch ($COUNT)"; exit 1; fi
echo "✅ Postgres 1M Load Verified."

# MSSQL Load (1M)
echo "      Loading 1M rows into MSSQL..."
MSSQL_CONN="mssql:Server=localhost,1434;Database=master;User Id=sa;Password=MySecretPassword123!;TrustServerCertificate=True;Encrypt=False;MultipleActiveResultSets=True"
$DTPIPE_BIN -i "parquet:$INPUT_DIR/high_volume.parquet" -o "$MSSQL_CONN" --mssql-table "VolData" --mssql-strategy Recreate > /dev/null
$DTPIPE_BIN -i "$MSSQL_CONN" -q "SELECT COUNT(*) FROM VolData" -o "csv" > "$INPUT_DIR/result_vol_mssql.csv"
COUNT=$(tail -n 1 "$INPUT_DIR/result_vol_mssql.csv" | tr -d '\r')
if [ "$COUNT" != "1000000" ]; then echo "❌ FAILED: MSSQL Volume Mismatch ($COUNT)"; exit 1; fi
echo "✅ MSSQL 1M Load Verified."

# Oracle Load (1M)
echo "      Loading 1M rows into Oracle..."
ORA_CONN="ora:Data Source=localhost:1522/FREEPDB1;User Id=system;Password=MySecretPassword123!;"
$DTPIPE_BIN -i "parquet:$INPUT_DIR/high_volume.parquet" -o "$ORA_CONN" --ora-table "VOL_DATA" --ora-strategy Recreate > /dev/null
$DTPIPE_BIN -i "$ORA_CONN" -q "SELECT COUNT(*) FROM VOL_DATA" -o "csv" > "$INPUT_DIR/result_vol_ora.csv"
COUNT=$(tail -n 1 "$INPUT_DIR/result_vol_ora.csv" | tr -d '\r')
if [ "$COUNT" != "1000000" ]; then echo "❌ FAILED: Oracle Volume Mismatch ($COUNT)"; exit 1; fi
echo "✅ Oracle 1M Load Verified."
rm "$INPUT_DIR"/high_volume.* "$INPUT_DIR"/result_vol_*.csv

# ---------------------------------------------------------
# 4. Pipeline 1: CSV -> Parquet (Anonymization & Transformation)
# ---------------------------------------------------------
echo "[4/9] Running Pipeline 1: CSV -> Parquet (Anonymize & Mask)..."
$DTPIPE_BIN -i "$INPUT_DIR/vicious_source.csv" -o "parquet:$INPUT_DIR/vicious.parquet" \
    --fake "Email:internet.email" \
    --fake-seed-column "Id" \
    --null "Details" \
    --mask "Name:?************" \
    > /dev/null

echo "      Parquet file created. Inspecting..."
$DTPIPE_BIN -i "parquet:$INPUT_DIR/vicious.parquet" -o "$INPUT_DIR/result_step1.csv" > /dev/null

if grep -q "alice@example.com" "$INPUT_DIR/result_step1.csv"; then echo "❌ FAILED: Anonymization failed"; exit 1; fi
if ! grep -q "@" "$INPUT_DIR/result_step1.csv"; then echo "❌ FAILED: Faking failed"; exit 1; fi
echo "✅ Anonymization Verified."

# ---------------------------------------------------------
# 5. Pipeline 2: Parquet -> SQLite
# ---------------------------------------------------------
echo "[5/9] Running Pipeline 2: Parquet -> SQLite..."
$DTPIPE_BIN -i "parquet:$INPUT_DIR/vicious.parquet" -o "sqlite:$INPUT_DIR/vicious.db" --sqlite-table "users" --sqlite-strategy Recreate > /dev/null
$DTPIPE_BIN -i "sqlite:$INPUT_DIR/vicious.db" -q "SELECT COUNT(*) FROM users" -o "csv" > "$INPUT_DIR/result_count.csv"
COUNT=$(tail -n 1 "$INPUT_DIR/result_count.csv" | tr -d '\r')
if [ "$COUNT" != "7" ]; then echo "❌ FAILED: SQLite Count Mismatch"; exit 1; fi
echo "✅ SQLite Count Verified."
rm "$INPUT_DIR"/result_count.csv

# ... Wait for containers if needed ...
echo "      Ensuring containers are ready..."
sleep 20 # Total wait ~20s + exec time of steps 2-5 (~5s) = 25s. Oracle needs 30s. Adding small buffer.
sleep 10 

# ---------------------------------------------------------
# 6. Pipeline 3: SQLite -> Postgres (Upsert)
# ---------------------------------------------------------
echo "[6/9] Running Pipeline 3: SQLite -> Postgres (Upsert)..."
$DTPIPE_BIN -i "sqlite:$INPUT_DIR/vicious.db" -q "SELECT * FROM users" -o "$PG_CONN" --pg-table "vicious_users" --pg-strategy Recreate --key "Id" > /dev/null

# Update Source
cat <<EOF > "$INPUT_DIR/vicious_inc.csv"
Id,Name,Email,Details,Amount,IsActive,JoinDate
1,Alice Updated,newalice@example.com,Updated,999,true,2023-01-01
8,New User,new@example.com,New,0,true,2024-01-01
EOF
$DTPIPE_BIN -i "$INPUT_DIR/vicious_inc.csv" -o "$PG_CONN" --pg-table "vicious_users" --pg-strategy Upsert --key "Id" > /dev/null

# Verify PG
$DTPIPE_BIN -i "$PG_CONN" -q "SELECT Name FROM vicious_users WHERE Id = '1'" -o "$INPUT_DIR/result_pg_check.csv" > /dev/null
if ! grep -q "Alice Updated" "$INPUT_DIR/result_pg_check.csv"; then echo "❌ FAILED: PG Upsert failed"; exit 1; fi
echo "✅ Postgres Upsert Verified."

# ---------------------------------------------------------
# 7. Pipeline 4: Query from File
# ---------------------------------------------------------
echo "[7/9] Testing Query from File..."
echo "SELECT * FROM vicious_users WHERE CAST(NULLIF(Amount, '') AS NUMERIC) > 100 ORDER BY Id" > "$INPUT_DIR/query.sql"
$DTPIPE_BIN -i "$PG_CONN" -q "$INPUT_DIR/query.sql" -o "$INPUT_DIR/result_final.csv" > /dev/null
LINE_COUNT=$(wc -l < "$INPUT_DIR/result_final.csv" | tr -d ' ')
if [ "$LINE_COUNT" != "5" ]; then echo "❌ FAILED: Query File Mismatch (Expected 5, Got $LINE_COUNT)"; exit 1; fi
echo "✅ Query File Verified."

# ---------------------------------------------------------
# 8. SQL Server Test
# ---------------------------------------------------------
echo "[8/9] Testing SQL Server..."
MSSQL_CONN="mssql:Server=localhost,1434;Database=master;User Id=sa;Password=MySecretPassword123!;TrustServerCertificate=True"
# Load Parquet -> MSSQL
$DTPIPE_BIN -i "parquet:$INPUT_DIR/vicious.parquet" -o "$MSSQL_CONN" --mssql-table "ViciousUsers" --mssql-strategy Recreate > /dev/null
# Verify Recreate
$DTPIPE_BIN -i "$MSSQL_CONN" -q "SELECT COUNT(*) FROM ViciousUsers" -o "csv" > "$INPUT_DIR/result_mssql.csv"
COUNT=$(tail -n 1 "$INPUT_DIR/result_mssql.csv" | tr -d '\r')
if [ "$COUNT" != "7" ]; then echo "❌ FAILED: MSSQL Count Mismatch (Expected 7, Got $COUNT)"; exit 1; fi
echo "✅ SQL Server Initial Load Verified."

# MSSQL Upsert
$DTPIPE_BIN -i "$INPUT_DIR/vicious_inc.csv" -o "$MSSQL_CONN" --mssql-table "ViciousUsers" --mssql-strategy Upsert --key "Id" > /dev/null
$DTPIPE_BIN -i "$MSSQL_CONN" -q "SELECT Name FROM ViciousUsers WHERE Id = '1'" -o "$INPUT_DIR/result_mssql_check.csv" > /dev/null
if ! grep -q "Alice Updated" "$INPUT_DIR/result_mssql_check.csv"; then echo "❌ FAILED: MSSQL Upsert failed"; exit 1; fi
echo "✅ SQL Server Upsert Verified."

# ---------------------------------------------------------
# 9. Oracle Test
# ---------------------------------------------------------
echo "[9/9] Testing Oracle..."
ORA_CONN="ora:Data Source=localhost:1522/FREEPDB1;User Id=system;Password=MySecretPassword123!;"
# Load Parquet -> Oracle
# Note: Oracle table names UPPERCASE usually, and columns too. DtPipe handles this via minimal quoting (so "ViciousUsers" might be "VICIOUSUSERS").
$DTPIPE_BIN -i "parquet:$INPUT_DIR/vicious.parquet" -o "$ORA_CONN" --ora-table "VICIOUS_USERS" --ora-strategy Recreate > /dev/null
# Verify Recreate
$DTPIPE_BIN -i "$ORA_CONN" -q "SELECT COUNT(*) FROM VICIOUS_USERS" -o "csv" > "$INPUT_DIR/result_ora.csv"
COUNT=$(tail -n 1 "$INPUT_DIR/result_ora.csv" | tr -d '\r')
if [ "$COUNT" != "7" ]; then echo "❌ FAILED: Oracle Count Mismatch (Expected 7, Got $COUNT)"; exit 1; fi
echo "✅ Oracle Initial Load Verified."

# Oracle Upsert
$DTPIPE_BIN -i "$INPUT_DIR/vicious_inc.csv" -o "$ORA_CONN" --ora-table "VICIOUS_USERS" --ora-strategy Upsert --key "Id" > /dev/null
$DTPIPE_BIN -i "$ORA_CONN" -q "SELECT Name FROM VICIOUS_USERS WHERE Id = 1" -o "$INPUT_DIR/result_ora_check.csv" > /dev/null
if ! grep -q "Alice Updated" "$INPUT_DIR/result_ora_check.csv"; then echo "❌ FAILED: Oracle Upsert failed"; exit 1; fi
echo "✅ Oracle Upsert Verified."

# ---------------------------------------------------------
# 10. Composite Key Test
# ---------------------------------------------------------
echo "[10/10] Composite Key Upsert Test (Region, Branch)..."

# Source V1
echo "Region,Branch,Target" > "$INPUT_DIR/composite_source.csv"
echo "EU,Paris,100" >> "$INPUT_DIR/composite_source.csv"
echo "EU,Berlin,200" >> "$INPUT_DIR/composite_source.csv"
echo "US,NY,500" >> "$INPUT_DIR/composite_source.csv"

# Source V2 (Upsert)
echo "Region,Branch,Target" > "$INPUT_DIR/composite_inc.csv"
echo "EU,Paris,150" >> "$INPUT_DIR/composite_inc.csv"         # Update Target 100 -> 150
echo "EU,Madrid,300" >> "$INPUT_DIR/composite_inc.csv"        # New Insert

verify_composite() {
    local file=$1
    local name=$2
    if grep -q "EU,Paris,150" "$file" && grep -q "EU,Madrid,300" "$file" && grep -q "EU,Berlin,200" "$file"; then
        echo "✅ $name Composite Verified."
    else
        echo "❌ FAILED: $name Composite Key Test Failed"
        cat "$file"
        exit 1
    fi
}

# DuckDB Composite
echo "      Testing DuckDB..."
$DTPIPE_BIN -i "$INPUT_DIR/composite_source.csv" -o "duck:$INPUT_DIR/composite.duckdb" --duck-table "comp_users" --duck-strategy Recreate --key "Region,Branch" > /dev/null
$DTPIPE_BIN -i "$INPUT_DIR/composite_inc.csv" -o "duck:$INPUT_DIR/composite.duckdb" --duck-table "comp_users" --duck-strategy Upsert --key "Region,Branch" > /dev/null
$DTPIPE_BIN -i "duck:$INPUT_DIR/composite.duckdb" -q "SELECT * FROM comp_users ORDER BY Region, Branch" -o "$INPUT_DIR/comp_duck.csv" > /dev/null
verify_composite "$INPUT_DIR/comp_duck.csv" "DuckDB"

# SQLite Composite
echo "      Testing SQLite..."
$DTPIPE_BIN -i "$INPUT_DIR/composite_source.csv" -o "sqlite:$INPUT_DIR/composite.db" --sqlite-table "comp_users" --sqlite-strategy Recreate --key "Region,Branch" > /dev/null
$DTPIPE_BIN -i "$INPUT_DIR/composite_inc.csv" -o "sqlite:$INPUT_DIR/composite.db" --sqlite-table "comp_users" --sqlite-strategy Upsert --key "Region,Branch" > /dev/null
$DTPIPE_BIN -i "sqlite:$INPUT_DIR/composite.db" -q "SELECT * FROM comp_users ORDER BY Region, Branch" -o "$INPUT_DIR/comp_sqlite.csv" > /dev/null
verify_composite "$INPUT_DIR/comp_sqlite.csv" "SQLite"

# Postgres Composite
echo "      Testing Postgres..."
$DTPIPE_BIN -i "$INPUT_DIR/composite_source.csv" -o "$PG_CONN" --pg-table "comp_users" --pg-strategy Recreate --key "Region,Branch" > /dev/null
$DTPIPE_BIN -i "$INPUT_DIR/composite_inc.csv" -o "$PG_CONN" --pg-table "comp_users" --pg-strategy Upsert --key "Region,Branch" > /dev/null
$DTPIPE_BIN -i "$PG_CONN" -q "SELECT * FROM comp_users ORDER BY Region, Branch" -o "$INPUT_DIR/comp_pg.csv" > /dev/null
verify_composite "$INPUT_DIR/comp_pg.csv" "Postgres"

# MSSQL Composite
echo "      Testing MSSQL..."
$DTPIPE_BIN -i "$INPUT_DIR/composite_source.csv" -o "$MSSQL_CONN" --mssql-table "CompUsers" --mssql-strategy Recreate --key "Region,Branch" > /dev/null
$DTPIPE_BIN -i "$INPUT_DIR/composite_inc.csv" -o "$MSSQL_CONN" --mssql-table "CompUsers" --mssql-strategy Upsert --key "Region,Branch" > /dev/null
$DTPIPE_BIN -i "$MSSQL_CONN" -q "SELECT * FROM CompUsers ORDER BY Region, Branch" -o "$INPUT_DIR/comp_mssql.csv" > /dev/null
verify_composite "$INPUT_DIR/comp_mssql.csv" "MSSQL"

# Oracle Composite
echo "      Testing Oracle..."
$DTPIPE_BIN -i "$INPUT_DIR/composite_source.csv" -o "$ORA_CONN" --ora-table "COMP_USERS" --ora-strategy Recreate --key "Region,Branch" > /dev/null
$DTPIPE_BIN -i "$INPUT_DIR/composite_inc.csv" -o "$ORA_CONN" --ora-table "COMP_USERS" --ora-strategy Upsert --key "Region,Branch" > /dev/null
$DTPIPE_BIN -i "$ORA_CONN" -q "SELECT * FROM COMP_USERS ORDER BY Region, Branch" -o "$INPUT_DIR/comp_ora.csv" > /dev/null
# Oracle CSV export might differ slightly if nulls/numbers, but this is simple data.
verify_composite "$INPUT_DIR/comp_ora.csv" "Oracle"

echo "--------------------------------------------------"
echo "🎉 GOLDEN SMOKE TEST PASSED: All Systems Nominal"
echo "--------------------------------------------------"

cleanup
