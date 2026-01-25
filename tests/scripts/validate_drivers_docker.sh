#!/bin/bash
set -e

# Path to executables
QUERYDUMP="./dist/release/querydump"

echo "========================================"
echo "    QueryDump Docker Integration Tests"
echo "========================================"

# 1. Check for Docker
if ! command -v docker &> /dev/null; then
    echo "⚠️  Docker not found. Skipping Docker-based integration tests."
    exit 0
fi

if ! docker info &> /dev/null; then
    echo "⚠️  Docker daemon not running. Skipping Docker-based integration tests."
    exit 0
fi

echo "🐳 Docker detected. Starting test containers..."

cleanup() {
    echo "🧹 Cleaning up containers..."
    docker rm -f qd_postgres qd_mssql &> /dev/null || true
}
trap cleanup EXIT

# 2. Start PostgreSQL
echo "Starting PostgreSQL..."
docker run -d --name qd_postgres -e POSTGRES_PASSWORD=Password123! -p 5432:5432 postgres:15-alpine
# Wait for PG
echo "Waiting for PostgreSQL..."
sleep 5

# Create some dummy data in PG
docker exec qd_postgres psql -U postgres -d postgres -c "CREATE TABLE test_data (id INT, name TEXT); INSERT INTO test_data VALUES (1, 'Alice'), (2, 'Bob');"

# 3. Test QueryDump against PostgreSQL
echo "Running QueryDump -> PostgreSQL..."
"$QUERYDUMP" --input "postgres:Host=localhost;Username=postgres;Password=Password123!;Database=postgres" \
             --query "SELECT * FROM test_data" \
             --output "csv:dist/pg_export.csv" \
             --limit 10

if [ -f "dist/pg_export.csv" ]; then
    echo "✅ PostgreSQL Export Successful"
else
    echo "❌ PostgreSQL Export Failed (File not found)"
    exit 1
fi

# 4. Start SQL Server (MSSQL)
# Note: MSSQL takes longer to start
echo "Starting SQL Server..."

# Detect Architecture
ARCH=$(uname -m)
if [ "$ARCH" == "arm64" ]; then
    echo "🍎 ARM64 detected (Apple Silicon). Using Azure SQL Edge (lighter, native support)."
    docker run -d --name qd_mssql -e "ACCEPT_EULA=Y" -e "MSSQL_SA_PASSWORD=Password123!" -p 1433:1433 mcr.microsoft.com/azure-sql-edge
else
    echo "💻 AMD64 detected. Using official MSSQL Server."
    docker run -d --name qd_mssql -e "ACCEPT_EULA=Y" -e "MSSQL_SA_PASSWORD=Password123!" -p 1433:1433 mcr.microsoft.com/mssql/server:2022-latest
fi

echo "Waiting for SQL Server (this takes ~15s)..."
until docker exec qd_mssql /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P 'Password123!' -Q "SELECT 1" &> /dev/null; do
    echo -n "."
    sleep 2
done
echo " Ready!"

# Create data in MSSQL
docker exec qd_mssql /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P 'Password123!' -Q "CREATE DATABASE TestDB; GO; USE TestDB; CREATE TABLE TestTable (ID INT, Name NVARCHAR(50)); INSERT INTO TestTable VALUES (1, 'Charlie'), (2, 'Dave'); GO;"

# 5. Test QueryDump against MSSQL
echo "Running QueryDump -> MSSQL..."
"$QUERYDUMP" --input "mssql:Server=localhost;Database=TestDB;User Id=sa;Password=Password123!;TrustServerCertificate=True" \
             --query "SELECT * FROM TestTable" \
             --output "csv:dist/mssql_export.csv" \
             --limit 10

if [ -f "dist/mssql_export.csv" ]; then
    echo "✅ MSSQL Export Successful"
else
    echo "❌ MSSQL Export Failed"
    exit 1
fi

# 6. Start Oracle (gvenzl/oracle-free)
# Using gvenzl image which supports ARM64/AMD64 and is lighter than official
echo "Starting Oracle (gvenzl/oracle-free)..."
docker run -d --name qd_oracle -e ORACLE_PASSWORD=Password123! -p 1521:1521 gvenzl/oracle-free:slim-faststart

echo "Waiting for Oracle (this can take 30-60s)..."
# Healthcheck loop: check for "DATABASE IS READY TO USE" in logs
until docker logs qd_oracle 2>&1 | grep -q "DATABASE IS READY TO USE"; do
    echo -n "."
    sleep 5
done
echo " Ready!"

# Create data in Oracle
echo "Seeding Oracle data..."
docker exec qd_oracle sqlplus -s system/Password123!@//localhost:1521/FREEPDB1 <<EOF
CREATE TABLE TEST_DATA (ID NUMBER, NAME VARCHAR2(50));
INSERT INTO TEST_DATA VALUES (1, 'Eve');
INSERT INTO TEST_DATA VALUES (2, 'Frank');
COMMIT;
EXIT;
EOF

# 7. Test QueryDump against Oracle
echo "Running QueryDump -> Oracle..."
"$QUERYDUMP" --input "oracle:User Id=system;Password=Password123!;Data Source=localhost:1521/FREEPDB1" \
             --query "SELECT * FROM TEST_DATA" \
             --output "csv:dist/oracle_export.csv" \
             --limit 10

if [ -f "dist/oracle_export.csv" ]; then
    echo "✅ Oracle Export Successful"
else
    echo "❌ Oracle Export Failed"
    exit 1
fi

cleanup

echo "🎉 All Docker Integration Tests Passed!"
