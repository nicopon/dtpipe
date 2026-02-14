#!/bin/bash

# Configuration
SCRIPT_DIR="$(dirname "$0")"
PROJECT_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"
DIST_DIR="$PROJECT_ROOT/dist/release"
DTPIPE_BIN="$DIST_DIR/dtpipe"
ARTIFACTS_DIR="tests/scripts/artifacts"
mkdir -p "$ARTIFACTS_DIR"

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

echo "========================================"
echo "    DtPipe Auto-Migration Validation"
echo "========================================"

# 1. SQLite Scenario
echo "Test 1: SQLite Auto-Migrate..."
SOURCE_DB="$ARTIFACTS_DIR/mig_source.db"
TARGET_DB="$ARTIFACTS_DIR/mig_target.db"
rm -f "$SOURCE_DB" "$TARGET_DB"

sqlite3 "$SOURCE_DB" "CREATE TABLE data (id INTEGER, val TEXT, new_col TEXT);"
sqlite3 "$SOURCE_DB" "INSERT INTO data VALUES (1, 'A', 'NEW!');"
sqlite3 "$TARGET_DB" "CREATE TABLE data (id INTEGER, val TEXT);"

$DTPIPE_BIN --input "sqlite:Data Source=$SOURCE_DB" \
               --query "SELECT * FROM data" \
               --output "sqlite:Data Source=$TARGET_DB" \
               --table "data" \
               --auto-migrate

# Verify column exists and data is there
if sqlite3 "$TARGET_DB" ".schema data" | grep -q "new_col"; then
    echo -e "${GREEN}✓ SQLite Auto-Migrate: Column added${NC}"
else
    echo -e "${RED}✗ SQLite Auto-Migrate: Column NOT added${NC}"
    exit 1
fi

# 2. Postgres Scenario
if command -v docker &> /dev/null && docker info &> /dev/null; then
    echo "Test 2: PostgreSQL Auto-Migrate..."
    docker exec -i dtpipe-integ-postgres psql -U postgres -d integration <<EOF
DROP TABLE IF EXISTS target_migrate;
CREATE TABLE target_migrate (id SERIAL PRIMARY KEY, name VARCHAR(50));
EOF

    $DTPIPE_BIN --input "generate:1" --query "SELECT * FROM data" \
                   --fake "id:random.number" \
                   --fake "name:name.fullName" \
                   --fake "email:internet.email" \
                   --output "pg:Host=localhost;Port=5440;Username=postgres;Password=password;Database=integration" \
                   --table "target_migrate" \
                   --auto-migrate
    
    # Verify via psql
    if docker exec -i dtpipe-integ-postgres psql -U postgres -d integration -c "\d target_migrate" | grep -q "email"; then
        echo -e "${GREEN}✓ Postgres Auto-Migrate: Column added${NC}"
    else
        echo -e "${RED}✗ Postgres Auto-Migrate: Column NOT added${NC}"
        exit 1
    fi
fi

exit 0
