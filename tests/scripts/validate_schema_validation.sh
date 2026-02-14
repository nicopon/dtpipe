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
echo "    DtPipe Schema Validation"
echo "========================================"

# Setup SQLite Source & Target (Incompatible)
SOURCE_DB="$ARTIFACTS_DIR/schema_source.db"
TARGET_DB="$ARTIFACTS_DIR/schema_target.db"
rm -f "$SOURCE_DB" "$TARGET_DB"

sqlite3 "$SOURCE_DB" "CREATE TABLE users (id INTEGER, name TEXT, extra TEXT);"
sqlite3 "$TARGET_DB" "CREATE TABLE users (id INTEGER, name TEXT);"

# 1. Test Strict Schema (Should fail because 'extra' is missing in target)
echo "Test 1: --strict-schema FAIL case..."
$DTPIPE_BIN --input "sqlite:Data Source=$SOURCE_DB" \
               --query "SELECT * FROM users" \
               --output "sqlite:Data Source=$TARGET_DB" \
               --table "users" \
               --strict-schema

if [ $? -ne 0 ]; then
    echo -e "${GREEN}✓ Strict Schema caught mismatch as expected${NC}"
else
    echo -e "${RED}✗ Strict Schema should have failed${NC}"
    exit 1
fi

# 2. Test No Schema Validation (Should proceed)
echo "Test 2: --no-schema-validation case..."
# Note: It will fail at insertion time, but we want to see it pass the validation stage
$DTPIPE_BIN --input "sqlite:Data Source=$SOURCE_DB" \
               --query "SELECT * FROM users" \
               --output "sqlite:Data Source=$TARGET_DB" \
               --table "users" \
               --no-schema-validation 2>&1 | grep -q "Verifying target schema compatibility..."

if [ $? -ne 0 ]; then
    echo -e "${GREEN}✓ No schema validation bypassed the check${NC}"
else
    # If grep found the string, it means it DID check
    echo -e "${RED}✗ No schema validation did not bypass the check${NC}"
    exit 1
fi

# --- PG TEST ---
if command -v docker &> /dev/null && docker info &> /dev/null; then
    echo "Test 3: PostgreSQL Strict Schema..."
    docker exec -i dtpipe-integ-postgres psql -U postgres -d integration <<EOF
DROP TABLE IF EXISTS target_strict;
CREATE TABLE target_strict (id SERIAL PRIMARY KEY, name VARCHAR(50));
EOF
    
    # Try to export 3 columns to 2 columns
    $DTPIPE_BIN --input "generate:1" --query "SELECT * FROM data" \
                   --fake "id:random.number" \
                   --fake "name:name.fullName" \
                   --fake "email:internet.email" \
                   --output "pg:Host=localhost;Port=5440;Username=postgres;Password=password;Database=integration" \
                   --table "target_strict" \
                   --strict-schema
    
    if [ $? -ne 0 ]; then
        echo -e "${GREEN}✓ Postgres Strict Schema caught mismatch${NC}"
    else
        echo -e "${RED}✗ Postgres Strict Schema should have failed${NC}"
        exit 1
    fi
fi

exit 0
