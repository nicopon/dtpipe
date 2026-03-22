#!/bin/bash
set -e

# validate_schema.sh
# Tests: strict-schema enforcement, no-schema-validation bypass, auto-migrate (add column).
# SQLite tests run without Docker. Postgres tests require Docker.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
ARTIFACTS_DIR="$SCRIPT_DIR/artifacts"
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
echo "    DtPipe Schema Validation"
echo "========================================"

if [ ! -f "$DTPIPE" ]; then
    echo "Building release..."
    "$PROJECT_ROOT/build.sh" > /dev/null
fi

has_docker() {
    command -v docker &>/dev/null && docker info &>/dev/null
}

# ----------------------------------------
# 1. SQLite strict-schema (should fail on mismatch)
# ----------------------------------------
echo "--- [1] SQLite strict-schema FAIL case ---"
SRC_DB="$ARTIFACTS_DIR/schema_source.db"
TGT_DB="$ARTIFACTS_DIR/schema_target.db"
rm -f "$SRC_DB" "$TGT_DB"

sqlite3 "$SRC_DB" "CREATE TABLE users (id INTEGER, name TEXT, extra TEXT);"
sqlite3 "$TGT_DB" "CREATE TABLE users (id INTEGER, name TEXT);"

set +e
"$DTPIPE" -i "sqlite:Data Source=$SRC_DB" \
  --query "SELECT * FROM users" \
  -o "sqlite:Data Source=$TGT_DB" \
  --table "users" \
  --strict-schema 2>/dev/null
EXIT=$?
set -e

[ $EXIT -ne 0 ] && pass "Strict-schema caught column mismatch" || fail "Strict-schema should have failed"

# ----------------------------------------
# 2. SQLite no-schema-validation (bypass)
# ----------------------------------------
echo "--- [2] SQLite --no-schema-validation ---"
set +e
OUTPUT=$("$DTPIPE" -i "sqlite:Data Source=$SRC_DB" \
  --query "SELECT * FROM users" \
  -o "sqlite:Data Source=$TGT_DB" \
  --table "users" \
  --no-schema-validation 2>&1)
set -e

# With no-schema-validation, the validation step should be skipped (won't print "Verifying target schema")
echo "$OUTPUT" | grep -q "Verifying target schema compatibility" \
  && fail "--no-schema-validation still ran schema check" \
  || pass "--no-schema-validation bypassed schema check"

# ----------------------------------------
# 3. SQLite auto-migrate (add missing column)
# ----------------------------------------
echo "--- [3] SQLite auto-migrate ---"
MIG_SRC="$ARTIFACTS_DIR/mig_source.db"
MIG_TGT="$ARTIFACTS_DIR/mig_target.db"
rm -f "$MIG_SRC" "$MIG_TGT"

sqlite3 "$MIG_SRC" "CREATE TABLE data (id INTEGER, val TEXT, new_col TEXT);"
sqlite3 "$MIG_SRC" "INSERT INTO data VALUES (1, 'A', 'NEW!');"
sqlite3 "$MIG_TGT" "CREATE TABLE data (id INTEGER, val TEXT);"

"$DTPIPE" -i "sqlite:Data Source=$MIG_SRC" \
  --query "SELECT * FROM data" \
  -o "sqlite:Data Source=$MIG_TGT" \
  --table "data" \
  --auto-migrate

sqlite3 "$MIG_TGT" ".schema data" | grep -q "new_col" \
  && pass "SQLite auto-migrate: new_col added" \
  || fail "SQLite auto-migrate: new_col NOT added"

# ----------------------------------------
# 4. Postgres strict-schema (Docker required)
# ----------------------------------------
echo "--- [4] Postgres strict-schema ---"
if ! has_docker; then
    skip "Docker not available"
else
    docker exec -i dtpipe-integ-postgres psql -U postgres -d integration <<'EOF' > /dev/null
DROP TABLE IF EXISTS target_strict;
CREATE TABLE target_strict (id SERIAL PRIMARY KEY, name VARCHAR(50));
EOF

    set +e
    "$DTPIPE" -i "generate:1" \
      --fake "id:random.number" \
      --fake "name:name.fullName" \
      --fake "email:internet.email" \
      --drop "GenerateIndex" \
      -o "pg:Host=localhost;Port=5440;Username=postgres;Password=password;Database=integration" \
      --table "target_strict" \
      --strict-schema 2>/dev/null
    EXIT=$?
    set -e

    [ $EXIT -ne 0 ] && pass "Postgres strict-schema caught mismatch" || fail "Postgres strict-schema should have failed"
fi

# ----------------------------------------
# 5. Postgres auto-migrate (add email column)
# ----------------------------------------
echo "--- [5] Postgres auto-migrate ---"
if ! has_docker; then
    skip "Docker not available"
else
    docker exec -i dtpipe-integ-postgres psql -U postgres -d integration <<'EOF' > /dev/null
DROP TABLE IF EXISTS target_migrate;
CREATE TABLE target_migrate (id SERIAL PRIMARY KEY, name VARCHAR(50));
EOF

    "$DTPIPE" -i "generate:1" \
      --fake "id:random.number" \
      --fake "name:name.fullName" \
      --fake "email:internet.email" \
      --drop "GenerateIndex" \
      -o "pg:Host=localhost;Port=5440;Username=postgres;Password=password;Database=integration" \
      --table "target_migrate" \
      --auto-migrate

    docker exec -i dtpipe-integ-postgres psql -U postgres -d integration -c "\d target_migrate" \
      | grep -q "email" \
      && pass "Postgres auto-migrate: email column added" \
      || fail "Postgres auto-migrate: email column NOT added"
fi

echo ""
echo -e "${GREEN}Schema validation complete!${NC}"
