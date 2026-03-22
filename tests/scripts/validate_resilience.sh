#!/bin/bash
set -e

# validate_resilience.sh
# Tests: YAML persistence, SQLite lock retry, network interruption (Toxiproxy), upsert resilience.
# Notes:
#   - Network tests require Toxiproxy running on localhost:8474 and Docker.
#   - SQLite locking test requires dotnet-script (for tools/Locker.cs).

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
echo "    DtPipe Resilience Validation"
echo "========================================"

if [ ! -f "$DTPIPE" ]; then
    echo "Building release..."
    "$PROJECT_ROOT/build.sh" > /dev/null
fi

# ----------------------------------------
# 1. YAML persistence (retry options round-trip)
# ----------------------------------------
echo "--- [1] YAML retry-options persistence ---"
YAML_FILE="$ARTIFACTS_DIR/resilience.yaml"
rm -f "$YAML_FILE"

"$DTPIPE" -i "generate:10" \
  --fake "id:random.uuid" \
  --drop "GenerateIndex" \
  -o "csv:$ARTIFACTS_DIR/res_out.csv" \
  --max-retries 12 \
  --retry-delay-ms 450 \
  --export-job "$YAML_FILE" --no-stats

if grep -iq "max-retries: 12" "$YAML_FILE" && grep -iq "retry-delay-ms: 450" "$YAML_FILE"; then
    pass "Retry options persisted to YAML"
else
    fail "Retry options not found in YAML"
fi

echo "  Running from YAML..."
"$DTPIPE" --job "$YAML_FILE"
pass "Execution from YAML succeeded"

# ----------------------------------------
# 2. SQLite lock retry
# ----------------------------------------
echo "--- [2] SQLite lock retry ---"
LOCKER_SCRIPT="$SCRIPT_DIR/tools/Locker.cs"

if [ ! -f "$LOCKER_SCRIPT" ]; then
    skip "Locker.cs not found at $LOCKER_SCRIPT — skipping lock test"
else
    DB_PATH="$ARTIFACTS_DIR/resilience_lock.db"
    SIGNAL_FILE="$DB_PATH.signal"
    rm -f "$DB_PATH" "$SIGNAL_FILE"

    sqlite3 "$DB_PATH" "DROP TABLE IF EXISTS sensitive_data; CREATE TABLE sensitive_data (id INTEGER PRIMARY KEY, content TEXT);"

    chmod +x "$LOCKER_SCRIPT" 2>/dev/null || true
    "$LOCKER_SCRIPT" "$DB_PATH" "3000" &
    LOCKER_PID=$!

    echo "  Waiting for lock signal..."
    for i in {1..30}; do
        [ -f "$SIGNAL_FILE" ] && break
        sleep 0.5
    done

    if [ ! -f "$SIGNAL_FILE" ]; then
        kill $LOCKER_PID 2>/dev/null || true
        fail "Timeout waiting for locker signal"
    fi

    START=$(date +%s)
    "$DTPIPE" -i "generate:10" \
      --fake "id:random.number" --fake "content:name.fullName" \
      --drop "GenerateIndex" \
      -o "sqlite:Data Source=$DB_PATH" \
      --table "sensitive_data" \
      --max-retries 5 \
      --retry-delay-ms 1000 \
      --strategy Append
    EXIT_CODE=$?
    END=$(date +%s)
    DURATION=$((END - START))
    wait $LOCKER_PID 2>/dev/null || true

    [ $EXIT_CODE -eq 0 ] || fail "DtPipe failed with exit code $EXIT_CODE"
    COUNT=$(sqlite3 "$DB_PATH" "SELECT COUNT(*) FROM sensitive_data;")
    [ "$COUNT" -eq 11 ] && pass "Lock retry: 11 rows written, took ${DURATION}s" || fail "Lock retry: expected 11 rows, got $COUNT"
fi

# ----------------------------------------
# 3. Network interruption (Append strategy)
# ----------------------------------------
echo "--- [3] Network interruption (Toxiproxy) ---"
TOXIPROXY_API="http://localhost:8474"
PROXY_NAME="postgres_proxy"
PROXIED_PG_PORT=5441

if ! curl -s --max-time 2 "$TOXIPROXY_API/proxies" > /dev/null 2>&1; then
    skip "Toxiproxy not available at $TOXIPROXY_API — skipping network test"
elif ! command -v docker &>/dev/null || ! docker info &>/dev/null; then
    skip "Docker not available — skipping network test"
else
    PG_USER="postgres"; PG_PASSWORD="password"; PG_DB="integration"
    CONN_STR="pg:Host=localhost;Port=$PROXIED_PG_PORT;Username=$PG_USER;Password=$PG_PASSWORD;Database=$PG_DB;Pooling=false;CommandTimeout=5;Timeout=5"

    curl -s -X DELETE "$TOXIPROXY_API/proxies/$PROXY_NAME" > /dev/null || true
    curl -s -X POST "$TOXIPROXY_API/proxies" -H "Content-Type: application/json" \
      -d "{\"name\":\"$PROXY_NAME\",\"listen\":\"0.0.0.0:$PROXIED_PG_PORT\",\"upstream\":\"postgres:5432\",\"enabled\":true}" > /dev/null

    docker exec -e PGPASSWORD=$PG_PASSWORD dtpipe-integ-postgres psql -U "$PG_USER" -d "$PG_DB" \
      -c "DROP TABLE IF EXISTS resilience_test; CREATE TABLE resilience_test (id INTEGER PRIMARY KEY, content TEXT);" > /dev/null

    TOTAL_ROWS=1000
    "$DTPIPE" -i "generate:$TOTAL_ROWS;rate=100" \
      --fake "id:random.number" --fake "content:name.fullName" \
      --drop "GenerateIndex" \
      -o "$CONN_STR" --table "resilience_test" \
      --max-retries 5 --retry-delay-ms 500 --strategy Append &
    DTPIPE_PID=$!

    sleep 3
    curl -s -X POST "$TOXIPROXY_API/proxies/$PROXY_NAME" -H "Content-Type: application/json" -d '{"enabled":false}' > /dev/null
    sleep 3
    curl -s -X POST "$TOXIPROXY_API/proxies/$PROXY_NAME" -H "Content-Type: application/json" -d '{"enabled":true}' > /dev/null

    wait $DTPIPE_PID
    EXIT_CODE=$?
    [ $EXIT_CODE -eq 0 ] || fail "Network interruption: DtPipe failed"

    COUNT=$(docker exec -e PGPASSWORD=$PG_PASSWORD dtpipe-integ-postgres psql -U "$PG_USER" -d "$PG_DB" -t \
              -c "SELECT COUNT(*) FROM resilience_test;" | xargs)
    [ "$COUNT" -eq "$TOTAL_ROWS" ] && pass "Network retry: $COUNT rows" || fail "Network retry: expected $TOTAL_ROWS, got $COUNT"
fi

# ----------------------------------------
# 4. Network interruption (Upsert strategy)
# ----------------------------------------
echo "--- [4] Network interruption (Upsert) ---"
if ! curl -s --max-time 2 "$TOXIPROXY_API/proxies" > /dev/null 2>&1; then
    skip "Toxiproxy not available — skipping upsert resilience test"
elif ! command -v docker &>/dev/null || ! docker info &>/dev/null; then
    skip "Docker not available — skipping upsert resilience test"
else
    docker exec -e PGPASSWORD=$PG_PASSWORD dtpipe-integ-postgres psql -U "$PG_USER" -d "$PG_DB" \
      -c "DROP TABLE IF EXISTS resilience_upsert; CREATE TABLE resilience_upsert (id INTEGER PRIMARY KEY, content TEXT);" > /dev/null

    TOTAL_ROWS=1000
    "$DTPIPE" -i "generate:$TOTAL_ROWS;rate=100" \
      --fake "id:random.number" --fake "content:name.fullName" \
      --drop "GenerateIndex" \
      -o "$CONN_STR" --table "resilience_upsert" \
      --max-retries 5 --retry-delay-ms 500 --strategy Upsert --key "id" &
    DTPIPE_PID=$!

    sleep 2
    curl -s -X POST "$TOXIPROXY_API/proxies/$PROXY_NAME" -H "Content-Type: application/json" -d '{"enabled":false}' > /dev/null
    sleep 4
    curl -s -X POST "$TOXIPROXY_API/proxies/$PROXY_NAME" -H "Content-Type: application/json" -d '{"enabled":true}' > /dev/null

    wait $DTPIPE_PID
    EXIT_CODE=$?
    [ $EXIT_CODE -eq 0 ] || fail "Upsert resilience: DtPipe failed"

    COUNT=$(docker exec -e PGPASSWORD=$PG_PASSWORD dtpipe-integ-postgres psql -U "$PG_USER" -d "$PG_DB" -t \
              -c "SELECT COUNT(*) FROM resilience_upsert;" | xargs)
    [ "$COUNT" -ge 900 ] && pass "Upsert resilience: $COUNT rows (>=900)" || fail "Upsert resilience: count too low ($COUNT)"
fi

echo ""
echo -e "${GREEN}Resilience validation complete!${NC}"
