#!/bin/bash
set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
DTPIPE_BIN="$PROJECT_ROOT/dist/release/dtpipe"

# Toxiproxy Configuration
TOXIPROXY_API="http://localhost:8474"
PROXY_NAME="postgres_proxy"
PROXIED_PG_PORT=5441

# Postgres Connection (via Proxy)
PG_HOST="localhost"
PG_PORT=$PROXIED_PG_PORT
PG_USER="postgres"
PG_PASSWORD="password"
PG_DB="integration"
CONN_STR="pg:Host=$PG_HOST;Port=$PG_PORT;Username=$PG_USER;Password=$PG_PASSWORD;Database=$PG_DB;Pooling=false;CommandTimeout=5;Timeout=5"

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

echo "========================================"
echo "    DtPipe Upsert Resilience Test"
echo "========================================"

# 1. Setup Toxiproxy
echo "🔧 Setting up Toxiproxy..."
curl -s -X DELETE "$TOXIPROXY_API/proxies/$PROXY_NAME" > /dev/null || true
curl -s -X POST "$TOXIPROXY_API/proxies" \
    -H "Content-Type: application/json" \
    -d '{
        "name": "'$PROXY_NAME'",
        "listen": "0.0.0.0:5441",
        "upstream": "postgres:5432",
        "enabled": true
    }' > /dev/null

echo "✓ Toxiproxy ready on port $PROXIED_PG_PORT"

# 2. Initialize Database for Upsert Test
echo "🧹 Initializing PostgreSQL Table..."
docker exec -e PGPASSWORD=$PG_PASSWORD dtpipe-integ-postgres psql -U "$PG_USER" -d "$PG_DB" -c "DROP TABLE IF EXISTS resilience_upsert; CREATE TABLE resilience_upsert (id INTEGER PRIMARY KEY, content TEXT);" > /dev/null

# 3. Run DtPipe with rate limiting
TOTAL_ROWS=1000
RATE=100
echo "🚀 [DtPipe] Starting Upsert export ($TOTAL_ROWS rows @ $RATE rows/s)..."

"$DTPIPE_BIN" --input "generate:$TOTAL_ROWS;rate=$RATE" \
    --fake "id:random.number" --fake "content:name.fullName" \
    --drop "GenerateIndex" \
    --output "$CONN_STR" \
    --table "resilience_upsert" \
    --max-retries 5 \
    --retry-delay-ms 500 \
    --strategy Upsert \
    --key "id" &
DTPIPE_PID=$!

set -x
sleep 2
echo "🔥 [Toxiproxy] DISABLING network..."
curl -s -i -X POST "$TOXIPROXY_API/proxies/$PROXY_NAME" \
    -H "Content-Type: application/json" \
    -d '{"enabled": false}'

sleep 4
echo "🟢 [Toxiproxy] ENABLING network..."
curl -s -i -X POST "$TOXIPROXY_API/proxies/$PROXY_NAME" \
    -H "Content-Type: application/json" \
    -d '{"enabled": true}'

wait $DTPIPE_PID
EXIT_CODE=$?
set +x

if [ $EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}✓ DtPipe succeeded!${NC}"
    
    # 4. Verify data count
    # Note: Using random numbers for ID might cause collisions, so count might be less than TOTAL_ROWS
    # But since we generate 1000 rows, and random number range is default (large), collisions are rare but possible.
    # To be precise, we should check that we have SOME data.
    COUNT=$(docker exec -e PGPASSWORD=$PG_PASSWORD dtpipe-integ-postgres psql -U "$PG_USER" -d "$PG_DB" -t -c "SELECT COUNT(*) FROM resilience_upsert;" | xargs)
    echo -e "${GREEN}✓ Final row count: $COUNT${NC}"
    
    # Check if we have at least 90% of rows (allowing for some collisions if random range is small, 
    # but normally default random is int32 range so collisions are negligible for 1000 rows)
    if [ "$COUNT" -ge 900 ]; then
         echo -e "${GREEN}✓ Data integrity check passed${NC}"
    else
         echo -e "${RED}✗ Data count too low ($COUNT). Something went wrong.${NC}"
         exit 1
    fi
else
    echo -e "${RED}✗ DtPipe Failed with exit code $EXIT_CODE${NC}"
    exit 1
fi
