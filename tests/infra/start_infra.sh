#!/bin/bash
# Start Docker infrastructure for integration tests
# This script checks if containers are already running and healthy before starting them.

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Check if docker-compose.yml exists
if [ ! -f "$COMPOSE_FILE" ]; then
    echo -e "${RED}Error: docker-compose.yml not found at $COMPOSE_FILE${NC}"
    exit 1
fi

# Check if Docker is available
if ! command -v docker &> /dev/null; then
    echo -e "${RED}Error: Docker is not installed${NC}"
    exit 1
fi

if ! docker info &> /dev/null; then
    echo -e "${RED}Error: Docker is not running or permission denied${NC}"
    exit 1
fi

# Function to check if a container is running and healthy
is_container_healthy() {
    local container_name=$1
    
    # Check if container exists and is running
    local state=$(docker inspect --format='{{.State.Status}}' "$container_name" 2>/dev/null || echo "not-found")
    
    if [ "$state" != "running" ]; then
        return 1
    fi
    
    # Check health status (if healthcheck is defined)
    local health_status=$(docker inspect --format='{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$container_name" 2>/dev/null || echo "none")
    
    if [ "$health_status" = "healthy" ]; then
        return 0
    fi

    if [ "$health_status" = "none" ]; then
        # If no healthcheck is defined, we rely on the container being 'running'
        return 0
    fi
    
    return 1
}

# Advanced check: verify if the database is actually accepting connections/queries
is_db_ready() {
    local container=$1
    case "$container" in
        "dtpipe-integ-postgres")
            docker exec "$container" pg_isready -U postgres >/dev/null 2>&1
            return $?
            ;;
        "dtpipe-integ-mssql")
            # Using sqlcmd inside the tools sidecar (shares networking via docker-compose)
            docker exec "dtpipe-integ-mssql-tools" /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P 'Password123!' -Q "SELECT 1" >/dev/null 2>&1
            return $?
            ;;
        "dtpipe-integ-oracle")
            # Oracle free has a healthcheck script or we can use sqlplus
            if docker exec "$container" bash -c "ls /usr/local/bin/healthcheck.sh" >/dev/null 2>&1; then
                docker exec "$container" /usr/local/bin/healthcheck.sh >/dev/null 2>&1
                return $?
            else
                docker exec "$container" sqlplus -L -S / as sysdba <<< "SELECT 1 FROM DUAL;" >/dev/null 2>&1
                return $?
            fi
            ;;
        *)
            return 0 # Default to success for others
            ;;
    esac
}

# List of expected containers from docker-compose.yml
CONTAINERS=("dtpipe-integ-postgres" "dtpipe-integ-mssql" "dtpipe-integ-oracle" "dtpipe-integ-mssql-tools")

check_all_ready() {
    for container in "${CONTAINERS[@]}"; do
        if ! is_container_healthy "$container"; then
            return 1
        fi
        # Deeper check for databases
        if ! is_db_ready "$container"; then
            return 1
        fi
    done
    return 0
}

# Check if all containers are healthy and ready
if check_all_ready; then
    echo -e "${GREEN}✓ All containers are already running and healthy${NC}"
    exit 0
fi

# Start containers
echo -e "${YELLOW}Starting Docker infrastructure...${NC}"
docker compose -f "$COMPOSE_FILE" up -d

# Wait for containers to be healthy
echo -e "${YELLOW}Waiting for containers to be healthy and databases to be ready...${NC}"
max_wait=120
elapsed=0

while [ $elapsed -lt $max_wait ]; do
    if check_all_ready; then
        echo -e "\n${GREEN}✓ All containers are running and healthy${NC}"
        exit 0
    fi
    
    sleep 3
    elapsed=$((elapsed + 3))
    echo -n "."
done

echo -e "\n${RED}Error: Infrastructure failed to become healthy within ${max_wait}s${NC}"
docker ps
exit 1
