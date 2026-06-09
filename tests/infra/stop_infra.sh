#!/bin/bash
# Stop container infrastructure for integration tests
# This script supports both Docker and Podman.

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Detect container engine: docker or podman
if command -v docker &> /dev/null && docker info &> /dev/null; then
    ENGINE_CMD="docker"
elif command -v podman &> /dev/null && podman info &> /dev/null; then
    ENGINE_CMD="podman"
else
    echo -e "${RED}Error: Neither Docker nor Podman is installed or running${NC}"
    exit 1
fi

# Detect compose tool: docker-compose, docker compose, or podman-compose
if command -v docker-compose &> /dev/null; then
    COMPOSE_CMD="docker-compose"
elif $ENGINE_CMD compose version &> /dev/null 2>&1; then
    COMPOSE_CMD="$ENGINE_CMD compose"
elif command -v podman-compose &> /dev/null; then
    COMPOSE_CMD="podman-compose"
else
    echo -e "${RED}Error: Neither docker-compose, docker compose, nor podman-compose is available${NC}"
    exit 1
fi

# Check if docker-compose.yml exists
if [ ! -f "$COMPOSE_FILE" ]; then
    echo -e "${RED}Error: docker-compose.yml not found at $COMPOSE_FILE${NC}"
    exit 1
fi

echo -e "${YELLOW}Stopping container infrastructure with $ENGINE_CMD...${NC}"
$COMPOSE_CMD -f "$COMPOSE_FILE" down

echo -e "${GREEN}✓ Infrastructure stopped${NC}"