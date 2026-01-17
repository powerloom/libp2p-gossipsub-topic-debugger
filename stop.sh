#!/bin/bash

# Determine docker compose command
if command -v docker-compose &> /dev/null; then
    DOCKER_COMPOSE_CMD="docker-compose"
else
    DOCKER_COMPOSE_CMD="docker compose"
fi

# First, force remove any stuck containers directly (bypasses docker-compose)
# This handles containers stuck in bad states (e.g., trying to pull non-existent images)
echo "🧹 Force removing any stuck containers..."
STUCK_CONTAINERS=$(docker ps -a --filter "name=p2p-debugger" --format "{{.ID}}" 2>/dev/null || true)
if [ -n "$STUCK_CONTAINERS" ]; then
    echo "$STUCK_CONTAINERS" | while read -r container_id; do
        echo "   Force removing container $container_id..."
        docker rm -f "$container_id" 2>/dev/null || true
    done
fi

# Also get containers from docker-compose and force remove them
COMPOSE_CONTAINERS=$($DOCKER_COMPOSE_CMD ps -q 2>/dev/null || true)
if [ -n "$COMPOSE_CONTAINERS" ]; then
    echo "$COMPOSE_CONTAINERS" | while read -r container_id; do
        echo "   Force removing compose container $container_id..."
        docker rm -f "$container_id" 2>/dev/null || true
    done
fi

# Now try docker-compose commands (they should work now that stuck containers are removed)
echo "🛑 Stopping containers gracefully..."
$DOCKER_COMPOSE_CMD stop --timeout 10 2>&1 | grep -v "Error\|cannot\|No such" || true

echo "🔪 Force killing any remaining containers..."
$DOCKER_COMPOSE_CMD kill 2>&1 | grep -v "Error\|cannot\|No such" || true

echo "🗑️  Removing containers and volumes..."
$DOCKER_COMPOSE_CMD down --volumes --remove-orphans 2>&1 | grep -v "Error\|cannot\|No such" || true

echo "✅ Cleanup complete"