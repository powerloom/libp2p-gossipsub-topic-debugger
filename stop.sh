#!/bin/bash
if command -v docker-compose &> /dev/null; then
    docker-compose down --volumes --remove-orphans
else
    docker compose down --volumes --remove-orphans
fi