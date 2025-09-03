#!/bin/bash
if command -v docker-compose &> /dev/null; then
    docker-compose build
else
    docker compose build
fi