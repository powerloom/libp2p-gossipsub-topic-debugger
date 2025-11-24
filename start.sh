#!/bin/bash
if command -v docker-compose &> /dev/null; then
    docker-compose build
    docker-compose up
else
    docker compose build
    docker compose up
fi