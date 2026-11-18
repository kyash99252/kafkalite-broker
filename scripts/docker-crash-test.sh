#!/usr/bin/env bash
set -euo pipefail

echo "[1/4] Starting broker container..."
docker compose up --build -d --wait broker

echo "[2/4] Simulating ungraceful broker termination (SIGKILL)..."
docker compose kill -s SIGKILL broker

echo "[3/4] Restarting broker against persisted volume..."
docker compose up -d --wait broker

echo "[4/4] Verifying health status..."
docker compose ps broker
echo "Container crash/restart smoke check passed."