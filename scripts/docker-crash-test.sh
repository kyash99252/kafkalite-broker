#!/usr/bin/env bash
set -euo pipefail

echo "[1/6] Building client CLI..."
mvn -B -q -pl broker-client -am package -DskipTests

echo "[2/6] Starting broker container..."
docker compose down -v >/dev/null 2>&1 || true
docker compose up --build -d --wait broker

echo "[3/6] Seeding messages and committing partial offsets..."
java -jar broker-client/target/kafkalite-client.jar 127.0.0.1 9092 seed

echo "[4/6] Simulating ungraceful broker termination (SIGKILL)..."
docker compose kill -s SIGKILL broker

echo "[5/6] Restarting broker against persisted Docker volume..."
docker compose up -d --wait broker

echo "[6/6] Verifying message durability, offset resumption & broker status..."
java -jar broker-client/target/kafkalite-client.jar 127.0.0.1 9092 verify
java -jar broker-client/target/kafkalite-client.jar 127.0.0.1 9092 status
echo "Docker SIGKILL crash recovery verification passed (AC-11)."