#!/usr/bin/env bash
set -euo pipefail

mvn -B -q -pl broker-benchmarks -am package -DskipTests

if [[ "${1:-}" == "--docker" ]]; then
  echo "Running benchmarks against Docker broker on 127.0.0.1:9092..."
  docker compose up --build -d --wait broker
  java -jar broker-benchmarks/target/kafkalite-benchmarks.jar remote 127.0.0.1 9092 10000 512
else
  java -jar broker-benchmarks/target/kafkalite-benchmarks.jar 10000 512
fi