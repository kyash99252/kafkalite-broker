#!/usr/bin/env bash
set -euo pipefail
java -cp broker-core/target/kafkalite-broker.jar com.kafkalite.core.lifecycle.LogInspector "$@"