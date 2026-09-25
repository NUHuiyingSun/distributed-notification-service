#!/usr/bin/env bash
# Blocks until all three services report UP on /actuator/health (default timeout 180s).
set -euo pipefail

TIMEOUT=${TIMEOUT:-180}
SERVICES=("api|http://localhost:8080" "dispatcher|http://localhost:8081" "worker|http://localhost:8082")

for entry in "${SERVICES[@]}"; do
  name=${entry%%|*}
  url=${entry#*|}
  printf 'Waiting for %-10s ' "${name}"
  deadline=$((SECONDS + TIMEOUT))
  until curl -fs "${url}/actuator/health" | grep -q '"status":"UP"'; do
    if (( SECONDS >= deadline )); then
      echo "TIMEOUT"
      exit 1
    fi
    printf '.'
    sleep 2
  done
  echo " UP"
done
