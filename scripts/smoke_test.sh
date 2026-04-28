#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "[smoke] bringing up core stack..."
docker compose up -d kafka zookeeper schema-registry redis mlflow model-service realtime-inference prometheus grafana >/dev/null

echo "[smoke] wait for model-service /health..."
for _ in $(seq 1 90); do
  if curl -sS --http1.1 -H 'Connection: close' --connect-timeout 1 --max-time 2 "http://localhost:18000/health" >/dev/null; then
    break
  fi
  sleep 2
done

echo "[smoke] hit /predict a few times (generate telemetry)..."
for _ in $(seq 1 10); do
  curl -sS --http1.1 -H 'Content-Type: application/json' -H 'Connection: close' \
    "http://localhost:18000/predict" \
    -d '{"card_id":"card_65759","amount":123.45}' >/dev/null
done

echo "[smoke] check /metrics contains model_service_*..."
if ! curl -sS --http1.1 -H 'Connection: close' "http://localhost:18000/metrics" | grep -q "model_service_predictions_total"; then
  echo "[smoke][fail] expected model_service_predictions_total in /metrics"
  exit 1
fi

echo "[smoke] produce a small batch to payments.v1..."
docker compose run --rm event-simulator >/dev/null

echo "[smoke] wait for decisions.v1 to receive scored messages..."
docker compose exec -T kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic decisions.v1 \
  --from-beginning \
  --max-messages 1 --timeout-ms 20000 >/dev/null

echo "[smoke] run drift-check..."
docker compose run --rm drift-check >/dev/null

echo "[smoke][ok] Phase 3 + Phase 4 smoke test passed"

