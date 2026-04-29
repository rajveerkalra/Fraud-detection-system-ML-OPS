#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "[smoke] clear local sinks (predictions/labels)..."
rm -f services/model-service/logs/predictions_eventid.jsonl services/model-service/logs/labels.jsonl || true

echo "[smoke] bringing up core stack..."
docker compose up -d kafka zookeeper schema-registry redis mlflow model-service realtime-inference label-simulator prometheus grafana >/dev/null

echo "[smoke] wait for model-service /health..."
health_ok=0
for _ in $(seq 1 120); do
  if curl -sS --http1.1 -H 'Connection: close' --connect-timeout 1 --max-time 2 "http://localhost:18000/health" >/dev/null; then
    health_ok=1
    break
  fi
  sleep 2
done
if [[ "$health_ok" -ne 1 ]]; then
  echo "[smoke][fail] model-service did not become healthy in time"
  docker compose logs --tail=120 model-service realtime-inference mlflow
  exit 1
fi

echo "[smoke] wait for Kafka broker readiness..."
kafka_ok=0
for _ in $(seq 1 60); do
  if docker compose exec -T kafka kafka-broker-api-versions --bootstrap-server kafka:29092 >/dev/null 2>&1; then
    kafka_ok=1
    break
  fi
  sleep 2
done
if [[ "$kafka_ok" -ne 1 ]]; then
  echo "[smoke][fail] Kafka broker did not become ready"
  docker compose logs --tail=120 kafka zookeeper realtime-inference
  exit 1
fi

echo "[smoke] check /metrics contains model_service_*..."
if ! curl -sS --http1.1 -H 'Connection: close' "http://localhost:18000/metrics" | grep -q "model_service_predictions_total"; then
  echo "[smoke][fail] expected model_service_predictions_total in /metrics"
  exit 1
fi

echo "[smoke] produce a small batch to payments.v1..."
docker compose run --rm event-simulator >/dev/null

echo "[smoke] wait for Kafka topic decisions.v1 metadata..."
topic_ok=0
for _ in $(seq 1 45); do
  if docker compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --describe --topic decisions.v1 >/dev/null 2>&1; then
    topic_ok=1
    break
  fi
  sleep 2
done
if [[ "$topic_ok" -ne 1 ]]; then
  echo "[smoke][fail] decisions.v1 topic metadata not ready after producing events"
  docker compose logs --tail=120 kafka realtime-inference event-simulator
  exit 1
fi

echo "[smoke] wait for model-service to record some predictions..."
for _ in $(seq 1 30); do
  if curl -sS --http1.1 -H 'Connection: close' "http://localhost:18000/metrics" | grep -q "model_service_predictions_total"; then
    # This metric exists even at 0, so also check file-based sink growth by requiring decisions below.
    break
  fi
  sleep 1
done

echo "[smoke] wait for decisions.v1 to receive scored messages..."
tmp_out="$(mktemp)"
docker compose exec -T kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic decisions.v1 \
  --from-beginning \
  --max-messages 1 --timeout-ms 30000 >"$tmp_out" || true
if [[ ! -s "$tmp_out" ]]; then
  echo "[smoke][fail] no scored message observed in decisions.v1"
  docker compose logs --tail=120 realtime-inference model-service kafka
  rm -f "$tmp_out"
  exit 1
fi
rm -f "$tmp_out"

echo "[smoke] wait a bit for delayed labels..."
sleep 8

echo "[smoke] run event_id evaluation (predictions x labels join)..."
docker compose run --rm evaluation-eventid >/dev/null || {
  echo "[smoke][fail] event_id evaluation failed"
  docker compose logs --tail=120 model-service realtime-inference label-simulator
  exit 1
}

echo "[smoke] run drift-check..."
docker compose run --rm drift-check >/dev/null

echo "[smoke][ok] Phase 3 + Phase 4 smoke test passed"

