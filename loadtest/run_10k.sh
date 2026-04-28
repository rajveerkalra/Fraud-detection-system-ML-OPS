#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

docker compose up -d
docker compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --alter --topic payments.v1 --partitions 12 || true

python3 -m venv .venv
source .venv/bin/activate
pip install -q -r services/event-simulator/requirements.txt

python services/event-simulator/producer.py --rate 10000 --seconds 60
