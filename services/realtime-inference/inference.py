import json
import os
import time
import uuid
from datetime import datetime, timezone

import requests
from confluent_kafka import Consumer, Producer


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def main() -> None:
    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
    in_topic = os.getenv("KAFKA_TOPIC_IN", "payments.v1")
    out_topic = os.getenv("KAFKA_TOPIC_OUT", "decisions.v1")
    group_id = os.getenv("KAFKA_GROUP_ID", "realtime-inference.v1")
    offset_reset = os.getenv("KAFKA_OFFSET_RESET", "latest")
    model_service_url = os.getenv("MODEL_SERVICE_URL", "http://model-service:8000")
    timeout_s = float(os.getenv("MODEL_TIMEOUT_SECONDS", "2.0"))

    c = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": group_id,
            "auto.offset.reset": offset_reset,
            "enable.auto.commit": True,
        }
    )
    p = Producer({"bootstrap.servers": bootstrap, "linger.ms": 5, "compression.type": "lz4"})

    c.subscribe([in_topic])
    processed = 0
    errors = 0

    while True:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            continue

        processed += 1
        try:
            evt = json.loads(msg.value().decode("utf-8"))
            event_id = evt.get("event_id") or str(uuid.uuid4())
            card_id = str(evt["card_id"])
            amount = float(evt["amount"])

            r = requests.post(
                f"{model_service_url}/predict",
                json={
                    "event_id": event_id,
                    "card_id": card_id,
                    "amount": amount,
                    "event_ts": evt.get("event_ts"),
                    "ingest_ts": evt.get("ingest_ts"),
                },
                timeout=timeout_s,
                headers={"Connection": "close"},
            )
            r.raise_for_status()
            pred = r.json()

            decision_evt = {
                "event_id": event_id,
                "card_id": card_id,
                "amount": amount,
                "event_ts": evt.get("event_ts"),
                "ingest_ts": evt.get("ingest_ts"),
                "fraud_probability": pred.get("fraud_probability"),
                "decision": pred.get("decision"),
                "model_version": pred.get("model_version"),
                "scored_at": utc_now_iso(),
            }

            p.produce(out_topic, key=card_id.encode("utf-8"), value=json.dumps(decision_evt).encode("utf-8"))
            p.poll(0)
        except Exception as e:
            errors += 1
            if errors % 50 == 0:
                print(f"errors={errors} last_error={e}")

        if processed % 1000 == 0:
            print(f"processed={processed} errors={errors}")
            p.flush(0)

        time.sleep(0)


if __name__ == "__main__":
    main()

