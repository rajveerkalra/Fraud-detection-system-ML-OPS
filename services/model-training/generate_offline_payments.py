import json
import os
import time
from datetime import datetime, timezone

import pandas as pd
from confluent_kafka import Consumer


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def main() -> None:
    """
    Consume payments from Kafka and write a Parquet file usable as Feast entity_df for historical joins.

    Output columns:
      - card_id (entity join key)
      - event_timestamp (datetime, UTC)  [required by Feast historical retrieval]
      - amount, channel, country, city, is_fraud (labels/features for training)
    """

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")
    topic = os.environ.get("KAFKA_TOPIC", "payments.v1")
    group_id = os.environ.get("KAFKA_GROUP", f"offline-payments-{int(time.time())}")
    seconds = int(os.environ.get("DURATION_SECONDS", "70"))
    out_path = os.environ.get("OUT_PATH", "feature_repo/data/payments_v1.parquet")

    c = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": group_id,
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
        }
    )
    c.subscribe([topic])

    end = time.time() + seconds
    rows = []
    while time.time() < end:
        msg = c.poll(1.0)
        if msg is None or msg.error():
            continue
        try:
            evt = json.loads(msg.value().decode("utf-8"))
        except Exception:
            continue

        # Producer emits ISO-8601 with Z.
        ts = evt.get("event_ts")
        if not ts:
            continue
        event_timestamp = pd.to_datetime(ts, utc=True, format="mixed")

        rows.append(
            {
                "card_id": evt.get("card_id"),
                "event_timestamp": event_timestamp,
                "amount": float(evt.get("amount") or 0.0),
                "channel": evt.get("channel"),
                "country": evt.get("country"),
                "city": evt.get("city"),
                "is_fraud": int(evt.get("is_fraud") or 0),
            }
        )

    c.close()

    df = pd.DataFrame(rows).dropna(subset=["card_id", "event_timestamp"])
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    df.to_parquet(out_path, index=False)
    print(f"wrote_payments rows={len(df)} path={out_path}")


if __name__ == "__main__":
    main()

