import json
import os
import time

import pandas as pd
from confluent_kafka import Consumer


def main() -> None:
    """
    Consume Flink feature outputs from Kafka and write a Parquet file used by Feast as the offline batch_source.

    Writes: feature_repo/data/card_velocity_1m_v1.parquet
    Columns must include:
      - card_id (entity join key)
      - event_timestamp (datetime, UTC)
      - created_timestamp (datetime, UTC)
      - txn_count_1m, amount_sum_1m
    """

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")
    topic = os.environ.get("KAFKA_TOPIC", "features.card_velocity_1m.v2")
    group_id = os.environ.get("KAFKA_GROUP", f"offline-features-{int(time.time())}")
    seconds = int(os.environ.get("DURATION_SECONDS", "90"))
    out_path = os.environ.get("OUT_PATH", "feature_repo/data/card_velocity_1m_v1.parquet")

    c = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
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

        try:
            rows.append(
                {
                    "card_id": evt["card_id"],
                    "event_timestamp": pd.to_datetime(evt["event_timestamp"], utc=True, format="mixed"),
                    "created_timestamp": pd.to_datetime(evt["created_timestamp"], utc=True, format="mixed"),
                    "txn_count_1m": int(evt["txn_count_1m"]),
                    "amount_sum_1m": float(evt["amount_sum_1m"]),
                }
            )
        except Exception:
            continue

    c.close()

    df = pd.DataFrame(rows).dropna(subset=["card_id", "event_timestamp"])
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    df.to_parquet(out_path, index=False)
    print(f"wrote_features rows={len(df)} path={out_path}")


if __name__ == "__main__":
    main()

