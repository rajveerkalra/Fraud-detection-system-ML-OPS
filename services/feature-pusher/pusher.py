import json
import os
import subprocess
import time
from datetime import datetime, timezone
from uuid import uuid4

import pandas as pd
from confluent_kafka import Consumer
from feast import FeatureStore
from feast.data_source import PushMode


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def main() -> None:
    kafka_bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")
    kafka_topic = os.environ.get("KAFKA_TOPIC", "features.card_velocity_1m.v2")
    kafka_offset_reset = os.environ.get("KAFKA_OFFSET_RESET", "earliest")
    feast_repo = os.environ.get("FEAST_REPO", "/repo")

    # Ensure repo is applied (idempotent) so registry exists + online store configured.
    subprocess.run(["feast", "-c", feast_repo, "apply"], check=True)

    store = FeatureStore(repo_path=feast_repo)
    print(f"feature_pusher_started topic={kafka_topic} offset_reset={kafka_offset_reset}", flush=True)

    c = Consumer(
        {
            "bootstrap.servers": kafka_bootstrap,
            "group.id": f"feature-pusher.card-velocity-1m.{uuid4().hex[:8]}",
            "auto.offset.reset": kafka_offset_reset,
            "enable.auto.commit": True,
        }
    )
    c.subscribe([kafka_topic])

    batch = []
    batch_max = 5000
    batch_flush_seconds = 1.0
    last_flush = time.time()

    while True:
        msg = c.poll(0.2)
        if msg is not None and not msg.error():
            try:
                payload = json.loads(msg.value().decode("utf-8"))
                batch.append(payload)
                if len(batch) == 1:
                    print(f"first_message_seen sample={payload}", flush=True)
            except Exception:
                # Ignore malformed messages; DLQ can be added later.
                pass

        now = time.time()
        should_flush = (len(batch) >= batch_max) or (batch and (now - last_flush) >= batch_flush_seconds)
        if not should_flush:
            continue

        df = pd.DataFrame(batch)
        batch = []
        last_flush = now

        # Feast push requires entity + event_timestamp column.
        # Our Flink sink provides event_timestamp + created_timestamp.
        df["event_timestamp"] = pd.to_datetime(df["event_timestamp"], utc=True, format="mixed")
        if "created_timestamp" in df.columns:
            df["created_timestamp"] = pd.to_datetime(df["created_timestamp"], utc=True, format="mixed")
        else:
            df["created_timestamp"] = utc_now()

        store.push(
            "card_velocity_1m_push_source",
            df[["card_id", "event_timestamp", "created_timestamp", "txn_count_1m", "amount_sum_1m"]],
            to=PushMode.ONLINE,
        )
        print(f"pushed_rows={len(df)} sample_card_id={df['card_id'].iloc[0] if len(df) else None}", flush=True)


if __name__ == "__main__":
    main()

