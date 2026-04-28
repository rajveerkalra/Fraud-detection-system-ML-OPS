import heapq
import json
import os
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

from confluent_kafka import Consumer, Producer


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap: str = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
    topic_in: str = os.getenv("KAFKA_TOPIC_IN", "payments.v1")
    topic_out: str = os.getenv("KAFKA_TOPIC_OUT", "labels.v1")
    group_id: str = os.getenv("KAFKA_GROUP_ID", "label-simulator.v1")
    offset_reset: str = os.getenv("KAFKA_OFFSET_RESET", "latest")
    label_delay_seconds: float = float(os.getenv("LABEL_DELAY_SECONDS", "60"))
    labels_log_path: str = os.getenv("LABELS_LOG_PATH", "/logs/labels.jsonl")
    flush_interval_s: float = float(os.getenv("FLUSH_INTERVAL_SECONDS", "2.0"))
    max_pending: int = int(os.getenv("MAX_PENDING", "200000"))


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def json_dumps(obj: dict) -> str:
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False)


def append_jsonl(path: str, record: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(f"{json_dumps(record)}\n")


def main() -> None:
    s = Settings()

    c = Consumer(
        {
            "bootstrap.servers": s.kafka_bootstrap,
            "group.id": s.group_id,
            "auto.offset.reset": s.offset_reset,
            "enable.auto.commit": True,
        }
    )
    p = Producer({"bootstrap.servers": s.kafka_bootstrap, "linger.ms": 5, "compression.type": "lz4"})

    # Min-heap of (due_epoch_seconds, label_record_dict)
    pending: list[tuple[float, dict]] = []
    last_flush = time.time()
    produced = 0
    dropped = 0

    c.subscribe([s.topic_in])
    print(
        f"label_simulator_started in={s.topic_in} out={s.topic_out} "
        f"delay_s={s.label_delay_seconds} log={s.labels_log_path}"
    )

    while True:
        now = time.time()

        # Emit due labels first (prevents drift if consumer is busy).
        while pending and pending[0][0] <= now:
            _, rec = heapq.heappop(pending)
            rec["label_timestamp"] = utc_now_iso()
            key = rec["event_id"].encode("utf-8")
            p.produce(s.topic_out, key=key, value=json_dumps(rec).encode("utf-8"))
            p.poll(0)
            append_jsonl(s.labels_log_path, rec)
            produced += 1

        if now - last_flush >= s.flush_interval_s:
            p.flush(0)
            last_flush = now

        msg = c.poll(0.25)
        if msg is None:
            continue
        if msg.error():
            continue

        try:
            evt = json.loads(msg.value().decode("utf-8"))
            event_id = str(evt.get("event_id") or uuid.uuid4())
            is_fraud = int(evt.get("is_fraud", 0))
            # Use a monotonic-ish due time; label_timestamp is set at emission time.
            due = time.time() + s.label_delay_seconds
            rec = {
                "event_id": event_id,
                "is_fraud": is_fraud,
                "label_timestamp": None,
                "delay_seconds": s.label_delay_seconds,
            }

            if len(pending) >= s.max_pending:
                # Drop oldest pending label to avoid unbounded memory usage in demos.
                heapq.heappop(pending)
                dropped += 1

            heapq.heappush(pending, (due, rec))

            if (produced + dropped) % 5000 == 0 and (produced + dropped) > 0:
                print(f"labels_produced={produced} dropped={dropped} pending={len(pending)}")
        except Exception as e:
            if (produced + dropped) % 1000 == 0:
                print(f"label_simulator_parse_error err={e}")


if __name__ == "__main__":
    main()

