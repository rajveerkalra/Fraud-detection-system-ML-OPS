import argparse
import json

import fastjsonschema
from confluent_kafka import Consumer, Producer


PAYMENT_JSON_SCHEMA = {
    "type": "object",
    "required": [
        "event_id",
        "card_id",
        "user_id",
        "merchant_id",
        "device_id",
        "ip",
        "account_id",
        "amount",
        "currency",
        "mcc",
        "channel",
        "is_card_present",
        "country",
        "city",
        "lat",
        "lon",
        "event_ts",
        "ingest_ts",
        "is_fraud",
    ],
    "properties": {
        "event_id": {"type": "string"},
        "card_id": {"type": "string"},
        "user_id": {"type": "string"},
        "merchant_id": {"type": "string"},
        "device_id": {"type": "string"},
        "ip": {"type": "string"},
        "account_id": {"type": "string"},
        "amount": {"type": "number"},
        "currency": {"type": "string"},
        "mcc": {"type": "string"},
        "channel": {"type": "string", "enum": ["pos", "ecom"]},
        "is_card_present": {"type": "boolean"},
        "country": {"type": "string"},
        "city": {"type": "string"},
        "lat": {"type": "number"},
        "lon": {"type": "number"},
        "event_ts": {"type": "string"},
        "ingest_ts": {"type": "string"},
        "is_fraud": {"type": "integer", "enum": [0, 1]},
    },
}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="payments.v1")
    ap.add_argument("--group", default="validator.v1")
    ap.add_argument("--dlq-topic", default="payments.v1.dlq")
    args = ap.parse_args()

    validate = fastjsonschema.compile(PAYMENT_JSON_SCHEMA)

    c = Consumer(
        {
            "bootstrap.servers": args.bootstrap,
            "group.id": args.group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
    )
    p = Producer({"bootstrap.servers": args.bootstrap})

    c.subscribe([args.topic])
    ok = 0
    bad = 0
    try:
        while True:
            msg = c.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                continue

            try:
                evt = json.loads(msg.value().decode("utf-8"))
                validate(evt)
                ok += 1
            except Exception as e:
                bad += 1
                payload = {
                    "error": str(e),
                    "raw": msg.value().decode("utf-8", errors="replace"),
                    "topic": msg.topic(),
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                }
                p.produce(args.dlq_topic, value=json.dumps(payload).encode("utf-8"))
                p.flush(1)

            if (ok + bad) % 10000 == 0:
                print(f"validated={ok} dlq={bad}")
    finally:
        c.close()


if __name__ == "__main__":
    main()
