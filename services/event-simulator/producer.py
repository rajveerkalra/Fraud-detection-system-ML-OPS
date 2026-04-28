import argparse
import json
import math
import time
import uuid
from datetime import datetime, timezone
from random import choice, random, randint, seed

import requests
from confluent_kafka import Producer


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def register_schema(schema_registry_url: str, subject: str, avsc_path: str) -> int:
    with open(avsc_path, "r", encoding="utf-8") as f:
        schema_text = f.read()
    payload = {"schema": schema_text}
    r = requests.post(
        f"{schema_registry_url}/subjects/{subject}/versions",
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        data=json.dumps(payload),
        timeout=10,
    )
    r.raise_for_status()
    return int(r.json()["id"])


def make_event() -> dict:
    # IDs are intentionally “stringy” like production systems.
    user_id = f"user_{randint(1, 200_000)}"
    card_id = f"card_{randint(1, 300_000)}"
    merchant_id = f"m_{randint(1, 50_000)}"
    device_id = f"dev_{randint(1, 400_000)}"
    account_id = f"acct_{randint(1, 200_000)}"

    geo_pool = [
        ("US", "New_York", 40.7128, -74.0060),
        ("US", "San_Francisco", 37.7749, -122.4194),
        ("GB", "London", 51.5072, -0.1276),
        ("IN", "Bengaluru", 12.9716, 77.5946),
        ("SG", "Singapore", 1.3521, 103.8198),
        ("BR", "Sao_Paulo", -23.5505, -46.6333),
    ]
    country, city, lat, lon = choice(geo_pool)

    # Approx log-normal amount distribution without numpy.
    # Box-Muller to generate a standard normal, then exp().
    u1 = max(random(), 1e-12)
    u2 = random()
    z = math.sqrt(-2.0 * math.log(u1)) * math.cos(2.0 * math.pi * u2)
    log_mean = 3.2
    log_sigma = 0.6
    amount = round(math.exp(log_mean + log_sigma * z), 2)
    if amount < 1:
        amount = 1.0
    currency = "USD"
    mcc = choice(["5411", "5812", "5912", "5999", "5732", "4121"])
    channel = choice(["pos", "ecom"])
    is_card_present = channel == "pos"
    ip = f"203.0.113.{randint(1, 254)}"

    event_ts = utc_now_iso()
    ingest_ts = utc_now_iso()

    # Base-rate fraud simulation (we’ll make this richer in later steps).
    fraud_prob = 0.002
    if amount > 500:
        fraud_prob += 0.01
    if channel == "ecom":
        fraud_prob += 0.003
    is_fraud = 1 if random() < fraud_prob else 0

    return {
        "event_id": str(uuid.uuid4()),
        "card_id": card_id,
        "user_id": user_id,
        "merchant_id": merchant_id,
        "device_id": device_id,
        "ip": ip,
        "account_id": account_id,
        "amount": amount,
        "currency": currency,
        "mcc": mcc,
        "channel": channel,
        "is_card_present": bool(is_card_present),
        "country": country,
        "city": city,
        "lat": float(lat),
        "lon": float(lon),
        "event_ts": event_ts,
        "ingest_ts": ingest_ts,
        "is_fraud": int(is_fraud),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="payments.v1")
    ap.add_argument("--schema-registry", default="http://localhost:18081")
    ap.add_argument("--schema-path", default="schemas/payments_v1.avsc")
    ap.add_argument("--rate", type=int, default=1000, help="events/sec steady rate")
    ap.add_argument("--seconds", type=int, default=30)
    ap.add_argument("--burst-rate", type=int, default=10000)
    ap.add_argument("--burst-seconds", type=int, default=0)
    args = ap.parse_args()

    subject = f"{args.topic}-value"
    schema_id = register_schema(args.schema_registry, subject, args.schema_path)
    print(f"Registered schema id={schema_id} subject={subject}")

    p = Producer(
        {
            "bootstrap.servers": args.bootstrap,
            "linger.ms": 5,
            "batch.num.messages": 10000,
            "compression.type": "lz4",
            "queue.buffering.max.ms": 50,
        }
    )

    seed(7)

    def delivery(err, msg):
        if err is not None:
            print(f"DELIVERY_ERROR: {err}")

    def run_for(seconds: int, rate: int):
        if seconds <= 0:
            return
        interval = 1.0 / max(rate, 1)
        end = time.time() + seconds
        sent = 0
        next_t = time.time()
        while time.time() < end:
            evt = make_event()
            key = evt["card_id"].encode("utf-8")
            p.produce(
                args.topic,
                value=json.dumps(evt).encode("utf-8"),
                key=key,
                callback=delivery,
            )
            sent += 1

            next_t += interval
            sleep_for = next_t - time.time()
            if sleep_for > 0:
                time.sleep(sleep_for)

            if sent % 10000 == 0:
                p.poll(0)

        p.flush(10)
        print(f"Sent {sent} events @ ~{rate} eps for {seconds}s")

    run_for(args.seconds, args.rate)
    run_for(args.burst_seconds, args.burst_rate)


if __name__ == "__main__":
    main()
