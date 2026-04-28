import argparse
import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class IngestionConfig:
    input_csv: str
    output_parquet: str
    output_report_json: str
    start_time_utc: str
    n_cards: int
    n_merchants: int
    seed: int


GEO_POOL = [
    ("US", "New_York", 40.7128, -74.0060),
    ("US", "San_Francisco", 37.7749, -122.4194),
    ("GB", "London", 51.5072, -0.1276),
    ("IN", "Bengaluru", 12.9716, 77.5946),
    ("SG", "Singapore", 1.3521, 103.8198),
    ("BR", "Sao_Paulo", -23.5505, -46.6333),
]


def parse_start_time(s: str) -> datetime:
    # Accept "2024-01-01T00:00:00Z" or "2024-01-01T00:00:00+00:00"
    if s.endswith("Z"):
        s = s.replace("Z", "+00:00")
    return datetime.fromisoformat(s).astimezone(timezone.utc)


def validate(df: pd.DataFrame) -> list[str]:
    errors: list[str] = []

    required = ["card_id", "merchant_id", "event_timestamp", "amount", "is_fraud"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        errors.append(f"missing_required_columns={missing}")

    if "amount" in df.columns:
        if df["amount"].isna().any():
            errors.append("amount_has_nulls")
        if (df["amount"] < 0).any():
            errors.append("amount_has_negative_values")
        p999 = float(df["amount"].quantile(0.999))
        if p999 > 100000:
            errors.append(f"amount_p999_too_high p999={p999}")

    if "is_fraud" in df.columns:
        bad_labels = ~df["is_fraud"].isin([0, 1])
        if bad_labels.any():
            errors.append(f"is_fraud_has_invalid_values count={int(bad_labels.sum())}")

    if "event_timestamp" in df.columns:
        if df["event_timestamp"].isna().any():
            errors.append("event_timestamp_has_nulls")
        if not pd.api.types.is_datetime64_any_dtype(df["event_timestamp"]):
            errors.append("event_timestamp_not_datetime_dtype")
        if df["event_timestamp"].dt.tz is None:
            errors.append("event_timestamp_not_utc")

    return errors


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input-csv", default=os.getenv("INPUT_CSV", "/data/creditcard.csv"))
    ap.add_argument(
        "--output-parquet",
        default=os.getenv("OUTPUT_PARQUET", "/workspace/feature_repo/data/payments_real.parquet"),
    )
    ap.add_argument(
        "--output-report-json",
        default=os.getenv("OUTPUT_REPORT_JSON", "/workspace/feature_repo/data/payments_real_report.json"),
    )
    ap.add_argument(
        "--start-time-utc",
        default=os.getenv("START_TIME_UTC", "2024-01-01T00:00:00Z"),
        help="Base timestamp used to convert dataset seconds to datetimes",
    )
    ap.add_argument("--n-cards", type=int, default=int(os.getenv("N_CARDS", "50000")))
    ap.add_argument("--n-merchants", type=int, default=int(os.getenv("N_MERCHANTS", "10000")))
    ap.add_argument("--seed", type=int, default=int(os.getenv("SEED", "7")))
    args = ap.parse_args()

    cfg = IngestionConfig(
        input_csv=args.input_csv,
        output_parquet=args.output_parquet,
        output_report_json=args.output_report_json,
        start_time_utc=args.start_time_utc,
        n_cards=args.n_cards,
        n_merchants=args.n_merchants,
        seed=args.seed,
    )

    rng = np.random.default_rng(cfg.seed)
    start = parse_start_time(cfg.start_time_utc)

    # Read only what we need for fintech mapping + label.
    # Note: "Time" is seconds since first transaction in dataset.
    raw = pd.read_csv(cfg.input_csv, usecols=["Time", "Amount", "Class"])

    raw = raw.rename(columns={"Time": "time_seconds", "Amount": "amount", "Class": "is_fraud"})
    raw["time_seconds"] = pd.to_numeric(raw["time_seconds"], errors="coerce")
    raw["amount"] = pd.to_numeric(raw["amount"], errors="coerce")
    raw["is_fraud"] = pd.to_numeric(raw["is_fraud"], errors="coerce").fillna(0).astype(int)

    raw = raw.dropna(subset=["time_seconds", "amount"])
    raw = raw[raw["amount"] >= 0].copy()

    # Map to our "payments.v1-like" fields used by the offline training pipeline.
    # We create stable pseudo-identities to support entity-based features.
    n = len(raw)
    raw["event_timestamp"] = pd.to_datetime(
        [start + timedelta(seconds=float(s)) for s in raw["time_seconds"].to_numpy()],
        utc=True,
    )

    # Stable pseudo card & merchant IDs derived from row index.
    idx = np.arange(n, dtype=np.int64)
    raw["card_id"] = pd.Series((idx % cfg.n_cards)).map(lambda x: f"card_{int(x)}")
    raw["merchant_id"] = pd.Series((idx % cfg.n_merchants)).map(lambda x: f"m_{int(x)}")

    # Add plausible categorical fields (optional, but helpful for realism).
    raw["channel"] = rng.choice(["pos", "ecom"], size=n, p=[0.7, 0.3])

    geo_idx = rng.integers(0, len(GEO_POOL), size=n)
    raw["country"] = [GEO_POOL[i][0] for i in geo_idx]
    raw["city"] = [GEO_POOL[i][1] for i in geo_idx]
    raw["lat"] = [float(GEO_POOL[i][2]) for i in geo_idx]
    raw["lon"] = [float(GEO_POOL[i][3]) for i in geo_idx]

    # Keep only columns downstream expects (plus a couple useful extras).
    out = raw[
        [
            "card_id",
            "merchant_id",
            "event_timestamp",
            "amount",
            "channel",
            "country",
            "city",
            "lat",
            "lon",
            "is_fraud",
        ]
    ].copy()

    errs = validate(out)
    report = {
        "rows": int(len(out)),
        "fraud_rate": float(out["is_fraud"].mean()) if len(out) else 0.0,
        "amount_min": float(out["amount"].min()) if len(out) else None,
        "amount_max": float(out["amount"].max()) if len(out) else None,
        "start_time_utc": cfg.start_time_utc,
        "n_cards": cfg.n_cards,
        "n_merchants": cfg.n_merchants,
        "validation_errors": errs,
        "generated_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }

    os.makedirs(os.path.dirname(cfg.output_parquet), exist_ok=True)
    out.to_parquet(cfg.output_parquet, index=False)

    os.makedirs(os.path.dirname(cfg.output_report_json), exist_ok=True)
    with open(cfg.output_report_json, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, sort_keys=True)

    if errs:
        raise SystemExit(f"ingestion_validation_failed errors={errs}")

    print(f"wrote_parquet path={cfg.output_parquet} rows={len(out)} fraud_rate={report['fraud_rate']:.6f}")
    print(f"wrote_report path={cfg.output_report_json}")


if __name__ == "__main__":
    main()

