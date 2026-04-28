import os
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd


def utc_floor_minute(ts: pd.Series) -> pd.Series:
    return ts.dt.floor("min")


def main() -> None:
    """
    Create a deterministic offline dataset for training + Feast offline sources.

    Outputs:
      - feature_repo/data/payments_v1.parquet
      - feature_repo/data/card_velocity_1m_v1.parquet
    """

    out_payments = os.environ.get("OUT_PAYMENTS", "feature_repo/data/payments_v1.parquet")
    out_features = os.environ.get("OUT_FEATURES", "feature_repo/data/card_velocity_1m_v1.parquet")
    n = int(os.environ.get("N_ROWS", "200000"))
    seed = int(os.environ.get("SEED", "7"))

    rng = np.random.default_rng(seed)

    start = datetime.now(timezone.utc) - timedelta(hours=2)
    # random seconds within 2 hours
    offsets = rng.integers(0, 2 * 60 * 60, size=n)
    event_ts = pd.to_datetime([start + timedelta(seconds=int(x)) for x in offsets], utc=True)

    card_id = pd.Series(rng.integers(1, 300_000, size=n)).map(lambda x: f"card_{x}")
    amount = np.round(rng.lognormal(mean=3.2, sigma=0.6, size=n), 2)
    channel = rng.choice(["pos", "ecom"], size=n, p=[0.7, 0.3])

    # simple fraud label with patterns
    base = rng.random(size=n) < 0.002
    high_amount = amount > 500
    is_ecom = channel == "ecom"
    is_fraud = (base | (high_amount & (rng.random(size=n) < 0.2)) | (is_ecom & (rng.random(size=n) < 0.01))).astype(int)

    payments = pd.DataFrame(
        {
            "card_id": card_id,
            "event_timestamp": event_ts,
            "amount": amount.astype(float),
            "channel": channel,
            "is_fraud": is_fraud.astype(int),
        }
    )

    os.makedirs(os.path.dirname(out_payments), exist_ok=True)
    payments.to_parquet(out_payments, index=False)

    # Build 1-minute tumbling window features like the Flink job.
    payments["window_end"] = utc_floor_minute(payments["event_timestamp"]) + pd.Timedelta(minutes=1)
    feats = (
        payments.groupby(["card_id", "window_end"], as_index=False)
        .agg(txn_count_1m=("amount", "count"), amount_sum_1m=("amount", "sum"))
        .rename(columns={"window_end": "event_timestamp"})
    )
    feats["created_timestamp"] = pd.Timestamp.now(tz="UTC")

    feats.to_parquet(out_features, index=False)

    print(f"wrote_payments rows={len(payments)} path={out_payments}")
    print(f"wrote_features rows={len(feats)} path={out_features}")


if __name__ == "__main__":
    main()

