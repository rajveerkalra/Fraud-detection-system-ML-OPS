import os

import pandas as pd


def utc_floor(ts: pd.Series, freq: str) -> pd.Series:
    return ts.dt.floor(freq)


def main() -> None:
    """
    Build offline feature parquet files from real ingested payments.

    Inputs:
      - feature_repo/data/payments_real.parquet

    Outputs:
      - feature_repo/data/card_velocity_1m_v1.parquet
      - feature_repo/data/card_velocity_5m_v1.parquet
    """
    payments_path = os.environ.get("PAYMENTS_PATH", "feature_repo/data/payments_real.parquet")
    out_1m = os.environ.get("OUT_1M", "feature_repo/data/card_velocity_1m_v1.parquet")
    out_5m = os.environ.get("OUT_5M", "feature_repo/data/card_velocity_5m_v1.parquet")

    payments = pd.read_parquet(payments_path)
    payments = payments.dropna(subset=["card_id", "event_timestamp", "amount"])
    payments["event_timestamp"] = pd.to_datetime(payments["event_timestamp"], utc=True)
    payments["amount"] = payments["amount"].astype(float)

    # 1-minute tumbling window features (matches Flink job shape).
    payments["window_end_1m"] = utc_floor(payments["event_timestamp"], "min") + pd.Timedelta(minutes=1)
    feats_1m = (
        payments.groupby(["card_id", "window_end_1m"], as_index=False)
        .agg(txn_count_1m=("amount", "count"), amount_sum_1m=("amount", "sum"))
        .rename(columns={"window_end_1m": "event_timestamp"})
    )
    feats_1m["created_timestamp"] = pd.Timestamp.now(tz="UTC")

    # 5-minute tumbling window features.
    payments["window_end_5m"] = utc_floor(payments["event_timestamp"], "5min") + pd.Timedelta(minutes=5)
    feats_5m = (
        payments.groupby(["card_id", "window_end_5m"], as_index=False)
        .agg(
            txn_count_5m=("amount", "count"),
            avg_transaction_amount_5m=("amount", "mean"),
        )
        .rename(columns={"window_end_5m": "event_timestamp"})
    )
    feats_5m["created_timestamp"] = pd.Timestamp.now(tz="UTC")

    os.makedirs(os.path.dirname(out_1m), exist_ok=True)
    feats_1m.to_parquet(out_1m, index=False)

    os.makedirs(os.path.dirname(out_5m), exist_ok=True)
    feats_5m.to_parquet(out_5m, index=False)

    print(f"wrote_1m_features rows={len(feats_1m)} path={out_1m}")
    print(f"wrote_5m_features rows={len(feats_5m)} path={out_5m}")


if __name__ == "__main__":
    main()

