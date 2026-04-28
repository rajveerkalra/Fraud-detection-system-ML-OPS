import json
import os
from dataclasses import dataclass
from typing import Iterable

import numpy as np
import pandas as pd


@dataclass
class Settings:
    payments_path: str = os.getenv("PAYMENTS_PATH", "/data/payments_v1.parquet")
    offline_features_path: str = os.getenv("OFFLINE_FEATURES_PATH", "/data/card_velocity_1m_v1.parquet")
    prediction_log_path: str = os.getenv("PREDICTION_LOG_PATH", "/logs/predictions.jsonl")
    recent_rows: int = int(os.getenv("RECENT_ROWS", "5000"))


def psi(expected: pd.Series, actual: pd.Series, bins: int = 10, eps: float = 1e-6) -> float:
    expected = pd.to_numeric(expected, errors="coerce").dropna()
    actual = pd.to_numeric(actual, errors="coerce").dropna()
    if expected.empty or actual.empty:
        return float("nan")

    qs = pd.Series(range(0, bins + 1)) / bins
    edges = expected.quantile(qs, interpolation="linear").to_numpy()
    edges[0] = -float("inf")
    edges[-1] = float("inf")
    edges = np.unique(edges)
    if len(edges) < 3:
        # Expected distribution is (near) constant; PSI isn't meaningful.
        return 0.0

    e_counts = pd.cut(expected, bins=edges, include_lowest=True).value_counts(sort=False).to_numpy()
    a_counts = pd.cut(actual, bins=edges, include_lowest=True).value_counts(sort=False).to_numpy()

    e_perc = e_counts / max(e_counts.sum(), 1)
    a_perc = a_counts / max(a_counts.sum(), 1)
    e_perc = (e_perc + eps) / (e_perc + eps).sum()
    a_perc = (a_perc + eps) / (a_perc + eps).sum()

    return float(np.sum((a_perc - e_perc) * np.log(a_perc / e_perc)))


def read_jsonl(path: str) -> Iterable[dict]:
    if not os.path.exists(path):
        return []
    out: list[dict] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except Exception:
                continue
    return out


def main() -> None:
    s = Settings()

    payments = pd.read_parquet(s.payments_path, columns=["amount"])
    offline_feats = pd.read_parquet(s.offline_features_path, columns=["txn_count_1m", "amount_sum_1m"])

    recent = list(read_jsonl(s.prediction_log_path))[-s.recent_rows :]
    recent_df = pd.DataFrame.from_records(recent)
    if recent_df.empty:
        print("drift_check: no recent prediction logs found (produce traffic first)")
        return

    results = {
        "psi_amount": psi(payments["amount"], recent_df["amount"]),
        "psi_txn_count_1m": psi(offline_feats["txn_count_1m"], recent_df["txn_count_1m"]),
        "psi_amount_sum_1m": psi(offline_feats["amount_sum_1m"], recent_df["amount_sum_1m"]),
        "n_recent": int(len(recent_df)),
    }

    print(json.dumps(results, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

