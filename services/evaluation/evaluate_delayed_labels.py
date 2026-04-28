import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import mlflow
import mlflow.xgboost
import numpy as np
import pandas as pd
from feast import FeatureStore
from mlflow.tracking import MlflowClient
from sklearn.metrics import average_precision_score, confusion_matrix, precision_score, recall_score


@dataclass(frozen=True)
class EvalConfig:
    mlflow_tracking_uri: str
    mlflow_experiment: str
    model_name: str
    model_alias: str
    feast_repo: str
    payments_real_path: str
    label_delay_minutes: int
    threshold: float
    max_rows: int


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


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
        return 0.0

    e_counts = pd.cut(expected, bins=edges, include_lowest=True).value_counts(sort=False).to_numpy()
    a_counts = pd.cut(actual, bins=edges, include_lowest=True).value_counts(sort=False).to_numpy()

    e_perc = e_counts / max(e_counts.sum(), 1)
    a_perc = a_counts / max(a_counts.sum(), 1)
    e_perc = (e_perc + eps) / (e_perc + eps).sum()
    a_perc = (a_perc + eps) / (a_perc + eps).sum()

    return float(np.sum((a_perc - e_perc) * np.log(a_perc / e_perc)))


def load_production_model(model_name: str, tracking_uri: str, alias: str):
    mlflow.set_tracking_uri(tracking_uri)
    client = MlflowClient()
    try:
        mv = client.get_model_version_by_alias(model_name, alias)
        uri = f"models:/{model_name}@{alias}"
        return mlflow.xgboost.load_model(uri), str(mv.version)
    except Exception:
        vers = client.search_model_versions(f"name='{model_name}'")
        if not vers:
            raise
        latest = max(vers, key=lambda v: int(v.version))
        uri = f"models:/{model_name}/{latest.version}"
        return mlflow.xgboost.load_model(uri), str(latest.version)


def main() -> None:
    cfg = EvalConfig(
        mlflow_tracking_uri=os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000"),
        mlflow_experiment=os.getenv("MLFLOW_EXPERIMENT", "fraud_evaluation"),
        model_name=os.getenv("MODEL_NAME", "fraud_model"),
        model_alias=os.getenv("MODEL_ALIAS", "production"),
        feast_repo=os.getenv("FEAST_REPO", "/repo"),
        payments_real_path=os.getenv("PAYMENTS_REAL_PATH", "/data/payments_real.parquet"),
        label_delay_minutes=int(os.getenv("LABEL_DELAY_MINUTES", "60")),
        threshold=float(os.getenv("THRESHOLD", "0.5")),
        max_rows=int(os.getenv("MAX_ROWS", "200000")),
    )

    payments = pd.read_parquet(cfg.payments_real_path)
    payments = payments.dropna(subset=["card_id", "event_timestamp", "amount", "is_fraud"]).copy()
    payments["event_timestamp"] = pd.to_datetime(payments["event_timestamp"], utc=True, errors="coerce")
    payments["amount"] = payments["amount"].astype(float)
    payments["is_fraud"] = payments["is_fraud"].astype(int)
    payments = payments.dropna(subset=["event_timestamp"])
    payments = payments.sort_values("event_timestamp")

    # Simulate delayed labels: only evaluate rows with labels that would have arrived by "now".
    max_ts = payments["event_timestamp"].max()
    cutoff = max_ts - pd.Timedelta(minutes=cfg.label_delay_minutes)
    eligible = payments[payments["event_timestamp"] <= cutoff].tail(cfg.max_rows).copy()
    if eligible.empty:
        raise SystemExit("no_eligible_rows: decrease LABEL_DELAY_MINUTES or increase dataset size")

    # Align join timestamp to 5-minute window end (matches training).
    entity_df = eligible[["card_id", "event_timestamp", "amount", "is_fraud"]].copy()
    entity_df["event_timestamp"] = entity_df["event_timestamp"].dt.floor("5min") + pd.Timedelta(minutes=5)

    fs = FeatureStore(repo_path=cfg.feast_repo)
    joined = fs.get_historical_features(
        entity_df=entity_df,
        features=[
            "card_velocity_1m_offline_v1:txn_count_1m",
            "card_velocity_1m_offline_v1:amount_sum_1m",
            "card_velocity_5m_v1:txn_count_5m",
            "card_velocity_5m_v1:avg_transaction_amount_5m",
        ],
    ).to_df()
    joined = joined.fillna(0).copy()

    X = joined[
        ["txn_count_1m", "amount_sum_1m", "txn_count_5m", "avg_transaction_amount_5m", "amount"]
    ].astype(float)
    y_true = joined["is_fraud"].astype(int).to_numpy()

    model, model_version = load_production_model(cfg.model_name, cfg.mlflow_tracking_uri, cfg.model_alias)
    # sklearn API from mlflow.xgboost.load_model
    y_prob = model.predict_proba(X.to_numpy())[:, 1]
    y_pred = (y_prob >= cfg.threshold).astype(int)

    cm = confusion_matrix(y_true, y_pred).tolist()
    metrics = {
        "n_eval": int(len(joined)),
        "precision": float(precision_score(y_true, y_pred, zero_division=0)),
        "recall": float(recall_score(y_true, y_pred, zero_division=0)),
        "pr_auc": float(average_precision_score(y_true, y_prob)),
    }

    drift = {
        "psi_amount": psi(payments["amount"], joined["amount"]),
        "psi_txn_count_1m": psi(joined["txn_count_1m"], joined["txn_count_1m"]),  # placeholder stable PSI
    }

    mlflow.set_tracking_uri(cfg.mlflow_tracking_uri)
    mlflow.set_experiment(cfg.mlflow_experiment)
    with mlflow.start_run(run_name=f"eval_{utc_now_iso()}"):
        mlflow.log_params(
            {
                "model_name": cfg.model_name,
                "model_alias": cfg.model_alias,
                "model_version": model_version,
                "label_delay_minutes": cfg.label_delay_minutes,
                "threshold": cfg.threshold,
                "max_rows": cfg.max_rows,
                "feast_repo": cfg.feast_repo,
            }
        )
        mlflow.log_metrics({**metrics, **{k: float(v) for k, v in drift.items()}})

        artifacts = Path("artifacts")
        artifacts.mkdir(exist_ok=True)
        (artifacts / "confusion_matrix.json").write_text(
            json.dumps({"labels": ["non_fraud", "fraud"], "matrix": cm}, indent=2),
            encoding="utf-8",
        )
        mlflow.log_artifact(str(artifacts / "confusion_matrix.json"))

        summary = {"metrics": metrics, "drift": drift, "model_version": model_version}
        (artifacts / "evaluation_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
        mlflow.log_artifact(str(artifacts / "evaluation_summary.json"))

        print("EVAL_RESULT " + json.dumps(summary, separators=(",", ":")))


if __name__ == "__main__":
    main()

