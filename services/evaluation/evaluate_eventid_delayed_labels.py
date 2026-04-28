import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import mlflow
import numpy as np
import pandas as pd
from sklearn.metrics import average_precision_score, precision_score, recall_score


@dataclass(frozen=True)
class EvalCfg:
    mlflow_tracking_uri: str
    mlflow_experiment: str
    prediction_log_path: str
    labels_log_path: str
    threshold: float
    # Only evaluate predictions older than this horizon, to reduce missing joins from late labels.
    label_grace_minutes: int
    # Integrity mode
    allow_partial: bool
    max_rows: int


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def read_jsonl(path: str) -> pd.DataFrame:
    rows: list[dict] = []
    if not os.path.exists(path):
        return pd.DataFrame()
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except Exception:
                continue
    return pd.DataFrame(rows)


def main() -> None:
    cfg = EvalCfg(
        mlflow_tracking_uri=os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000"),
        mlflow_experiment=os.getenv("MLFLOW_EXPERIMENT", "fraud_evaluation_eventid"),
        prediction_log_path=os.getenv("PREDICTION_LOG_PATH", "/logs/predictions.jsonl"),
        labels_log_path=os.getenv("LABELS_LOG_PATH", "/logs/labels.jsonl"),
        threshold=float(os.getenv("THRESHOLD", "0.5")),
        label_grace_minutes=int(os.getenv("LABEL_GRACE_MINUTES", "5")),
        allow_partial=os.getenv("ALLOW_PARTIAL", "0") == "1",
        max_rows=int(os.getenv("MAX_ROWS", "200000")),
    )

    preds = read_jsonl(cfg.prediction_log_path)
    labels = read_jsonl(cfg.labels_log_path)

    if preds.empty:
        raise SystemExit(f"no_predictions_found path={cfg.prediction_log_path}")
    if labels.empty:
        raise SystemExit(f"no_labels_found path={cfg.labels_log_path}")

    # Normalize
    preds = preds.dropna(subset=["event_id", "fraud_probability"]).copy()
    preds["event_id"] = preds["event_id"].astype(str)
    preds["fraud_probability"] = pd.to_numeric(preds["fraud_probability"], errors="coerce")
    preds["ts"] = pd.to_datetime(preds.get("ts"), utc=True, errors="coerce")
    preds = preds.dropna(subset=["fraud_probability", "ts"])

    labels = labels.dropna(subset=["event_id", "is_fraud"]).copy()
    labels["event_id"] = labels["event_id"].astype(str)
    labels["is_fraud"] = pd.to_numeric(labels["is_fraud"], errors="coerce").fillna(0).astype(int)
    labels["label_timestamp"] = pd.to_datetime(labels.get("label_timestamp"), utc=True, errors="coerce")
    labels = labels.dropna(subset=["label_timestamp"])

    # Deduplicate by event_id: keep latest label (handles late corrections / re-labels).
    labels = labels.sort_values("label_timestamp").drop_duplicates(subset=["event_id"], keep="last")
    preds = preds.sort_values("ts").drop_duplicates(subset=["event_id"], keep="last")

    # Late-label handling: evaluate only predictions that *should* have labels by now.
    max_label_ts = labels["label_timestamp"].max()
    cutoff = max_label_ts - pd.Timedelta(minutes=cfg.label_grace_minutes)
    preds_eval = preds[preds["ts"] <= cutoff].tail(cfg.max_rows).copy()
    if preds_eval.empty:
        raise SystemExit("no_eligible_predictions: decrease LABEL_GRACE_MINUTES or generate more traffic")

    joined = preds_eval.merge(labels[["event_id", "is_fraud", "label_timestamp"]], on="event_id", how="left")
    missing = int(joined["is_fraud"].isna().sum())
    join_rate = float(1.0 - missing / max(len(joined), 1))

    if missing > 0 and not cfg.allow_partial:
        raise SystemExit(f"missing_joins event_id_unlabeled={missing} join_rate={join_rate:.4f}")

    joined = joined.dropna(subset=["is_fraud"]).copy()
    y_true = joined["is_fraud"].astype(int).to_numpy()
    y_prob = joined["fraud_probability"].astype(float).to_numpy()
    y_pred = (y_prob >= cfg.threshold).astype(int)

    metrics = {
        "n_predictions_total": int(len(preds)),
        "n_labels_total": int(len(labels)),
        "n_eval": int(len(joined)),
        "join_rate": float(join_rate),
        "missing_joins": int(missing),
        "precision": float(precision_score(y_true, y_pred, zero_division=0)),
        "recall": float(recall_score(y_true, y_pred, zero_division=0)),
        "pr_auc": float(average_precision_score(y_true, y_prob)) if len(np.unique(y_true)) > 1 else float("nan"),
    }

    # Useful time/lag diagnostics
    lag_s = (joined["label_timestamp"] - joined["ts"]).dt.total_seconds()
    metrics.update(
        {
            "label_lag_p50_s": float(lag_s.quantile(0.5)),
            "label_lag_p95_s": float(lag_s.quantile(0.95)),
        }
    )

    # MLflow logging
    mlflow.set_tracking_uri(cfg.mlflow_tracking_uri)
    mlflow.set_experiment(cfg.mlflow_experiment)
    with mlflow.start_run(run_name=f"eval_eventid_{utc_now_iso()}"):
        mlflow.log_params(
            {
                "threshold": cfg.threshold,
                "label_grace_minutes": cfg.label_grace_minutes,
                "allow_partial": int(cfg.allow_partial),
                "prediction_log_path": cfg.prediction_log_path,
                "labels_log_path": cfg.labels_log_path,
            }
        )
        mlflow.log_metrics(metrics)

        artifacts = Path("artifacts")
        artifacts.mkdir(exist_ok=True)
        summary = {"metrics": metrics}
        (artifacts / "eventid_evaluation_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
        mlflow.log_artifact(str(artifacts / "eventid_evaluation_summary.json"))
        print("EVAL_RESULT " + json.dumps(summary, separators=(",", ":")))


if __name__ == "__main__":
    main()

