import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import mlflow
import mlflow.xgboost
import pandas as pd
from feast import FeatureStore
from sklearn.metrics import (
    average_precision_score,
    confusion_matrix,
    precision_score,
    recall_score,
)
from sklearn.model_selection import train_test_split
from xgboost import XGBClassifier


def utc_now() -> datetime:
    return datetime.now(timezone.utc)

@dataclass(frozen=True)
class TrainConfig:
    mlflow_tracking_uri: str
    mlflow_experiment: str
    model_name: str
    feast_repo: str
    payments_path: str
    test_size: float
    seed: int


def main() -> None:
    """
    Phase 3 training pipeline (REAL dataset + XGBoost):
    - Reads real payments parquet (labels + entities)
    - Uses Feast get_historical_features to join offline windowed features (no skew)
    - Trains XGBoost with class imbalance handling
    - Logs metrics + artifacts and registers model in MLflow as `fraud_model`
    """

    cfg = TrainConfig(
        mlflow_tracking_uri=os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:15000"),
        mlflow_experiment=os.environ.get("MLFLOW_EXPERIMENT", "fraud_detection"),
        model_name=os.environ.get("MODEL_NAME", "fraud_model"),
        feast_repo=os.environ.get("FEAST_REPO", "feature_repo"),
        payments_path=os.environ.get("PAYMENTS_PATH", "feature_repo/data/payments_real.parquet"),
        test_size=float(os.environ.get("TEST_SIZE", "0.2")),
        seed=int(os.environ.get("SEED", "42")),
    )

    payments = pd.read_parquet(cfg.payments_path)
    payments = payments.dropna(subset=["card_id", "event_timestamp", "amount", "is_fraud"]).copy()
    payments["event_timestamp"] = pd.to_datetime(payments["event_timestamp"], utc=True)
    payments["amount"] = payments["amount"].astype(float)
    payments["is_fraud"] = payments["is_fraud"].astype(int)

    # Feast entity_df needs event_timestamp and entity keys.
    # Extra columns are preserved in the returned dataframe, so we keep labels + raw amount here.
    # IMPORTANT: windowed features are timestamped at window end, so we align join time
    # to the 5-minute window end to guarantee both 1m and 5m features resolve.
    entity_df = payments[["card_id", "event_timestamp", "amount", "is_fraud"]].copy()
    entity_df["event_timestamp"] = entity_df["event_timestamp"].dt.floor("5min") + pd.Timedelta(minutes=5)

    fs = FeatureStore(repo_path=cfg.feast_repo)

    feature_refs = [
        "card_velocity_1m_offline_v1:txn_count_1m",
        "card_velocity_1m_offline_v1:amount_sum_1m",
        "card_velocity_5m_v1:txn_count_5m",
        "card_velocity_5m_v1:avg_transaction_amount_5m",
    ]

    joined = fs.get_historical_features(
        entity_df=entity_df,
        features=feature_refs,
    ).to_df()

    joined = joined.fillna(0).copy()

    X = joined[
        [
            "txn_count_1m",
            "amount_sum_1m",
            "txn_count_5m",
            "avg_transaction_amount_5m",
            "amount",
        ]
    ].astype(float)
    y = joined["is_fraud"].astype(int)

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=cfg.test_size,
        random_state=cfg.seed,
        stratify=y if y.nunique() > 1 else None,
    )

    pos = int((y_train == 1).sum())
    neg = int((y_train == 0).sum())
    scale_pos_weight = float(neg / max(pos, 1))

    clf = XGBClassifier(
        n_estimators=int(os.environ.get("N_ESTIMATORS", "300")),
        max_depth=int(os.environ.get("MAX_DEPTH", "6")),
        learning_rate=float(os.environ.get("LEARNING_RATE", "0.05")),
        subsample=float(os.environ.get("SUBSAMPLE", "0.9")),
        colsample_bytree=float(os.environ.get("COLSAMPLE_BYTREE", "0.9")),
        reg_lambda=float(os.environ.get("REG_LAMBDA", "1.0")),
        min_child_weight=float(os.environ.get("MIN_CHILD_WEIGHT", "1.0")),
        scale_pos_weight=scale_pos_weight,
        objective="binary:logistic",
        eval_metric="aucpr",
        tree_method=os.environ.get("TREE_METHOD", "hist"),
        random_state=cfg.seed,
        n_jobs=2,
    )
    clf.fit(X_train, y_train)

    prob = clf.predict_proba(X_test)[:, 1]
    pred = (prob >= float(os.environ.get("THRESHOLD", "0.5"))).astype(int)

    metrics = {
        "pr_auc": float(average_precision_score(y_test, prob)),
        "precision": float(precision_score(y_test, pred, zero_division=0)),
        "recall": float(recall_score(y_test, pred, zero_division=0)),
    }

    cm = confusion_matrix(y_test, pred).tolist()

    mlflow.set_tracking_uri(cfg.mlflow_tracking_uri)
    mlflow.set_experiment(cfg.mlflow_experiment)

    with mlflow.start_run(run_name=f"train_{utc_now().isoformat()}"):
        mlflow.log_params(
            {
                "model_type": "xgboost",
                "payments_path": cfg.payments_path,
                "features": ",".join(list(X.columns)),
                "test_size": cfg.test_size,
                "seed": cfg.seed,
                "scale_pos_weight": scale_pos_weight,
            }
        )
        mlflow.log_metrics(metrics)

        # Artifacts
        artifacts_dir = Path("artifacts")
        artifacts_dir.mkdir(exist_ok=True)
        import json

        (artifacts_dir / "confusion_matrix.json").write_text(
            json.dumps({"labels": ["non_fraud", "fraud"], "matrix": cm}, indent=2),
            encoding="utf-8",
        )
        mlflow.log_artifact(str(artifacts_dir / "confusion_matrix.json"))

        mlflow.xgboost.log_model(
            xgb_model=clf,
            artifact_path="model",
            registered_model_name=cfg.model_name,
        )

        print(f"trained_and_logged model_name={cfg.model_name} metrics={metrics}")


if __name__ == "__main__":
    main()

