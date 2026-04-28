import os
from datetime import datetime, timezone

import mlflow
import mlflow.sklearn
import pandas as pd
from feast import FeatureStore
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, precision_score, recall_score, roc_auc_score
from sklearn.model_selection import train_test_split


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def main() -> None:
    """
    Phase 1 training pipeline:
    - Reads payments parquet (labels + entity_df)
    - Uses Feast get_historical_features to join streaming features (offline)
    - Trains baseline Logistic Regression fraud model
    - Logs + registers model in MLflow
    """

    mlflow_tracking_uri = os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:15000")
    mlflow_experiment = os.environ.get("MLFLOW_EXPERIMENT", "fraud_detection")
    model_name = os.environ.get("MODEL_NAME", "fraud_model")

    feast_repo = os.environ.get("FEAST_REPO", "feature_repo")
    payments_path = os.environ.get("PAYMENTS_PATH", "feature_repo/data/payments_v1.parquet")

    payments = pd.read_parquet(payments_path)
    payments = payments.dropna(subset=["card_id", "event_timestamp", "is_fraud"])

    # Feast entity_df needs event_timestamp and entity keys.
    # Extra columns are preserved in the returned dataframe, so we keep labels + raw amount here.
    entity_df = payments[["card_id", "event_timestamp", "amount", "is_fraud"]].copy()

    fs = FeatureStore(repo_path=feast_repo)

    feature_refs = [
        "card_velocity_1m_v1:txn_count_1m",
        "card_velocity_1m_v1:amount_sum_1m",
    ]

    joined = fs.get_historical_features(
        entity_df=entity_df,
        features=feature_refs,
    ).to_df()

    joined = joined.fillna(0)

    X = joined[["txn_count_1m", "amount_sum_1m", "amount"]].astype(float)
    y = joined["is_fraud"].astype(int)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y if y.nunique() > 1 else None
    )

    clf = LogisticRegression(max_iter=200, n_jobs=1)
    clf.fit(X_train, y_train)

    prob = clf.predict_proba(X_test)[:, 1]
    pred = (prob >= 0.5).astype(int)

    metrics = {
        "accuracy": float(accuracy_score(y_test, pred)),
        "precision": float(precision_score(y_test, pred, zero_division=0)),
        "recall": float(recall_score(y_test, pred, zero_division=0)),
    }
    if y_test.nunique() > 1:
        metrics["roc_auc"] = float(roc_auc_score(y_test, prob))

    mlflow.set_tracking_uri(mlflow_tracking_uri)
    mlflow.set_experiment(mlflow_experiment)

    with mlflow.start_run(run_name=f"train_{utc_now().isoformat()}"):
        mlflow.log_params(
            {
                "model_type": "log_reg",
                "features": ",".join(["txn_count_1m", "amount_sum_1m", "amount"]),
            }
        )
        mlflow.log_metrics(metrics)

        mlflow.sklearn.log_model(
            sk_model=clf,
            artifact_path="model",
            registered_model_name=model_name,
        )

        print(f"trained_and_logged model_name={model_name} metrics={metrics}")


if __name__ == "__main__":
    main()

