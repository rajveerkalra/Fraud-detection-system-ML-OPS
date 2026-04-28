import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import mlflow
import numpy as np
from fastapi import FastAPI, HTTPException, Response
from feast import FeatureStore
from mlflow.tracking import MlflowClient
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest

from .schemas import PredictRequest, PredictResponse


@dataclass
class Settings:
    feast_repo: str = os.getenv("FEAST_REPO", "/repo")
    mlflow_tracking_uri: str = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
    model_name: str = os.getenv("MODEL_NAME", "fraud_model")
    decision_threshold: float = float(os.getenv("DECISION_THRESHOLD", "0.5"))
    prediction_log_path: str = os.getenv("PREDICTION_LOG_PATH", "/logs/predictions.jsonl")


settings = Settings()

app = FastAPI(title="Fraud Model Service", version="0.1.0")


_fs: FeatureStore | None = None
_model: Any | None = None
_model_version: str = "unknown"
_model_loaded_at: float = 0.0
_model_cache_seconds: float = 30.0


REQUESTS_TOTAL = Counter(
    "model_service_requests_total",
    "Total requests to model-service",
    ["endpoint", "status"],
)
PREDICTIONS_TOTAL = Counter(
    "model_service_predictions_total",
    "Total predictions emitted",
    ["decision"],
)
MODEL_RELOADS_TOTAL = Counter(
    "model_service_model_reloads_total",
    "Total MLflow model reloads",
)
FEATURE_MISSING_TOTAL = Counter(
    "model_service_feature_missing_total",
    "Count of missing online features",
    ["feature"],
)
PREDICT_LATENCY_SECONDS = Histogram(
    "model_service_predict_latency_seconds",
    "Latency of /predict",
    buckets=(0.005, 0.01, 0.02, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0),
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def append_jsonl(path: str, record: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(f"{json_dumps(record)}\n")


def json_dumps(obj: dict) -> str:
    # Compact and stable-ish for logs
    import json

    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False)


def get_feature_store() -> FeatureStore:
    global _fs
    if _fs is None:
        _fs = FeatureStore(repo_path=settings.feast_repo)
    return _fs


def _load_latest_model() -> tuple[Any, str]:
    """
    Load the latest model version from MLflow Model Registry.
    """
    mlflow.set_tracking_uri(settings.mlflow_tracking_uri)
    client = MlflowClient()
    # Prefer the latest version by version number (demo-ready).
    versions = client.search_model_versions(f"name='{settings.model_name}'")
    if not versions:
        raise RuntimeError(f"No registered model found: {settings.model_name}")
    latest = max(versions, key=lambda v: int(v.version))
    model_uri = f"models:/{settings.model_name}/{latest.version}"
    model = mlflow.sklearn.load_model(model_uri)
    return model, str(latest.version)


def get_model() -> tuple[Any, str]:
    global _model, _model_version, _model_loaded_at
    now = time.time()
    if _model is None or (now - _model_loaded_at) > _model_cache_seconds:
        model, ver = _load_latest_model()
        _model = model
        _model_version = ver
        _model_loaded_at = now
        MODEL_RELOADS_TOTAL.inc()
    return _model, _model_version


@app.get("/health")
def health() -> dict:
    return {
        "status": "ok",
        "mlflow_tracking_uri": settings.mlflow_tracking_uri,
        "model_name": settings.model_name,
        "model_version": _model_version,
    }


@app.get("/metrics")
def metrics() -> Response:
    payload = generate_latest()
    return Response(content=payload, media_type=CONTENT_TYPE_LATEST)


@app.post("/predict", response_model=PredictResponse)
def predict(req: PredictRequest) -> PredictResponse:
    t0 = time.time()
    try:
        REQUESTS_TOTAL.labels(endpoint="/predict", status="attempt").inc()
        fs = get_feature_store()
        feats = fs.get_online_features(
            features=[
                "card_velocity_1m_v1:txn_count_1m",
                "card_velocity_1m_v1:amount_sum_1m",
            ],
            entity_rows=[{"card_id": req.card_id}],
        ).to_dict()

        raw_txn = feats["txn_count_1m"][0]
        raw_sum = feats["amount_sum_1m"][0]
        if raw_txn is None:
            FEATURE_MISSING_TOTAL.labels(feature="txn_count_1m").inc()
        if raw_sum is None:
            FEATURE_MISSING_TOTAL.labels(feature="amount_sum_1m").inc()
        txn_count_1m = raw_txn or 0
        amount_sum_1m = raw_sum or 0.0

        X = np.array([[float(txn_count_1m), float(amount_sum_1m), float(req.amount)]], dtype=float)
        model, ver = get_model()
        prob = float(model.predict_proba(X)[0][1])
        decision = "fraud" if prob >= settings.decision_threshold else "approve"

        PREDICTIONS_TOTAL.labels(decision=decision).inc()
        latency = time.time() - t0
        PREDICT_LATENCY_SECONDS.observe(latency)

        # Persist minimal telemetry for offline monitoring/drift demos.
        append_jsonl(
            settings.prediction_log_path,
            {
                "ts": utc_now_iso(),
                "card_id": req.card_id,
                "amount": float(req.amount),
                "txn_count_1m": int(txn_count_1m),
                "amount_sum_1m": float(amount_sum_1m),
                "fraud_probability": prob,
                "decision": decision,
                "model_version": ver,
                "latency_ms": round(latency * 1000.0, 3),
            },
        )

        REQUESTS_TOTAL.labels(endpoint="/predict", status="ok").inc()
        return PredictResponse(
            card_id=req.card_id,
            fraud_probability=prob,
            decision=decision,
            model_version=ver,
        )
    except Exception as e:
        REQUESTS_TOTAL.labels(endpoint="/predict", status="error").inc()
        raise HTTPException(status_code=500, detail=str(e))

