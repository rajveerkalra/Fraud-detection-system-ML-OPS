import os

import mlflow
from mlflow.tracking import MlflowClient


def main() -> None:
    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:15000")
    model_name = os.environ.get("MODEL_NAME", "fraud_model")
    alias = os.environ.get("MODEL_ALIAS", "production").lower()

    mlflow.set_tracking_uri(tracking_uri)
    client = MlflowClient()

    versions = client.search_model_versions(f"name='{model_name}'")
    if not versions:
        raise SystemExit(f"no_model_versions_found name={model_name}")

    latest = max(versions, key=lambda v: int(v.version))
    ver = str(latest.version)

    # Prefer aliases (newer MLflow); fall back to stages if needed.
    try:
        client.set_registered_model_alias(model_name, alias, ver)
        print(f"set_alias model={model_name} alias={alias} version={ver}")
        return
    except Exception as e:
        print(f"alias_set_failed err={e}")

    stage = "Production" if alias in ("prod", "production") else "Staging"
    client.transition_model_version_stage(
        name=model_name,
        version=ver,
        stage=stage,
        archive_existing_versions=False,
    )
    print(f"set_stage model={model_name} stage={stage} version={ver}")


if __name__ == "__main__":
    main()

