import os

from feast import FeatureStore


def main() -> None:
    feast_repo = os.environ.get("FEAST_REPO", "/repo")
    store = FeatureStore(repo_path=feast_repo)

    card_id = os.environ.get("CARD_ID", "card_1")

    features = [
        "card_velocity_1m_v1:txn_count_1m",
        "card_velocity_1m_v1:amount_sum_1m",
    ]
    resp = store.get_online_features(
        features=features,
        entity_rows=[{"card_id": card_id}],
    ).to_dict()
    print(resp)


if __name__ == "__main__":
    main()

