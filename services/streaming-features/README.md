# streaming-features (Step 2 scaffold)

This service will become the **real-time feature engineering** job.

## Goal

Consume `payments.v1` from Kafka and compute windowed/velocity features (event-time) and publish:

- online feature updates (later via Feast → Redis)
- enriched events / scores (later to inference)
- DLQ for malformed events

## Next implementation steps

- Choose runtime: **Flink SQL** (fastest to iterate) vs **PyFlink DataStream** (more flexible).
- Add local Flink cluster (Docker) and Kafka connector.
- Implement initial features:
  - `card_txn_count_1m`, `card_amount_sum_5m`
  - `merchant_txn_count_5m`
  - `geo_change_flag_5m`

