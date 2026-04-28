-- Flink SQL: consume payments.v1 (JSON) and produce card velocity features.
--
-- Requires Kafka SQL connector jar in /opt/flink/usrlib, e.g.:
--   flink-sql-connector-kafka-3.2.0-1.19.jar

SET 'execution.checkpointing.interval' = '10s';
SET 'pipeline.name' = 'streaming_features_card_velocity_v1';

CREATE TABLE payments_v1 (
  event_id STRING,
  card_id STRING,
  user_id STRING,
  merchant_id STRING,
  device_id STRING,
  ip STRING,
  account_id STRING,
  amount DOUBLE,
  currency STRING,
  mcc STRING,
  channel STRING,
  is_card_present BOOLEAN,
  country STRING,
  city STRING,
  lat DOUBLE,
  lon DOUBLE,
  event_ts STRING,
  ingest_ts STRING,
  is_fraud INT,
  event_time AS TO_TIMESTAMP_LTZ(UNIX_TIMESTAMP(event_ts, 'yyyy-MM-dd''T''HH:mm:ss.SSSX') * 1000, 3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'payments.v1',
  'properties.bootstrap.servers' = 'kafka:29092',
  'properties.group.id' = 'flink-features-card-velocity-v1',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

CREATE TABLE card_velocity_1m_v1 (
  card_id STRING,
  event_timestamp TIMESTAMP_LTZ(3),
  created_timestamp TIMESTAMP_LTZ(3),
  txn_count_1m BIGINT,
  amount_sum_1m DOUBLE
) WITH (
  'connector' = 'kafka',
  'topic' = 'features.card_velocity_1m.v2',
  'properties.bootstrap.servers' = 'kafka:29092',
  'format' = 'json'
);

INSERT INTO card_velocity_1m_v1
SELECT
  card_id,
  window_end AS event_timestamp,
  CURRENT_TIMESTAMP AS created_timestamp,
  COUNT(*) AS txn_count_1m,
  CAST(SUM(amount) AS DOUBLE) AS amount_sum_1m
FROM TABLE(
  TUMBLE(TABLE payments_v1, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
)
GROUP BY card_id, window_start, window_end;

