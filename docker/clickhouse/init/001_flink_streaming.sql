CREATE TABLE IF NOT EXISTS analytics.orders_events_enriched_queue
(
  event_id UInt64,
  user_id UInt64,
  order_ts String,
  amount Float64,
  currency String,
  amount_bucket String,
  processed_at String
)
ENGINE = Kafka
SETTINGS
  kafka_broker_list = 'kafka:9092',
  kafka_topic_list = 'orders_events_enriched',
  kafka_group_name = 'clickhouse-orders-enriched',
  kafka_format = 'JSONEachRow',
  kafka_num_consumers = 1;

CREATE TABLE IF NOT EXISTS analytics.orders_events_enriched
(
  event_id UInt64,
  user_id UInt64,
  order_ts DateTime,
  amount Decimal(12, 2),
  currency String,
  amount_bucket LowCardinality(String),
  processed_at DateTime
)
ENGINE = ReplacingMergeTree(processed_at)
ORDER BY (event_id, processed_at);

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_orders_events_enriched
TO analytics.orders_events_enriched
AS
SELECT
  event_id,
  user_id,
  parseDateTimeBestEffort(order_ts) AS order_ts,
  toDecimal64(amount, 2) AS amount,
  currency,
  amount_bucket,
  parseDateTimeBestEffort(processed_at) AS processed_at
FROM analytics.orders_events_enriched_queue;
