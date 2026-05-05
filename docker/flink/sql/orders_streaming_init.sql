SET 'execution.runtime-mode' = 'streaming';

CREATE TABLE IF NOT EXISTS orders_events_stream_src (
  event_id BIGINT,
  user_id BIGINT,
  order_ts STRING,
  amount DOUBLE,
  currency STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'orders_events_stream',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-orders-stream',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

CREATE TABLE IF NOT EXISTS orders_events_enriched_sink (
  event_id BIGINT,
  user_id BIGINT,
  order_ts STRING,
  amount DOUBLE,
  currency STRING,
  amount_bucket STRING,
  processed_at STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'orders_events_enriched',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601'
);
