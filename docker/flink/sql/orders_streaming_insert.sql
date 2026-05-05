INSERT INTO orders_events_enriched_sink
SELECT
  event_id,
  user_id,
  order_ts,
  amount,
  currency,
  'mid' AS amount_bucket,
  CAST(CURRENT_TIMESTAMP AS STRING) AS processed_at
FROM orders_events_stream_src;
