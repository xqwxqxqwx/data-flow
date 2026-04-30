{% set kafka_raw = adapter.get_relation(
    database=target.database,
    schema=target.schema,
    identifier='kafka_orders_raw'
) %}

{% if kafka_raw is not none %}
select
  event_id,
  user_id,
  order_ts,
  amount,
  currency,
  now() as ingest_ts
from {{ kafka_raw }}
{% else %}
select
  toUInt64(0) as event_id,
  toUInt64(0) as user_id,
  toDateTime('1970-01-01 00:00:00') as order_ts,
  toDecimal64(0, 2) as amount,
  '' as currency,
  toDateTime('1970-01-01 00:00:00') as ingest_ts
where 1 = 0
{% endif %}
