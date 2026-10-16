with ranked as (
  select
    event_id,
    user_id,
    order_ts,
    amount,
    currency,
    ingest_ts,
    row_number() over (partition by event_id order by ingest_ts desc) as rn
  from {{ ref('bronze_orders_stream') }}
)
select
  event_id,
  user_id,
  order_ts,
  amount,
  currency,
  ingest_ts
from ranked
where rn = 1
  and amount >= 0
