select
  order_id,
  argMax(user_id, ts_ms) as user_id,
  argMax(order_ts, ts_ms) as order_ts,
  argMax(amount, ts_ms) as amount,
  argMax(currency, ts_ms) as currency,
  max(ts_ms) as last_ts_ms
from cdc_orders_events
where coalesce(op, '') != 'd'
group by order_id

