-- Bronze pass-through: Relic Route orders loaded from Postgres.
select
  order_id,
  customer_id,
  order_ts,
  channel,
  status,
  currency,
  total_amount,
  van_id,
  stop_id,
  updated_at
from bronze_relic_orders
