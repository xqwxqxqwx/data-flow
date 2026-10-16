select
  toDate(order_ts) as order_date,
  count() as orders_cnt,
  sum(amount) as amount_sum,
  avg(amount) as avg_order_amount
from {{ ref('silver_orders_stream') }}
group by 1
