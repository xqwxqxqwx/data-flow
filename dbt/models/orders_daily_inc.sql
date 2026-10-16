select
  toDate(order_ts) as order_date,
  count() as orders_cnt,
  sum(amount) as amount_sum,
  any(currency) as any_currency
from {{ ref('raw_orders_inc') }}
group by 1

