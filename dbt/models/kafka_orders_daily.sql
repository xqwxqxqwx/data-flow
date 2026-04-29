select
  toDate(order_ts) as order_date,
  count() as orders_cnt,
  sum(amount) as amount_sum
from kafka_orders_raw
group by 1
