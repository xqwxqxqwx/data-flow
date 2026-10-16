-- MVP mart: sales by day and channel (online vs van).
select
  toDate(o.order_ts) as order_date,
  o.channel,
  count() as orders_cnt,
  sum(o.total_amount) as gmv,
  uniqExact(o.customer_id) as customers_cnt
from {{ ref('stg_relic_orders') }} as o
where o.status in ('paid', 'fulfilled')
group by
  order_date,
  o.channel
