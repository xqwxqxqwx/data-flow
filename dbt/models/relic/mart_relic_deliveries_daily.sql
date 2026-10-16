-- Delivery performance per day, method and city (online orders only).
select
  toDate(d.created_at) as delivery_date,
  d.method,
  d.city,
  count() as deliveries_cnt,
  countIf(d.status = 'delivered') as delivered_cnt,
  countIf(d.status in ('pending', 'shipped')) as in_progress_cnt,
  countIf(d.status in ('cancelled', 'returned')) as failed_cnt,
  avgIf(dateDiff('hour', d.created_at, d.delivered_at), d.status = 'delivered') as avg_hours_to_deliver,
  sum(d.delivery_fee) as delivery_fee_total
from {{ ref('stg_relic_deliveries') }} as d
group by
  delivery_date,
  d.method,
  d.city
