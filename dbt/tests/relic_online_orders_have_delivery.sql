-- Every online order ships; van orders are handed over on the spot.
select
  o.order_id,
  o.channel,
  d.delivery_id
from {{ ref('stg_relic_orders') }} as o
left join {{ ref('stg_relic_deliveries') }} as d
  on o.order_id = d.order_id
where (o.channel = 'online' and d.delivery_id = 0)
   or (o.channel = 'van' and d.delivery_id != 0)
