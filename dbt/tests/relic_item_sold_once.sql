-- A unique vintage piece must not be in two live (non-cancelled) orders.
select
  oi.item_id,
  count() as live_orders
from {{ ref('stg_relic_order_items') }} as oi
inner join {{ ref('stg_relic_orders') }} as o
  on oi.order_id = o.order_id
where o.status != 'cancelled'
group by oi.item_id
having live_orders > 1
