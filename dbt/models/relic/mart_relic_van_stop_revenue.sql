-- Revenue per van stop day (van-channel orders only).
select
  s.stop_date,
  s.stop_id,
  s.van_id,
  s.location_id,
  count() as orders_cnt,
  sum(o.total_amount) as gmv
from {{ ref('stg_relic_orders') }} as o
inner join {{ ref('stg_relic_van_stops') }} as s
  on o.stop_id = s.stop_id
where o.channel = 'van'
  and o.status in ('paid', 'fulfilled')
group by
  s.stop_date,
  s.stop_id,
  s.van_id,
  s.location_id
