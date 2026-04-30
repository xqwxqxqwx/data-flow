select *
from {{ ref('silver_orders_stream') }}
where amount < 0
