with base as (
  select
    user_id,
    updated_at as valid_from,
    leadInFrame(updated_at) over (
      partition by user_id
      order by updated_at, update_id
      rows between current row and 1 following
    ) as valid_to,
    email,
    country
  from {{ ref('raw_user_updates') }}
)
select
  user_id,
  email,
  country,
  valid_from,
  valid_to,
  valid_to is null as is_current
from base

