with invoices as (
  select
      account_id,
      id,
      service_type,
      status,
      billing_cycle,
      is_trial,
      billing_version,
      _created as created_at,
      _updated as updated_at
  from {{ source('billing', 'invoices') }}
  where (status = 'open' OR status = 'paid')
)
select *
from invoices
