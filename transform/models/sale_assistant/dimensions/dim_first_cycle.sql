{{
    config(
        materialized='incremental',
        unique_key='account_id'
    )
}}
with invoices as (
  SELECT
    account_id,
    billing_cycle,
    _created
  FROM {{ source('billing', 'invoices') }}
  WHERE is_trial is not TRUE
  AND account_id is not NULL
  AND status in ('open', 'paid')
),
new_invoices as (
  {% if is_incremental() %}
  select invoices.* from invoices
  left outer join {{ this }} as t on invoices.account_id = t.account_id
  where t.account_id is null
  {% else %}
  select * from invoices
  {% endif %}
),
cycle_ranking as (
  select account_id,
    billing_cycle,
    to_char(_created::timestamp without time zone at time zone 'utc' at time zone 'Asia/Ho_Chi_Minh', 'YYYY-MM') as invoice_month,
    row_number() over (
      partition by account_id
      order by _created
    ) as rank
  from new_invoices
),
first_rank as (
  select
    account_id,
    billing_cycle,
    invoice_month
  from cycle_ranking
  where rank = 1
),
final as (
  select
    fr.account_id,
    (CASE WHEN fr.billing_cycle is not NULL THEN fr.billing_cycle ELSE dbc.billing_cycle_key END) as first_cycle
  from first_rank fr
  left outer join dim_billing_cycle as dbc on dbc.month = fr.invoice_month
)
select *
from final