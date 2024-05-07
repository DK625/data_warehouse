{% snapshot monthly_snapshot_balances %}

{{
    config(
      unique_key='balance_snapshot_id',
      strategy='check',
      check_cols=['primary_balance', 'promotion_balance'],
    )
}}

with balance_types as (
  select * from {{ source('billing', 'balance_types') }}
),
balances as (
  select b.account_id,
        CASE
          WHEN bt.name = 'primary' THEN b.amount
        END as primary_balance,
        CASE
          WHEN bt.name != 'primary' THEN b.amount
        END as promotion_balance
  from {{ source('billing', 'balances') }} b
  left outer join balance_types bt on bt.id = b.balance_type_id
),
time_temp as (
    select NOW() as "month",
           NOW() - interval '1 month' as "prev_month"
),
current_month as (
    select {{get_month('t.month')}} as "month",
           {{get_month('t.prev_month')}} as "prev_month"
    from time_temp as t
),
result as (
  select concat(account_id, (select c.month from current_month c)) as balance_snapshot_id,
         account_id,
         (select c.prev_month from current_month c) as "month",
         sum(primary_balance) as primary_balance,
         sum(promotion_balance) as promotion_balance
  from balances
  group by account_id
)
select *
from result {% endsnapshot %}
