with balance_adjustments as (
    select "month", created_at, amount, balance_type, type, payment_id, account_id from {{ ref("fact_balance_adjustments") }}
    where type = 'increase'
),
payments as (
    select id, method, notes from {{ ref('dim_payments') }}
),
accounts as (
    select id, email, is_test from {{ ref('dim_accounts') }}
),
increased_adjustments as (
    select a.email,
           ba.month,
           ba.created_at,
           p.method,
           ba.amount as total_amount,
           case
             when ba.balance_type = 'primary' then ba.amount::FLOAT
             else 0
           end as primary_amount,
           case
             when ba.balance_type != 'primary' then ba.amount::FLOAT
             else 0
           end as promotion_amount,
           p.notes
    from balance_adjustments ba
    left outer join payments p on p.id = ba.payment_id
    inner join accounts a on a.id = ba.account_id
    where a.is_test is FALSE
)
select * from increased_adjustments  order by created_at
