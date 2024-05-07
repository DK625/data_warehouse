{{
    config(
        materialized='incremental',
        unique_key='report_key'
    )
}}
with accounts as (
    select id, email, is_test
    from {{ ref('dim_accounts') }}
),
billing_cycles as (
    select *
    from {{ ref('dim_billing_cycle') }}
),
balances as (
    select
        b.account_id,
        b.month,
        b.primary_balance,
        b.promotion_balance
    from {{ source('billing_snapshots', 'monthly_snapshot_balances') }} as b
    join billing_cycles as bc on bc.month = b.month
    where true
    {% if is_incremental() %}
    and bc.next_month >= (select max("month") from {{ this }})
    {% endif %}
),
max_month_cte as (
    select max("month") as max_month from balances
),
min_month_cte as (
    select bc.next_month as min_month
    from billing_cycles as bc
    where bc.month = (select min("month") from balances)
),
total_increased as (
    select
        concat(ba.account_id, ba.month) as report_key,
        ba.account_id,
        ba.month,
        sum(CASE WHEN ba.balance_type = 'primary' THEN ba.amount ELSE 0 END) as primary_amount_increased,
        sum(CASE WHEN ba.balance_type != 'primary' THEN ba.amount ELSE 0 END) as promotion_amount_increased
    from {{ ref("fact_balance_adjustments") }} ba
    where type = 'increase'
    {% if is_incremental() %}
    and ba.month >= (select max("month") from {{ this }})
    {% endif %}
    group by account_id, ba.month
),
total_decreased as (
    select
        concat(ba.account_id, ba.month) as report_key,
        ba.account_id,
        ba.month,
        sum(CASE WHEN ba.balance_type = 'primary' THEN ba.amount ELSE 0 END) as primary_amount_decreased,
        sum(CASE WHEN ba.balance_type != 'primary' THEN ba.amount ELSE 0 END) as promotion_amount_decreased
    from {{ ref("fact_balance_adjustments") }} ba
    where type = 'decrease'
    {% if is_incremental() %}
    and ba.month >= (select max("month") from {{ this }})
    {% endif %}
    group by account_id, ba.month
),
balance_adjustments as (
    select
        (
            CASE WHEN ti.account_id is not null THEN ti.account_id
            ELSE td.account_id
            END
        ) as account_id,
        (
            CASE WHEN ti.month is not null THEN ti.month
            ELSE td.month
            END
        ) as "month",
        coalesce(ti.primary_amount_increased, 0) as primary_amount_increased,
        coalesce(ti.promotion_amount_increased, 0) as promotion_amount_increased,
        coalesce(td.primary_amount_decreased, 0) as primary_amount_decreased,
        coalesce(td.promotion_amount_decreased, 0) as promotion_amount_decreased
    from total_increased as ti
    full outer join total_decreased as td on td.report_key = ti.report_key
),
result as (
    select
        concat(ba.account_id, ba.month) as report_key,
        a.email,
        ba.month,
        coalesce(b_start.primary_balance, 0) as primary_balance_start,
        coalesce(b_start.promotion_balance, 0) as promotion_balance_start,
        ba.primary_amount_increased,
        ba.promotion_amount_increased,
        ba.primary_amount_decreased,
        ba.promotion_amount_decreased,
        coalesce(b_end.primary_balance, 0) as primary_balance_end,
        coalesce(b_end.promotion_balance, 0) as promotion_balance_end
    from balance_adjustments as ba
    join billing_cycles as bc on bc.month = ba.month
    left outer join balances as b_start on b_start.account_id = ba.account_id and b_start.month = bc.previous_month
    left outer join balances as b_end on b_end.account_id = ba.account_id and b_end.month = bc.month
    left outer join accounts as a on a.id = ba.account_id
    where a.is_test is FALSE
    and ba.month <= (select max_month from max_month_cte)
    and ba.month >= (select min_month from min_month_cte)
),
final as (
    select *,
        primary_balance_start + primary_amount_increased - primary_amount_decreased as primary_balance_recalculated,
        promotion_balance_start + promotion_amount_increased - promotion_amount_decreased as promotion_balance_recalculated
    from result
)
select *
from final