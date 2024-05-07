with decreased_balance_adjustments as (
    select * from {{ ref('report_decrease_balance_adjustments')}}
),
result as (
    select billing_cycle, email, sum(total_amount) as total, sum(promotion_amount) as promotion_amount, sum(primary_amount) as primary_amount
    from decreased_balance_adjustments
    group by billing_cycle, email
)
select * from result
