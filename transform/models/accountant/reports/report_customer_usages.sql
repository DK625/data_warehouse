with decreased_balance_adjustments as (
    select * from {{ ref('report_decrease_balance_adjustments')}}
),
result as (
    select balance_adjustment_id, billing_cycle, email, service_type, invoice_id, created_at, total_amount as total
    from decreased_balance_adjustments
)
select * from result
