with balance_adjustments as (
    select id, "month", created_at, amount, balance_type, type, account_id, invoice_id from {{ ref("fact_balance_adjustments") }}
    where type = 'decrease'
),
accounts as (
    select id, email, is_test from {{ ref('dim_accounts') }}
),
invoices as (
    select iv.* from {{ ref('dim_invoices') }} as iv
),
decreased_adjustments as (
    select
        ba.id as balance_adjustment_id,
        a.email,
        ba.month,
        ba.created_at,
        i.service_type,
        i.billing_cycle,
        ba.amount as total_amount,
        (CASE WHEN ba.balance_type = 'primary' THEN ba.amount ELSE 0 END) as primary_amount,
        (CASE WHEN ba.balance_type != 'primary' THEN ba.amount ELSE 0 END) as promotion_amount,
        i.id as invoice_id
    from balance_adjustments ba
    join accounts a on a.id = ba.account_id
    left outer join invoices i on i.id = ba.invoice_id
    where a.is_test is FALSE
)
select * from decreased_adjustments order by email, service_type, created_at