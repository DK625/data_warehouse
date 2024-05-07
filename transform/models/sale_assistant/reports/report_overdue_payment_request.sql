with fact_pr as (
    select * from {{ ref('fact_payment_requests') }}
),
    accounts as (
        select * from {{ ref('dim_sale_account') }}
    ),
    result as (
        select
            a.email,
            pr.billing_cycle,
            pr.month,
            pr.due_date,
            pr.balance,
            pr.topup,
            pr.this_month_debt,
            pr.other_month_debt,
            pr.primary_balance
        from fact_pr as pr
        join accounts as a on a.id = pr.account_id
        where a.is_test is not true
    )
select *
from result
