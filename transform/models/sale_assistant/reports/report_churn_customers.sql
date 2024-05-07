with fact_pr as (
    select * from {{ ref('fact_payment_requests') }}
),
    accounts as (
        select * from {{ ref('dim_sale_account') }}
    ),
    customers as (
        select email, sale, sale_team from {{ ref('dim_customer') }}
    ),
    result as (
        select
            a.email,
            a.payment_type,
            '' as "type",
            pr.billing_cycle,
            pr.month,
            pr.due_date,
            pr.balance,
            pr.topup,
            pr.this_month_debt,
            pr.other_month_debt,
            c.sale as "sale_name",
            c.sale_team,
            pr.primary_balance,
            pr.promotion_balance
        from fact_pr as pr
        join accounts as a on a.id = pr.account_id
        left outer join customers c on a.email = c.email
        where a.is_test is not true
    )
select *
from result
