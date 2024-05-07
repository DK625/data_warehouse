with payment_requests as (
    select
        id,
        tenant_id,
        billing_cycle,
        month,
       {{ timestamp_to_local('due_date') }} as due_date,
        balance,
        primary_balance,
        promotion_balance,
        topup,
        this_month_debt,
        other_month_debt
    from {{ source('billing', 'payment_requests') }}
),
    accounts as (
        select * from {{ ref('dim_sale_account') }}
    ),
    white_list as (
        select email, min("date") as expires_at from {{ source('billing', 'whitelist_users') }}
        group by email
    ),
    result as (
        select
            a.id as account_id,
            pr.billing_cycle,
            pr.month,
            pr.due_date,
            pr.balance,
            pr.primary_balance,
            pr.promotion_balance,
            pr.topup,
            pr.this_month_debt,
            pr.other_month_debt
        from payment_requests as pr
        join accounts as a on a.tenant_id = pr.tenant_id
        left outer join white_list as wl on a.email = wl.email
        where wl.email is null or (wl.expires_at is not null and pr.due_date < wl.expires_at)
    )
select *
from result
