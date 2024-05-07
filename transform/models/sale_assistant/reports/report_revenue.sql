with usages as (
        select * from {{ ref('fact_usages') }}
    ),
    accounts as (
        select id,
            email,
            payment_type,
            is_test
        from {{ ref('dim_sale_account') }}
    ),
    first_cycle as (
        select account_id, first_cycle
        from {{ ref('dim_first_cycle')}}
    ),
    customers as (
        select * from {{ ref('dim_customer') }}
    ),
    result as (
        select
            a.email,
            a.payment_type,
            '' as "type",
            u.billing_cycle,
            u.service_type,
            u.usage_open,
            u.total_open,
            u.total_paid,
            u.subtotal,
            u.total,
            u.discount,
            fc.first_cycle,
            c.sale as "sale_name",
            c.customer_type,
            c.sale_team,
            c.is_test
        from usages as u
        join accounts as a on a.id = u.account_id
        left outer join first_cycle fc on a.id = fc.account_id
        left outer join customers c on a.email = c.email
        where a.is_test is not true
    )
select *
from result
