with payments as (
    select id, method, notes from {{ source('billing', 'payments') }}
)
select * from payments
