with updated_balance_adjustments as (
    select ba.*
    from {{ source('billing', 'balance_adjustments') }} as ba
),
balance_types as (
    select id, name
    from {{ source('billing', 'balance_types') }}
),
balance_adjustments as (
    select
           bas.id,
           bas.account_id,
           bas.invoice_id,
           bas.payment_id,
           {{convert_to_date_id('bas._created')}} as date_id,
           {{get_month('bas._created')}} as "month",
           {{timestamp_to_local('bas._created')}} as "created_at",
           {{timestamp_to_local('bas._updated')}} as "updated_at",
           bas.account_balance,
           bas.amount,
           (CASE WHEN bt.name is not null THEN bt.name ELSE 'primary' END) as balance_type,
           bas.type
    from updated_balance_adjustments bas
    left outer join balance_types as bt on bt.id = bas.balance_type_id
)

select * from balance_adjustments
