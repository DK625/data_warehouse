with accounts as (select id,
                         tenant_id,
                         email,
                         payment_type,
                         is_test
                  from {{ source('billing', 'accounts') }}),
     first_cycle as (select account_id, first_cycle
                     from {{ ref('dim_first_cycle')}}),
     result as (select a.id,
                       a.email,
                       a.tenant_id,
                       a.payment_type,
                       fc.first_cycle,
                       a.is_test
                from accounts a
                         left outer join first_cycle fc on a.id = fc.account_id)
select *
from result