with bills as (
    select * from {{ source('billing', 'bills') }}
),
     bill_lines as (
         select * from {{ source('billing', 'bill_lines') }}
     ),
     services as (
         select * from {{ source('billing', 'services') }}
     ),
     raw_v4_invoices as (
         select
             i.invoice_id,
             billing_cycle,
             account_id,
             service_type,
             total,
             subtotal,
             (subtotal - total) as discount,
             (CASE WHEN i.status = 'paid' THEN i.total ELSE 0 END) as paid_amount,
             (CASE WHEN i.status = 'open' THEN i.total ELSE 0 END) as open_amount
         from {{ source('billing', 'v4_invoices') }} as i
         where (i.status = 'paid' or i.status = 'open')
         and is_trial is not true
    ),
    v4_invoices as (
        select
            billing_cycle,
            account_id,
            service_type,
            sum(subtotal) as subtotal,
            sum(total) as total,
            sum(total) - sum(subtotal) as discount,
            sum(paid_amount) as paid_amount,
            sum(open_amount) as open_amount
        from raw_v4_invoices as i
        group by billing_cycle, account_id, service_type
    ),
    temp_bills as (
        select b.billing_cycle, b.id, b.account_id, s.name as service_type, total, subtotal
        from bills b join services s on s.id = b.service_id
        and b.bill_type = 'usage'
        and b.status in ('active', 'closed')
        and s.name in ('cloud_server', 'dbaas', 'container_registry', 'kubernetes_engine', 'api_gateway', 'ddos', 'vod', 'kas', 'app_engine', 'lms', 'traffic_manager')
    ),
    bill_data as (
        select billing_cycle, account_id, service_type, sum(total) as total, sum(subtotal) as subtotal
        from temp_bills b
        group by billing_cycle, account_id, service_type
    ),
    bill_line_data as (
        select b.billing_cycle, b.account_id, b.service_type, sum(bl.total) as total, sum(bl.subtotal) as subtotal
        from bill_lines bl
        join temp_bills b on bl.bill_id = b.id
        group by b.billing_cycle, b.account_id, b.service_type
    ),
    v4_usages as (
        select
            bill.billing_cycle,
            bill.account_id,
            bill.service_type,
            (bill.total - coalesce(open_amount, 0)) as usage_open,
            coalesce(open_amount, 0) as total_open,
            coalesce(paid_amount, 0) as total_paid,
            coalesce(bill_line.subtotal, 0) as subtotal,
            coalesce(bill_line.total, 0) as total,
            (coalesce(bill_line.subtotal, 0) - coalesce(bill_line.total, 0)) as discount
        from bill_data as bill
        left outer join bill_line_data bill_line on bill_line.billing_cycle = bill.billing_cycle and bill_line.account_id = bill.account_id and bill_line.service_type = bill.service_type
        left outer join v4_invoices as iv on iv.billing_cycle = bill.billing_cycle and iv.account_id = bill.account_id and iv.service_type = bill.service_type
    ),
    v3_usages as (
        select
            iv.billing_cycle,
            iv.account_id,
            iv.service_type,
            0 as usage_open,
            sum(case when iv.status = 'open' then iv.total else 0 end) as total_open,
            sum(case when iv.status = 'paid' then iv.total else 0 end) as total_paid,
            sum(iv.subtotal) as subtotal,
            sum(iv.total) as total,
            (sum(iv.subtotal) - sum(iv.total)) as discount
        FROM {{ source('billing', 'invoices') }} as iv
        WHERE (status = 'open' OR status = 'paid')
          AND is_trial is not TRUE
          AND (billing_version LIKE '3%' or billing_version is null)
        group by iv.billing_cycle, iv.account_id, iv.service_type
    ),
    usages as (
        select * from v3_usages
        union all
        select * from v4_usages
    ),
    result as (
        select
            billing_cycle,
            account_id,
            service_type,
            sum(usage_open) as usage_open,
            sum(total_open) as total_open,
            sum(total_paid) as total_paid,
            sum(subtotal) as subtotal,
            sum(total) as total,
            sum(discount) as discount
        from usages
        group by billing_cycle, account_id, service_type
    )
select *
from result
