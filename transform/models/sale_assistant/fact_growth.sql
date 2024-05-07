WITH OrderedData AS (
    SELECT
        f.account_id,
        f.service_type,
        f.billing_cycle,
        d.month AS billing_month,
        SUM(f.total) AS total_in_cycle,
        SUM(f.subtotal) AS subtotal_in_cycle,
        LAG(SUM(f.total), 1, 0) OVER (
            PARTITION BY f.account_id, f.service_type
            ORDER BY d.month
        ) AS previous_total,
            LAG(SUM(f.subtotal), 1, 0) OVER (
            PARTITION BY f.account_id, f.service_type
            ORDER BY d.month
        ) AS previous_subtotal
    FROM
        {{ ref('fact_usages') }} as f
            JOIN
        dim_billing_cycle d ON f.billing_cycle = d.billing_cycle_key
    GROUP BY
        f.account_id, f.service_type, f.billing_cycle, d.month
)
SELECT
    account_id,
    service_type,
    billing_cycle,
    billing_month,
    total_in_cycle,
    previous_total,
    total_in_cycle - previous_total AS growth_amount_total,
    subtotal_in_cycle,
    previous_subtotal,
    subtotal_in_cycle - previous_subtotal AS growth_amount_subtotal
FROM
    OrderedData
ORDER BY
    account_id
