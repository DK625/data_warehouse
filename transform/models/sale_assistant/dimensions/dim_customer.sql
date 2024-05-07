select * from {{ ref('snapshot_customers') }}
where dbt_valid_to is null
