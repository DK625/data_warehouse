{% snapshot snapshot_customers %}

{{
    config(
      unique_key='email',
      strategy='check',
      check_cols=['customer_key'],
    )
}}

select {{ dbt_utils.generate_surrogate_key(['id', 'email', 'customer_code', 'sale', 'sale_team']) }} as customer_key, id, email, customer_code, customer_type, sale, sale_team, is_test from {{ source('crm', 'customers') }}

{% endsnapshot %}