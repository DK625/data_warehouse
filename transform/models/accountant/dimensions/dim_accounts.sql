select
    id,
    email,
    payment_type,
    is_test
from {{ source('billing', 'accounts') }}