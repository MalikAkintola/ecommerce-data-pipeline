with raw_order_payments as (
    select
        order_id,
        payment_sequential,
        payment_type,
        payment_installments,
        payment_value
    from {{ source('raw', 'olist_order_payments') }}
)
select
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    ROUND(payment_value, 2)                 as payment_value,

    -- Derived columns
    case
        when payment_installments > 1 
        then TRUE else FALSE
    end                                     as is_installment
from raw_order_payments