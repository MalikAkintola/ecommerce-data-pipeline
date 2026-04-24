with raw_orders as (
    select
        order_id,
        customer_id,
        order_status,
        order_purchase_timestamp,
        order_approved_at,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        order_estimated_delivery_date
    from {{ source('raw', 'olist_orders') }}
)
select
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at                as approved_at,
    order_delivered_carrier_date     as shipped_date,
    order_delivered_customer_date    as delivered_date,
    order_estimated_delivery_date    as estimated_delivery_date,

    -- Derived columns
    DATEDIFF('day', order_purchase_timestamp, 
        order_delivered_customer_date)              as actual_delivery_days,
    DATEDIFF('day', order_purchase_timestamp, 
        order_estimated_delivery_date)              as estimated_delivery_days,
    case 
        when order_delivered_customer_date <= order_estimated_delivery_date 
        then TRUE else FALSE 
    end                                             as is_delivered_on_time
from raw_orders