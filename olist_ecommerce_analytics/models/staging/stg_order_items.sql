with raw_order_items as (
    select
        order_id,
        order_item_id,
        product_id,
        seller_id,
        shipping_limit_date,
        price,
        freight_value
    from {{ source('raw', 'olist_order_items') }}
)
select
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,
    ROUND(price, 2)                  AS price,
    ROUND(freight_value, 2)          AS freight_value,

    -- Derived columns
    ROUND(price + freight_value, 2)  AS total_item_cost,
from raw_order_items