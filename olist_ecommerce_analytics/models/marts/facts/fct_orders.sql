{{ config(materialized='table')}}

with stg_orders as (
    select 
        order_id,
        customer_id,
        order_status,
        order_purchase_timestamp,
        approved_at,
        shipped_date,
        delivered_date,
        estimated_delivery_date,
        actual_delivery_days,
        estimated_delivery_days,
        is_delivered_on_time
    from {{ ref('stg_orders') }}
), order_items_agg as(
    select 
        order_id,
        COUNT(order_item_id) as total_items,
        sum(price) as total_items_value,
        sum(freight_value) as total_freight_value,
        sum(total_item_cost) as total_order_value
    from {{ ref('stg_order_items') }}
    group by order_id
),
orders_payments_agg as(
    select 
        order_id,
        sum(payment_value) as total_payment_value,
        MAX(payment_installments) as max_payment_installments,
        mode(payment_type) as most_used_payment_type
    from {{ ref('stg_order_payments') }}
    group by order_id
)
select 
    o.order_id,
    o.customer_id,

    CAST(TO_CHAR(o.order_purchase_timestamp, 'YYYYMMDD') AS INT)          AS ordered_date_key,
    CAST(TO_CHAR(o.approved_at, 'YYYYMMDD') AS INT)         AS approved_date_key,
    CAST(TO_CHAR(o.shipped_date, 'YYYYMMDD') AS INT)          AS shipped_date_key,
    CAST(TO_CHAR(o.delivered_date, 'YYYYMMDD') AS INT)        AS delivered_date_key,
    CAST(TO_CHAR(o.estimated_delivery_date, 'YYYYMMDD') AS INT) AS estimated_delivery_date_key,

    o.order_status,
    o.order_purchase_timestamp,
    o.approved_at,
    o.shipped_date,
    o.delivered_date,
    o.estimated_delivery_date,


    o.actual_delivery_days,
    o.estimated_delivery_days,
    o.is_delivered_on_time,

    op.max_payment_installments,
    op.total_payment_value,
    op.most_used_payment_type,

    oi.total_items,
    oi.total_items_value,
    oi.total_freight_value,
    oi.total_order_value
from stg_orders o
left join orders_payments_agg op on o.order_id = op.order_id
left join order_items_agg oi on o.order_id = oi.order_id