with raw_products as (
    select
        product_id,
        product_category_name,
        product_name_length,
        product_description_length,
        product_photos_qty,
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm
    from {{ source('raw', 'olist_products') }}
),
raw_translated_categories as (
    select
        product_category_name,
        product_category_name_english
    from {{ source('raw', 'olist_product_category_name_translation') }}
)
select
    p.product_id,
    coalesce(p.product_category_name, 'Unknown') as product_category_name_portuguese,
    coalesce(t.product_category_name_english, p.product_category_name, 'Unknown') as product_category_name_english,
    p.product_name_length,
    p.product_description_length,
    p.product_photos_qty,
    round(p.product_weight_g, 2) as product_weight_g,
    round(p.product_length_cm, 2) as product_length_cm,
    round(p.product_height_cm, 2) as product_height_cm,
    round(p.product_width_cm, 2) as product_width_cm
from raw_products p
left join raw_translated_categories t
    on p.product_category_name = t.product_category_name