with raw_sellers as (
    select
        seller_id,
        seller_zip_code_prefix,
        seller_city,
        seller_state
    from {{ source('raw', 'olist_sellers') }}
)
select
    seller_id,
    seller_zip_code_prefix as zip_code,
    seller_city as city,
    seller_state as state
from raw_sellers