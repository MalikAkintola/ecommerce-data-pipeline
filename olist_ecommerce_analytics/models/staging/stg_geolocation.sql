with raw_geolocation as (
    select
        geolocation_zip_code_prefix,
        geolocation_lat,
        geolocation_lng,
        geolocation_city,
        geolocation_state
    from {{ source('raw', 'olist_geolocation') }}
)
select
    geolocation_zip_code_prefix as zip_code,
    geolocation_lat as latitude,
    geolocation_lng as longitude,
    geolocation_city as city,
    geolocation_state as state
from raw_geolocation