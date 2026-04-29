{{ config(materialized='table') }}

with stg_customers as (
    select 
        seller_id,
        zip_code,
        city,
        state
    from {{ ref('stg_sellers') }}
), stg_geolocation as 
    (select 
        latitude,
        longitude,
        city,
        state
    from {{ ref('stg_geolocation') }}
)
select 
        s.seller_id,
        s.zip_code,
        s.city,
        s.state,
        g.longitude,
        g.latitude
from stg_sellers s
left join stg_geolocation g
on s.city = g.city and s.state = g.state