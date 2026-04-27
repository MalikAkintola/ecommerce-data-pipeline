{{ config(materialized='table') }}

with stg_customers as (
    select 
        customer_id,
        customer_unique_id,
        zip_code,
        city,
        state
    from {{ ref('stg_customers') }}
), stg_geolocation as 
    (select 
        zip_code,
        latitude,
        longitude,
        city,
        state
    from {{ ref('stg_geolocation') }}
)
select 
        c.customer_id,
        c.customer_unique_id,
        c.city,
        c.state,
        g.longitude,
        g.latitude
from stg_customers c
left join stg_geolocation g
on c.city = g.city and c.state = g.state