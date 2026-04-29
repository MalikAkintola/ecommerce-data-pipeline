with raw_customers as (
    select
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state
    from {{ source('raw', 'olist_customers') }}
)
select 
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix as zip_code,
    REGEXP_REPLACE(
    REGEXP_REPLACE(
    REGEXP_REPLACE(
    REGEXP_REPLACE(
    REGEXP_REPLACE(
    REGEXP_REPLACE(
        TRIM(LOWER(customer_city)),
    '[àáâãä]', 'a'),
    '[èéêë]', 'e'),
    '[ìíîï]', 'i'),
    '[òóôõö]', 'o'),
    '[ùúûü]', 'u'),
    '[ç]', 'c')                             AS city,
    customer_state as state
from raw_customers