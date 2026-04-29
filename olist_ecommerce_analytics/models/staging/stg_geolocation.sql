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
        latitude,
        longitude,
        city,
        state
    from (select
            AVG(geolocation_lat) as latitude,
            AVG(geolocation_lng) as longitude,
            REGEXP_REPLACE(
            REGEXP_REPLACE(
            REGEXP_REPLACE(
            REGEXP_REPLACE(
            REGEXP_REPLACE(
            REGEXP_REPLACE(
                TRIM(LOWER(geolocation_city)),
            '[àáâãä]', 'a'),
            '[èéêë]', 'e'),
            '[ìíîï]', 'i'),
            '[òóôõö]', 'o'),
            '[ùúûü]', 'u'),
            '[ç]', 'c')                             AS city,
            geolocation_state as state,
            from raw_geolocation
            group by geolocation_city, geolocation_state) p


