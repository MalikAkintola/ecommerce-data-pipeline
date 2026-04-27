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
        zip_code,
        latitude,
        longitude,
        city,
        state
    from (select
            geolocation_zip_code_prefix as zip_code,
            geolocation_lat as latitude,
            geolocation_lng as longitude,
            geolocation_city as city,
            geolocation_state as state,
            row_number() over(partition by city, state order by latitude) as rownum
            from raw_geolocation) p
where rownum = 1

