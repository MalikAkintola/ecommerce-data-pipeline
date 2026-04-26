{{ config(materialized='table') }}

WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2016-01-01' as date)",
        end_date="cast('2019-01-01' as date)"
    ) }}
)

SELECT
    -- Keys
    DATE_PART('year', date_day) * 10000 + 
    DATE_PART('month', date_day) * 100 + 
    DATE_PART('day', date_day)              AS date_key,        
    date_day                                AS date_actual,

    -- Day details
    DATE_PART('day', date_day)              AS day_of_month,
    DATE_PART('dayofweek', date_day)        AS day_of_week,    
    DATE_PART('dayofyear', date_day)        AS day_of_year,
    DAYNAME(date_day)                       AS day_name,        

    -- Week details
    DATE_PART('week', date_day)             AS week_of_year,
    DATE_TRUNC('week', date_day)            AS week_start_date,

    -- Month details
    DATE_PART('month', date_day)            AS month_of_year,
    MONTHNAME(date_day)                     AS month_name,  
    DATE_TRUNC('month', date_day)           AS month_start_date,
    LAST_DAY(date_day)                      AS month_end_date,

    -- Quarter details
    DATE_PART('quarter', date_day)          AS quarter_of_year,
    CASE DATE_PART('quarter', date_day)
        WHEN 1 THEN 'Q1'
        WHEN 2 THEN 'Q2'
        WHEN 3 THEN 'Q3'
        WHEN 4 THEN 'Q4'
    END                                     AS quarter_name,
    DATE_TRUNC('quarter', date_day)         AS quarter_start_date,

    -- Year details
    DATE_PART('year', date_day)             AS year_actual,
    DATE_TRUNC('year', date_day)            AS year_start_date,

    -- Flags
    CASE 
        WHEN DAYNAME(date_day) IN ('Sat', 'Sun') 
        THEN TRUE ELSE FALSE 
    END                                     AS is_weekend,
    CASE 
        WHEN DAYNAME(date_day) NOT IN ('Sat', 'Sun') 
        THEN TRUE ELSE FALSE 
    END                                     AS is_weekday,
    CASE
        WHEN date_day = DATE_TRUNC('month', date_day)
        THEN TRUE ELSE FALSE
    END                                     AS is_first_day_of_month,
    CASE
        WHEN date_day = LAST_DAY(date_day)
        THEN TRUE ELSE FALSE
    END                                     AS is_last_day_of_month

FROM date_spine