
-- clean_weather_no_nulls_2023
-- this model fills the rows in 2023 weather data where values are null, will a 3 day rolling average
-- the max date for historical weather data is 4 days before the max data for historical shelter data
-- these 4 days are the rows be filled with rolling averages

{{ 
    config(
        materialized='table'
    ) 
}}

with
    shelter_dates_cte as (
        select
            distinct
              date(occupancy_date) as occupancy_date
        from {{ ref ('clean_shelter_2023') }}  ---`toronto-shelter-project.data_clean.clean_shelter_2023`
    
    )

    , weather_dates_cte as (
        select
              date(date) as weather_date
            , temp_min
            , temp_max
            , temp_avg
        from {{ ref ('clean_weather_2023') }} ---`toronto-shelter-project.data_clean.clean_weather_2023`
    )

    , combine_cte as (
        select
              sd.occupancy_date
            , wd.temp_min
            , wd.temp_max
            , wd.temp_avg
        from shelter_dates_cte as sd
        left join weather_dates_cte as wd
            on sd.occupancy_date = wd.weather_date
    )
    
    , avg_temp_cte as (
        select
              occupancy_date
            , temp_min
            , temp_max
            , temp_avg
            , avg(temp_min) over (order by occupancy_date rows between 3 preceding and 1 preceding) as roll_avg_temp_min
            , avg(temp_max) over (order by occupancy_date rows between 3 preceding and 1 preceding) as roll_avg_temp_max
            , avg(temp_avg) over (order by occupancy_date rows between 3 preceding and 1 preceding) as roll_avg_temp_avg
        from combine_cte
    )
    

    , fill_nulls_cte as (
        select
              occupancy_date
            , coalesce(temp_min, roll_avg_temp_min) as temp_min
            , coalesce(temp_max, roll_avg_temp_max) as temp_max
            , coalesce(temp_avg, roll_avg_temp_avg) as temp_avg
        from avg_temp_cte
    )
        select * from fill_nulls_cte



