/*
This model combines historical daily weather data for 2022 and 2023 for Toronto.
The model also selects only the necessary features of min temp, max temp, and average temp.
*/

{{
    config(
        materialized = "table",
        schema       = "data_clean"
    )
}}

with 
    toronto_weather_2022 as (
        -- Extracts min, max, and average temperature data for Toronto for 2022.
        select
            date
          , min as temp_min
          , max as temp_max
          , temp as temp_avg
        from {{source('data_raw', 'weather_historical_view_2022')}}
        where stn = '712650'
    )

    , toronto_weather_2023 as (
        -- Extracts min, max, and average temperature data for Toronto for 2023.
        select
            date
          , min as temp_min
          , max as temp_max
          , temp as temp_avg
        from {{source('data_raw', 'weather_historical_view_2023')}}
        where stn = '712650'
    )

    , combined_weather_data as (
        -- Combines the extracted temperature data for 2022 and 2023.
          select
            distinct *
          from toronto_weather_2022

          union distinct
          
          select
            distinct *
          from toronto_weather_2023
      )
        select 
          * 
        from combined_weather_data
        order by date
