
{{
    config(
        materialized = "table",
        schema       = "data_features"
    )
}}


with
  
  distinct_shelter_dates as (
    -- Gets distinct shelter occupancy dates and their primary keys.
    select
        distinct
            pkey
          , occupancy_date as occupancy_date
    from {{ref('shelter_occupancy_2022_2023_flagged_id')}}  
  )

  , shelter_dates_with_weather as (
    -- Joins the shelter dates with weather data. Identifies missing weather data for specific dates.
      select
          t1.occupancy_date
        , t1.pkey
        , t2.temp_min
        , t2.temp_max
        , t2.temp_avg
      from distinct_shelter_dates as t1
      left join {{ref('weather_historical_2022_2023')}} as t2
       on t1.occupancy_date = t2.date
  )
      
  , three_day_rolling_avg as (
    -- Calculates a rolling 3-day average temperature for min, max, and avg temps for each shelter by date.
      select
          occupancy_date
        , pkey
        , temp_min as temp_min_old
        , temp_max as temp_max_old
        , temp_avg as temp_avg_old
        , avg(temp_min) over (partition by pkey order by occupancy_date rows between 2 preceding and 1 preceding) as roll_avg_temp_min
        , avg(temp_max) over (partition by pkey order by occupancy_date rows between 2 preceding and 1 preceding) as roll_avg_temp_max
        , avg(temp_avg) over (partition by pkey order by occupancy_date rows between 2 preceding and 1 preceding) as roll_avg_temp_avg
      from shelter_dates_with_weather
  )

  , filled_weather_data as (
    -- Replaces any missing weather data with the calculated 3-day rolling average.
      select
          *
        , coalesce(temp_min_old, roll_avg_temp_min) as temp_min
        , coalesce(temp_max_old, roll_avg_temp_max) as temp_max
        , coalesce(temp_avg_old, roll_avg_temp_avg) as temp_avg
      from three_day_rolling_avg
  )
  
  , cleaned_weather_data as (
    -- Removes the old and intermediate temperature columns, leaving only the final cleaned data.
      select
        * except (
            temp_min_old
          , temp_max_old
          , temp_avg_old
          , roll_avg_temp_min
          , roll_avg_temp_max
          , roll_avg_temp_avg
        )
      from filled_weather_data
  )
  
  , final_shelter_with_weather as (
    -- Joins the original shelter data with the cleaned weather data, resulting in a combined dataset.
        select
            t1.*
          , t2.temp_min
          , t2.temp_max
          , t2.temp_avg
        from  {{ref('shelter_occupancy_2022_2023_flagged_id')}} as t1
        left join cleaned_weather_data as t2
         on t1.pkey = t2.pkey
          and t1.occupancy_date = t2.occupancy_date
  )
    select * from final_shelter_with_weather