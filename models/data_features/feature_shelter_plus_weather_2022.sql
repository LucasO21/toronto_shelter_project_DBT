-- this model combines shelter data with weather data daily for 2022


with 
    combined_cte as (
      select
        sd.*
        , wd.temp_min
        , wd.temp_max
        , wd.temp_avg
      from {{ ref ('feature_shelter_2022') }} as sd  
      left join {{ ref ('feature_weather_2022') }} as wd  
      on sd.occupancy_date = wd.date
      
    )
      select 
        * 
    from combined_cte