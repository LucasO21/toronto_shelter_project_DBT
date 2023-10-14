{{ 
    config(
        materialized = 'table'
    ) 
}}


select
    stn
  , date
  , min as temp_min
  , max as temp_max
  , temp as temp_avg
from {{ ref ('raw_weather_2022_view') }}
where stn = '712650'