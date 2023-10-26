
-- this model creates a view of historical weather data (2022) in the project

{{ config(materialized='view') }}

select
  stn,
  wban,
  date, 
  year,
  mo,
  da,
  temp,
  count_temp,
  dewp,
  count_dewp,
  slp,
  count_slp,
  stp,
  count_stp,
  visib,
  count_visib,
  wdsp,
  count_wdsp,
  mxpsd,
  gust,
  max,
  flag_max,
  min,
  flag_min,
  prcp,
  flag_prcp,
  sndp,
  fog,
  rain_drizzle,
  snow_ice_pellets,
  hail,
  thunder,
  tornado_funnel_cloud
from
  `bigquery-public-data.noaa_gsod.gsod2022`


