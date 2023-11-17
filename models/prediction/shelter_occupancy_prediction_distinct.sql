/*
This model dedupes future predictions
*/

{{
    config(
        materialized = "table",
        schema       = "data_pred"
    )
}}

with 
    distinct_pred as (
        select
            distinct
                *
        from {{source('data_pred', 'shelter_occupancy_predictions')}}

    )

    , distinct_pred_date_format as (
      select
          *
        , parse_datetime("%F %I:%M %p", pred_time) as pred_time_dt
      from distinct_pred
  )
  
  , distinct_pred_rank as (
      select
          *
        , rank() over (partition by pkey order by pred_time_dt desc) as pred_rank
      from distinct_pred_date_format
  )
    select * from distinct_pred_rank
