-- This model gets features needed for predicting overnight shelter occupancy for the next 5 days. 

{{ 
    config(
        materialized = 'table',
        schema       = 'data_features'
    ) 
}}

with 
    max_date_cte as (
        select
          max(date(occupancy_date)) as end_date
        from {{ref ('feature_shelter_2023')}} --`toronto-shelter-project.data_features.feature_shelter_2023`
    )

    , max_date_minus_7_cte as (
      select
        date_add(end_date, interval -6 day) as start_date
      from max_date_cte
    )
    
    , last_7_days_avg as (
        select
          --distinct
          --date(occupancy_date) as date
          organization_id
        , shelter_id
        , location_id
        , program_id
        , sector
        , program_model
        , program_area
        , overnight_service_type
        , capacity_type
        , round(avg(capacity_actual)) as avg_capacity_actual_l7d
        , round(avg(occupied)) as avg_occupied_l7d
        , round(avg(occupancy_rate)) as avg_occupancy_rate_l7d
    from `toronto-shelter-project.data_features.feature_shelter_2023`
    where date(occupancy_date) >= (select * from max_date_minus_7_cte)
      and date(occupancy_date) <= (select * from max_date_cte)
      and modeling_cohort = 1
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9
    )
    
    , forecast_dates_cte as (
          select
            date_add(end_date, interval x day) as forecast_date
          from 
            max_date_cte, 
            unnest(generate_array(1, 5)) as x
  )
        select
              fct.forecast_date as occupancy_date
            , var.*
        from forecast_dates_cte as fct
        cross join last_7_days_avg as var


  

      









