
/*
This model gets features for predicing overnight shelter occupancy for the next 5 days.
 * Focuses on pkeys where `model_cohort_adj` = 1, i.e pkeys that have existing for 75% or more days in a given year
 * Create future dates (next 5 days) for each pkey
 * Extract features needed for making predictions (for each pkey) including organization_id, program_id, location_id, etc
 * Combine with weather forecast for the next 5 days
*/

{{
    config(
        materialized = "table",
        schema       = "data_features"
    )
}}


with 
  -- Get max shelter occupancy date and the 7th date prior to that max date for each pkey
  date_range as (
      select
          pkey
        , max(occupancy_date) as max_date
        , date_sub(max(occupancy_date), interval 7 day) as start_date
      from {{ref ('shelter_occupancy_weather_integration_2022_2023')}}
      group by 1 
  )
    --select * from date_range;
  
  , source_features as (
      -- Get features needed to calculate the average values of capacity, occupancy, and occupancy rate for the last 7 days.
      select
          t1.pkey
        , t1.occupancy_date
        , t1.capacity_actual
        , t1.occupied
        , t1.occupancy_rate
        , t2.max_date
        , t2.start_date
      from {{ref ('shelter_occupancy_weather_integration_2022_2023')}} as t1
      left join date_range as t2
        on t1.pkey = t2.pkey
      where t1.pkey is not null
  )
    --select * from source_features;
  
  , average_last_seven_days as (
    -- Calculate the average values of capacity, occupancy, and occupancy rate for the last 7 days.
      select
          pkey
        , round(avg(capacity_actual)) as avg_capacity_actual_l7d
        , round(avg(occupied)) as avg_occupied_l7d
        , round(avg(occupancy_rate)) as avg_occupancy_rate_l7d
      from source_features
      where occupancy_date between start_date and max_date
      group by 1
  )
    --select * from average_last_seven_days;
  
  , latest_occupancy_date as (
     -- Determine the latest date for which occupancy data is available.
      select
        max(max_date) as max_date
      from date_range
  )
    --select * from latest_occupancy_date;

  , next_five_forecast_dates as (
      -- Generate the next five dates after the latest occupancy date for which we want to predict.

          select
            date_add(max_date, interval x day) as occupancy_date
          from latest_occupancy_date, 
          unnest(generate_array(1, 5)) as x
  )
  --select * from next_five_forecast_dates;

  , forecast_date_with_avg_values as (
    -- Pair each forecasted date with the average values calculated from the past seven days.

        select
            t1.occupancy_date
          , t2.pkey
          , t2.avg_capacity_actual_l7d
          , t2.avg_occupied_l7d
          , t2.avg_occupancy_rate_l7d
        from next_five_forecast_dates as t1
        cross join average_last_seven_days as t2
  )
    --select * from forecast_date_with_avg_values;
  
  , required_features_from_data as (
      -- Extract only the relevant features for prediction from the entire dataset.

          select
            distinct 
              *
            except(
                x_id
              , occupancy_date
              , year_date
              , occupied
              , unavailable
              , occupancy_rate
              , days_open
              , days_open_pct
              , model_cohort
              , model_cohort_adj
              , temp_min
              , temp_max
              , temp_avg
              , service_user_count
              , capacity_actual
              , capacity_funding
            )
          from {{ref ('shelter_occupancy_weather_integration_2022_2023')}}
          where model_cohort_adj = 1
    )
    --select * from required_features_from_data;

    , features_with_forecast_dates as (
        select
            t1.*
          , t2.occupancy_date
          , t2.avg_capacity_actual_l7d
          , t2.avg_occupied_l7d
          , t2.avg_occupancy_rate_l7d
          from required_features_from_data as t1
          left join forecast_date_with_avg_values as t2
            on t1.pkey = t2.pkey
    )

    --select * from features_with_forecast_dates where occupancy_date is null;

    , arrange as (
      -- Arrange the columns in a desired sequence for clarity.

        select 
            occupancy_date
          , pkey
          , organization_id
          , organization_name
          , shelter_id
          , shelter_group
          , location_id
          , location_name
          , location_address
          , location_postal_code
          , location_city
          , location_province
          , program_id
          , program_name
          , sector_id
          , sector
          , program_model_id
          , program_model
          , overnight_service_type_id
          , overnight_service_type
          , program_area_id 
          , program_area
          , capacity_type_id
          , capacity_type
          , avg_capacity_actual_l7d
          , avg_occupied_l7d
          , avg_occupancy_rate_l7d
      from features_with_forecast_dates

    )

        select 
          *
        from arrange
      


        


  

      








