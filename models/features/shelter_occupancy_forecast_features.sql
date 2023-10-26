-- This model gets features needed for predicting overnight shelter occupancy for the next 5 days. 


with 
    latest_occupancy_date as (
      -- Determine the latest date for which occupancy data is available.

        select
          max(date(occupancy_date)) as end_date
        from {{ref ('shelter_occupancy_weather_integration_2022_2023') }}  
    )

    , start_date_for_avg_calculation as (
      -- Find the start date for calculating a 7-day average, which is 6 days before the latest occupancy date.

      select
        date_add(end_date, interval -6 day) as start_date
      from latest_occupancy_date
    )
    
    , average_last_seven_days as (
      -- Calculate the average values of capacity, occupancy, and occupancy rate for the last 7 days.

        select
          --distinct
          --date(occupancy_date) as date
          pkey
        , round(avg(capacity_actual)) as avg_capacity_actual_l7d
        , round(avg(occupied)) as avg_occupied_l7d
        , round(avg(occupancy_rate)) as avg_occupancy_rate_l7d
    from {{ref ('shelter_occupancy_weather_integration_2022_2023') }}
    where date(occupancy_date) >= (select * from start_date_for_avg_calculation)
      and date(occupancy_date) <= (select * from latest_occupancy_date)
      and model_cohort_adj = 1
    group by 1
    )
    
    , next_five_forecast_dates as (
      -- Generate the next five dates after the latest occupancy date for which we want to predict.

          select
            date_add(end_date, interval x day) as occupancy_date
          from 
            latest_occupancy_date, 
            unnest(generate_array(1, 5)) as x
  )

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
          from {{ref ('shelter_occupancy_weather_integration_2022_2023') }}
          where model_cohort_adj = 1
    )

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

      


        


  

      








