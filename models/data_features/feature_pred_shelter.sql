/*
This SQL query calculates and combines data related to shelter occupancy forecasting. 
It begins by retrieving location information and identifying the maximum occupancy date. 
Then, it calculates the start date for a 7-day window, computes average occupancy statistics for that period, and generates forecast dates for the next 5 days.
These forecasts are combined with location details, creating a comprehensive dataset. 
Finally, the query selects and returns the merged data, enabling the analysis of overnight shelter occupancy for the upcoming 5 days, associating it with relevant location information and historical averages.
*/


-- This cte retrieves location information.
with location_info as (
    select distinct
        pkey,
        organization_name,
        shelter_group,
        program_name,
        location_name,
        location_address,
        location_postal_code,
        location_city,
        location_province
    from {{ ref ('feature_shelter_2023') }} --`toronto-shelter-project.data_features.feature_shelter_2023`
),

-- This cte finds the maximum date for occupancy.
max_occupancy_date as (
    select max(date(occupancy_date)) as end_date
    from {{ ref ('feature_shelter_2023') }} --`toronto-shelter-project.data_features.feature_shelter_2023`
),

-- This cte calculates the start date for the last 7 days.
start_date_last_7_days as (
    select date_add(end_date, interval -6 day) as start_date
    from max_occupancy_date
),

-- This cte calculates the average occupancy for the last 7 days.
avg_occupancy_last_7_days as (
    select
        pkey,
        organization_id,
        shelter_id,
        location_id,
        program_id,
        sector_id,
        program_model_id,
        program_area_id,
        overnight_service_type_id,
        capacity_type_id,
        round(avg(capacity_actual)) as avg_capacity_actual_l7d,
        round(avg(occupied)) as avg_occupied_l7d,
        round(avg(occupancy_rate)) as avg_occupancy_rate_l7d
    from {{ ref ('feature_shelter_2023') }}  --`toronto-shelter-project.data_features.feature_shelter_2023`
    where date(occupancy_date) between (select * from start_date_last_7_days) and (select * from max_occupancy_date)
        and modeling_cohort = 1
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
),

-- This cte generates forecast dates for the next 5 days.
forecast_dates as (
    select date_add(end_date, interval x day) as forecast_date
    from max_occupancy_date, unnest(generate_array(1, 5)) as x
),

-- This cte combines forecast dates with the last 7 days' averages.
forecast_and_averages as (
    select
        fct.forecast_date as occupancy_date,
        avg.*
    from forecast_dates as fct
    cross join avg_occupancy_last_7_days as avg
),

-- This cte combines location information with forecast and averages.
final_result as (
    select
        faa.*,
        loc.organization_name,
        loc.shelter_group,
        loc.program_name,
        loc.location_address,
        loc.location_name,
        loc.location_postal_code,
        loc.location_city,
        loc.location_province
    from forecast_and_averages as faa
    left join location_info as loc
    on faa.pkey = loc.pkey
)

-- Select all columns from the final result.
select 
    *
from final_result
