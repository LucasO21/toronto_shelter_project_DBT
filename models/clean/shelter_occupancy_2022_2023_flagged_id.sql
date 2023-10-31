/*
This dbt model is designed to enrich and transform shelter occupancy data over the years 2022 and 2023. 
Initially, it adds sector, program model, program area, and overnight service type identifiers by joining to an 'id_mapping' table to standardize textual descriptions into IDs.
Following this, it creates a concatenated primary key ('pkey') for each record to ensure uniqueness, which combines organization, shelter, location, program, sector, program model, program area, overnight service type, and capacity type IDs.
The model then computes the number of days each shelter was open each year and translates this into a percentage of the year.
A flag ('model_cohort') is assigned to identify whether a shelter was open for at least 75% of the year, signifying a cohort for potential further analysis.
Lastly, the model integrates these new flags and computes an adjusted 'model_cohort' flag that accounts for the presence of necessary IDs, ensuring that only records with complete ID information are flagged accordingly. 
The final output rearranges the columns into a specified order, ready for analysis or reporting purposes.
*/


{{
    config(
        materialized = "table",
        schema       = "data_clean"
    )
}}

with 
    sector_id as (
        -- Associate the data with sector ids.
        select
           t1.*
         , t2.sector_id
        from {{ref('shelter_occupancy_2022_2023')}} as t1 
        left join {{ref('shelter_occupancy_id_mapping')}} as t2 
          on lower(t1.sector) = lower(t2.value)
    )
    
    , program_model_id as (
        -- Associate the data with program model ids.
          select
              t1.*
            , t2.program_model_id
          from sector_id as t1
          left join {{ref('shelter_occupancy_id_mapping')}} as t2
          on lower(t1.program_model) = lower(t2.value)
    )

      , program_area_id as (
            -- Associate the data with program area ids.

          select
              t1.*
            , t2.program_area_id
          from program_model_id as t1
          left join {{ref('shelter_occupancy_id_mapping')}} as t2 
          on lower(t1.program_area) = lower(t2.value)
    )

      , overnight_service_type_id as (
            -- Associate the data with overnight service type ids.

          select
              t1.*
            , t2.overnight_service_type_id
          from program_area_id as t1
          left join {{ref('shelter_occupancy_id_mapping')}} as t2 
          on lower(t1.overnight_service_type) = lower(t2.value)
    )

      , capacity_type_id_cte as (
            -- Associate the data with capacity type ids.

          select
              t1.*
            , t2.capacity_type_id
          from overnight_service_type_id as t1
          left join {{ref('shelter_occupancy_id_mapping')}} as t2 
          on lower(t1.capacity_type) = lower(t2.value)
    )
    
    , pkey_cte as (
            -- Generate a concatenated primary key from various id columns.

        select
            *
          , concat(
                cast(organization_id as string), '-'
              , cast(shelter_id as string), '-'
              , cast(location_id as string), '-'
              , cast(program_id as string), '-'
              , cast(sector_id as string), '-'
              , cast(program_model_id as string), '-'
              , cast(program_area_id as string), '-'
              , cast(overnight_service_type_id as string), '-'
              , cast(capacity_type_id as string)
            ) as pkey
        from capacity_type_id_cte
    )

    , days_open_cte as (
            -- Calculate the percentage of days open in a year.

        select
            pkey
          , extract(year from occupancy_date) as year_date
          , cast(count(distinct occupancy_date) as float64) as days_open        
        from pkey_cte
        group by 1, 2
    )
    
    , days_open_pct_cte as (
         -- Assign a flag based on the percentage of days open.

        select
            *
          , case 
              when year_date = 2022 then days_open / 365.0
              when year_date = 2023 then days_open / cast(extract(dayofyear from current_date()) as float64)
              else null
              end as days_open_pct
            from days_open_cte
    )
    
    , days_open_flag_cte as (
            -- Assign a flag based on the percentage of days open.

        select
            *
          , case 
              when days_open_pct >= 0.75 then 1 else 0 end as model_cohort
        from days_open_pct_cte
    )
  
    , add_flags as (
            -- Integrate the computed flags into the main data.

        select
            t1.*
          , t2.year_date
          , t2.days_open
          , t2.days_open_pct
          , t2.model_cohort
        from pkey_cte as t1
        left join days_open_flag_cte as t2
          on t1.pkey = t2.pkey
    )
    
    , arrange_columns as (
            -- Rearrange and select the columns in the desired order. Apply adjustments on the 'model_cohort' flag.

        select
            x_id
          , occupancy_date
          , year_date
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
          , service_user_count
          , capacity_actual
          , capacity_funding
          , occupied
          , unavailable
          , occupancy_rate
          , days_open
          , days_open_pct
          , model_cohort
          , case
              when organization_id is null or cast(organization_id as string) = '' then 0
              when shelter_id is null or cast(shelter_id as string) = '' then 0
              when location_id is null or cast(location_id as string) = '' then 0
              when program_id is null or cast(program_id as string) = '' then 0
              when sector_id is null or cast(sector_id as string) = '' then 0
              when program_model_id is null or cast(program_model_id as string) = '' then 0
              when program_area_id is null or cast(program_area_id as string) = '' then 0
              when overnight_service_type_id is null or cast(overnight_service_type_id as string) = '' then 0
              when capacity_type_id is null or cast(capacity_type_id as string) = '' then 0
              else model_cohort
            end as model_cohort_adj
        from add_flags
    )
      select 
        * 
      from arrange_columns
    
