

with 
    sector_id as (
      -- joining sector ids
        select
           t1.*
         , t2.sector_id
        from {{ref('clean_combined_shelter')}} as t1 --`toronto-shelter-project.data_clean.clean_combined_shelter` as t1
        left join `toronto-shelter-project.data_clean.clean_id_mapping` as t2 
          on lower(t1.sector) = lower(t2.value)
    )
    
    , program_model_id as (
        -- joining program model ids
          select
              t1.*
            , t2.program_model_id
          from sector_id as t1
          left join {{ref('clean_id_mapping')}} as t2 --`toronto-shelter-project.data_clean.clean_id_mapping` as t2 
          on lower(t1.program_model) = lower(t2.value)
    )

      , program_area_id as (
          select
              t1.*
            , t2.program_area_id
          from program_model_id as t1
          left join {{ref('clean_id_mapping')}} as t2 --`toronto-shelter-project.data_clean.clean_id_mapping` as t2 
          on lower(t1.program_area) = lower(t2.value)
    )

      , overnight_service_type_id as (
          select
              t1.*
            , t2.overnight_service_type_id
          from program_area_id as t1
          left join {{ref('clean_id_mapping')}} as t2 --`toronto-shelter-project.data_clean.clean_id_mapping` as t2 
          on lower(t1.overnight_service_type) = lower(t2.value)
    )

      , capacity_type_id_cte as (
          select
              t1.*
            , t2.capacity_type_id
          from overnight_service_type_id as t1
          left join {{ref('clean_id_mapping')}} as t2 --`toronto-shelter-project.data_clean.clean_id_mapping` as t2 
          on lower(t1.capacity_type) = lower(t2.value)
    )
    
    , pkey_cte as (
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
        select
            pkey
          , extract(year from occupancy_date) as year_date
          , cast(count(distinct occupancy_date) as float64) as days_open        
        from pkey_cte
        group by 1, 2
    )
    
    , days_open_pct_cte as (
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
        select
            *
          , case 
              when days_open_pct >= 0.75 then 1 else 0 end as model_cohort
        from days_open_pct_cte
    )
  
    , add_flags as (
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
              when (
                (organization_id is null or organization_id = '')
                  (or shelter_id is null or shelter_id = '')
                  (or location_id is null or location_id = '')
                  (or program_id is null or program_id = '')
                  (or sector_id is null or sector_id = '')
                  (or program_model_id is null or program_model_id = '')
                  (or program_area_id is null or program_area_id = '')
                  (or overnight_service_type_id is null or overnight_service_type_id = '')
                  (or capacity_type_id is null or capacity_type_id = '')
              ) then 0
              else model_cohort
              end as model_cohort_adj

        from add_flags
    )
      select * from arrange_columns
    
