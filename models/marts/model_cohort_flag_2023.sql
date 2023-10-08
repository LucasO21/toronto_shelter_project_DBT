

with 
    days_open_cte as ( 
      select 
          organization_name
        , shelter_group
        , program_id
        , program_area
        , sector
        , program_model
        , overnight_service_type
        , capacity_type
        , location_id
        , count(distinct occupancy_date) as days_open
      from {{ ref ('clean_shelter_2023' )}}
      group by 1, 2, 3, 4, 5, 6, 7, 8, 9
    )
    
    , days_open_pct_cte as (
        select
            *
          , (days_open_cte.days_open / (select extract(dayofyear from current_date()))) as days_open_pct  
        from days_open_cte  
    )

    , days_open_flag_cte as (
        select
            *
          , case when days_open_pct_cte.days_open_pct >= 0.75 then 1 else 0 end as model_cohort  
        from days_open_pct_cte  
    )
select * from days_open_flag_cte

