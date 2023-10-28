

{{
    config(
        materialized = "table",
        schema       = "data_clean"
    )
}}


with 
    distinct_values as (
        -- Retrieve distinct values from multiple columns and associate them with their respective column names.
        select
            distinct sector as value
          , 'sector' as name
        from {{ref('shelter_occupancy_2022_2023')}} --`toronto-shelter-project.data_clean.clean_combined_shelter`

        union distinct

        select
            distinct program_model as value
          , 'program_model' as name
        from {{ref('shelter_occupancy_2022_2023')}}

        union distinct

        select
            distinct program_area as value
          , 'program_area' as name
        from {{ref('shelter_occupancy_2022_2023')}}

        union distinct

        select
            distinct overnight_service_type as value
          , 'overnight_service_type' as name
        from {{ref('shelter_occupancy_2022_2023')}}

        union distinct

        select
            distinct capacity_type as value
          , 'capacity_type' as name
        from {{ref('shelter_occupancy_2022_2023')}}
    )
    
    , values_with_ids as (
        -- Assign a unique ID to each distinct value, partitioned by their associated column name.
        select
            *
          , row_number() over (partition by name) as id
        from distinct_values
    )
    
    , wide_format_ids as (
        -- Convert the long-format data to wide format, where each distinct value type has its own ID column.
        select
            *
          , case when name like '%sector%' then id end as sector_id
          , case when name like '%program_model%' then id end as program_model_id
          , case when name like '%program_area%' then id end as program_area_id
          , case when name like '%overnight_service_type%' then id end as overnight_service_type_id
          , case when name like '%capacity_type%' then id end as capacity_type_id
        from values_with_ids
    )

select * from wide_format_ids
