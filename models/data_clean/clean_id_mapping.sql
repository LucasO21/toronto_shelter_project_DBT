



with
    get_distinct as (
        select
            distinct sector as value
          , 'sector' as name
        from `toronto-shelter-project.data_clean.clean_combined_shelter`

        union distinct

        select
            distinct program_model as value
          , 'program_model' as name
        from {{ref('clean_combined_shelter')}}   ---`toronto-shelter-project.data_clean.clean_combined_shelter`

        union distinct

        select
            distinct program_area as value
          , 'program_area' as name
        from `toronto-shelter-project.data_clean.clean_combined_shelter`

        union distinct

        select
            distinct overnight_service_type as value
          , 'overnight_service_type' as name
        from `toronto-shelter-project.data_clean.clean_combined_shelter`

        union distinct

        select
            distinct capacity_type as value
          , 'capacity_type' as name
        from `toronto-shelter-project.data_clean.clean_combined_shelter`
        
    )
    
    , assign_ids as (

          select
              *
            , row_number() over (partition by name) as id
          from get_distinct
    )
    
    , assign_ids_wide as (

        select
            *
          , case when name like '%sector%' then id end as sector_id
          , case when name like '%program_model%' then id end as program_model_id
          , case when name like '%program_area%' then id end as program_area_id
          , case when name like '%overnight_service_type%' then id end as overnight_service_type_id
          , case when name like '%capacity_type%' then id end as capacity_type_id
        from assign_ids
    )
    select * from assign_ids_wide
    
    