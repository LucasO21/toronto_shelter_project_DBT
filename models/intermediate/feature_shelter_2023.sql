-- this model preps the 2023 dataset for modeling
-- it uses the 'model_cohort_flag_2022' model to flag shelter locations that were open 75% or more days in 2023 and flags those as 1
-- it also flags any shelter locations with missing data for locationa and program info and flags those as 0
-- shelter locations flagged as 1 will be included in the modeling cohort while shelters flagged as 0 will be excluded


with 
    cte_1 as (
        select 
            t1.*
            , t2.days_open
            , t2.days_open_pct
            , t2.model_cohort    
        from {{ ref ('clean_shelter_2023') }} as t1
        left join {{ref ('model_cohort_flag_2023') }} as t2
        on t1.organization_name           = t2.organization_name
            and t1.shelter_group          = t2.shelter_group
            and t1.program_id             = t2.program_id
            and t1.program_area           = t2.program_area
            and t1.sector                 = t2.sector
            and t1.program_model          = t2.program_model
            and t1.overnight_service_type = t2.overnight_service_type
            and t1.capacity_type          = t2.capacity_type
            and t1.location_id            = t2.location_id
    )

    , cte_2 as (
        select
            *
        , case 
            when location_address       = '' then 0 
            when location_city          = '' then 0 
            when program_name           = '' then 0 
            when program_model          = '' then 0 
            when overnight_service_type = '' then 0 
            when program_area           = '' then 0 
            when capacity_type          = '' then 0 
            else model_cohort
        end as modeling_cohort
        from cte_1
        
    )
        select
            * except (model_cohort)
        from cte_2
