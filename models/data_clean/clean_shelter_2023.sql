{{ 
    config(
        materialized='table'
    ) 
}}
 

with 
  
    new_columns as (
        select
          *
          , case when lower(capacity_type) like '%bed%' then capacity_actual_bed else capacity_actual_room end as capacity_actual
          , case when lower(capacity_type) like '%bed%' then capacity_funding_bed else capacity_funding_room end as capacity_funding
          , case when lower(capacity_type) like '%bed%' then occupied_beds else occupied_rooms end as occupied
          , case when lower(capacity_type) like '%bed%' then unoccupied_beds else unoccupied_rooms end as unoccupied
          , case when lower(capacity_type) like '%bed%' then unavailable_beds else unavailable_rooms end as unavailable
          , case when lower(capacity_type) like '%bed%' then occupancy_rate_beds else occupancy_rate_rooms end as occupancy_rate
        from {{ source ('data_raw', 'raw_shelter_2023') }}  --`toronto-shelter-project`.`data_raw`.`raw_shelter_2023`
    )
    
    , drop_columns as (
          select
            * except(
                capacity_actual_bed
              , capacity_funding_bed
              , occupied_beds
              , unoccupied_beds
              , unavailable_beds
              , capacity_actual_room
              , capacity_funding_room
              , occupied_rooms
              , unoccupied_rooms
              , unavailable_rooms
              , occupancy_rate_beds
              , occupancy_rate_rooms
            )
          from new_columns
    )

    , add_id_columns as (
          select
              *
            , case 
                when lower(sector) = 'families' then 1
                when lower(sector) = 'men' then 2
                when lower(sector) = 'mixed adult' then 3
                when lower(sector) = 'women' then 4
                when lower(sector) = 'youth' then 5
                else 6
              end as sector_id
            
            , case
                when lower(program_model) = 'emergency' then 1 else 2 end as program_model_id

            , case 
                when lower(program_area) = 'covid-19 response' then 1
                when lower(program_area) = 'base shelter and overnight services system' then 2
                when lower(program_area) = 'winter programs' then 3
                when lower(program_area) = 'temporary refugee response' then 4
                when lower(program_area) = 'base program - refugee' then 5
                else 6
              end as program_area_id
            
            , case 
                when lower(overnight_service_type) = '24-hour respite site' then 1
                when lower(overnight_service_type) = "24-Hour women's drop-in" then 2
                when lower(overnight_service_type) = 'isolation/recovery site' then 3
                when lower(overnight_service_type) = 'motel/hotel shelter' then 4
                when lower(overnight_service_type) = 'shelter' then 5
                when lower(overnight_service_type) = 'warming centre' then 6
                when lower(overnight_service_type) = 'alternative space protocol' then 7
                else 8
              end as overnight_service_type_id

            , case
                when lower(capacity_type) = 'bed based capacity' then 1 else 2 end as capacity_type_id

          from drop_columns     

    )

    , arrange_columns as (

          select 
                  x_id
                , occupancy_date
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
                , unoccupied
                , unavailable
                , occupancy_rate
              from add_id_columns

    )
        select
            concat(organization_id, shelter_id, location_id, program_id, sector_id, program_model_id, program_area_id, capacity_type_id) as pkey
          , *
        from arrange_columns

              