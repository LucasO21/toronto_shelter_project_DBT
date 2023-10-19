/*
This model  combines unions raw shelter data for 2022 with raw shelter data for 2023.
Next the model transforms shelter occupancy value columns for bed and rooms from wide to long.
*/

with
    combine_2022_2023 as (
      select
          *
      from {{source('data_raw', 'raw_shelter_2022')}}  --`toronto-shelter-project.data_raw.raw_shelter_2022` 

      union all

      select
        *
      from {{source('data_raw', 'raw_shelter_2023')}}  --`toronto-shelter-project.data_raw.raw_shelter_2023`
    )

    , new_columns as (
      select
          *
          , case when lower(capacity_type) like '%bed%' then capacity_actual_bed else capacity_actual_room end as capacity_actual
          , case when lower(capacity_type) like '%bed%' then capacity_funding_bed else capacity_funding_room end as capacity_funding
          , case when lower(capacity_type) like '%bed%' then occupied_beds else occupied_rooms end as occupied
          , case when lower(capacity_type) like '%bed%' then unoccupied_beds else unoccupied_rooms end as unoccupied
          , case when lower(capacity_type) like '%bed%' then unavailable_beds else unavailable_rooms end as unavailable
          , case when lower(capacity_type) like '%bed%' then occupancy_rate_beds else occupancy_rate_rooms end as occupancy_rate
        from combine_2022_2023
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
      select * from drop_columns