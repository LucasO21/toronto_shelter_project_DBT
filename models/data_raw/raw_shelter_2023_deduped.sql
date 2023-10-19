
{{ 
    config(
        materialized = 'table',
        schema       = 'data_raw'
    ) 
}}

select
    distinct *
from {{ source ('data_raw', 'raw_shelter_2023') }}