/*
This model dedupes future predictions
*/

{{
    config(
        materialized = "table",
        schema       = "data_pred"
    )
}}

select
    distinct
        *
from {{source('data_pred', 'shelter_occupancy_predictions')}}