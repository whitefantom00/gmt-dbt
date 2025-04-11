{{ config(
    materialized='table',
) }}

create table SRC_Yelp.temperature_degreef as
SELECT *
FROM {{ source('STG_Climate', 'temperature_degreef') }}