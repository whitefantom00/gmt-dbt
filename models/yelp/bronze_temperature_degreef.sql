{{ config(
    materialized='table',
) }}

-- create or replace table SRC_Yelp.temperature_degreef as
SELECT *
FROM {{ source('STG_Climate', 'temperature_degreef') }}