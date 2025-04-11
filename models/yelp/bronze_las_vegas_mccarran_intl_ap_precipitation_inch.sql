{{ config(
    materialized='table',
) }}

create table SRC_Yelp.las_vegas_mccarran_intl_ap_precipitation_inch as
SELECT *
FROM {{ source('STG_Climate', 'las_vegas_mccarran_intl_ap_precipitation_inch') }}