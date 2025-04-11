{{ config(
    materialized='table',
) }}

create table SRC_Yelp.bronze_yelp_academic_dataset_tip as
SELECT *
FROM {{ source('STG_Yelp', 'yelp_academic_dataset_tip') }}