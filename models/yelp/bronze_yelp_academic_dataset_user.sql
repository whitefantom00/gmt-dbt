{{ config(
    materialized='table',
) }}

create table SRC_Yelp.yelp_academic_dataset_user as
SELECT *
FROM {{ source('STG_Yelp', 'yelp_academic_dataset_user') }}