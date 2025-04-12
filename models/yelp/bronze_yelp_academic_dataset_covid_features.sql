{{ config(
    materialized='table',
) }}

-- create or replace table SRC_Yelp.yelp_academic_dataset_covid_features as
SELECT *
FROM {{ source('STG_Yelp', 'yelp_academic_dataset_covid_features') }}