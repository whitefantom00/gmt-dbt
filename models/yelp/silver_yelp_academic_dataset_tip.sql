{{ config(
    materialized='table',
) }}

select 
    date(date) as issue_date, 
    sum(compliment_count) as compliment_count
from {{ ref('bronze_yelp_academic_dataset_tip') }}
where 1=1
group by 1
order by 1 desc