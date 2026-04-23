{{ config(materialized='table') }}

select generate_series::date as date_day
from generate_series('2019-01-01'::timestamp, '2030-12-31'::timestamp, interval '1 day')
