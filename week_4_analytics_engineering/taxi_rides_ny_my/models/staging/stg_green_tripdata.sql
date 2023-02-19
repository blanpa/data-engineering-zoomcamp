{{ config(materialized="view") }}

-- utility-logic-375619.trips_data_all.yellow_tripdata
select *
from {{ sources("staging", "green_tripdata") }}
limit 100
