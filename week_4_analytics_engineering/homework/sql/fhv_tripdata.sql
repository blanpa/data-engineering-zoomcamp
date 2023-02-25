CREATE or REPLACE EXTERNAL TABLE `utility-logic-375619.trips_data_all_us.fhv_tripdata`
OPTIONS(
  format = "CSV",
  uris = ['gs://workflow_bucket_gcp_us/data/fhv/fhv_tripdata_2019-*.csv.gz']
);

SELECT * FROM trips_data_all_us.fhv_tripdata limit 10;

-- Create a partitioned table from external table
CREATE OR REPLACE TABLE `utility-logic-375619.trips_data_all_us.fhv_tripdata_partitoned`
PARTITION BY DATE(pickup_datetime) AS
SELECT * FROM trips_data_all_us.fhv_tripdata;


CREATE OR REPLACE TABLE trips_data_all_us.fhv_tripdata_partitoned_clustered
PARTITION BY DATE(pickup_datetime)
CLUSTER BY Affiliated_base_number AS
SELECT * FROM trips_data_all_us.fhv_tripdata;