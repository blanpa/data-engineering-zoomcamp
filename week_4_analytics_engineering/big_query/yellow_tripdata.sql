CREATE or REPLACE EXTERNAL TABLE `utility-logic-375619.trips_data_all.yellow_tripdata`
OPTIONS(
  format = "CSV",
  uris = ['gs://workflow_bucket_gcp/data/yellow/yellow_tripdata_2019-*.csv.gz', 'gs://workflow_bucket_gcp/data/yellow/yellow_tripdata_2020-*.csv.gz']
);

SELECT * FROM trips_data_all.yellow_tripdata limit 10;