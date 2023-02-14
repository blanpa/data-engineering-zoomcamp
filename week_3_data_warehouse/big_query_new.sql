-- Query public available table
SELECT station_id, name FROM
    bigquery-public-data.new_york_citibike.citibike_stations
LIMIT 100;


-- Creating external table referring to gcs path
-- table: utility-logic-375619.practice_week_3
-- folder: workflow_bucket_gcp/yellow/
-- files: yellow_tripdata_2019-01.csv.gz
CREATE OR REPLACE EXTERNAL TABLE `utility-logic-375619.practice_week_3.external_yellow_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://workflow_bucket_gcp/data/yellow/yellow_tripdata_2019-*.csv.gz', 'gs://workflow_bucket_gcp/data/yellow/yellow_tripdata_2020-*.csv.gz']
);


-- Check yello trip data
SELECT * FROM practice_week_3.external_yellow_tripdata limit 10;


-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE `utility-logic-375619.practice_week_3.yellow_tripdata_non_partitoned` AS
SELECT * FROM practice_week_3.external_yellow_tripdata;


-- Create a partitioned table from external table
CREATE OR REPLACE TABLE `utility-logic-375619.practice_week_3.yellow_tripdata_partitoned`
PARTITION BY DATE(tpep_pickup_datetime) AS
SELECT * FROM practice_week_3.external_yellow_tripdata;


-- Impact of partition
-- Scanning 1.6GB of data
SELECT DISTINCT(VendorID)
FROM `utility-logic-375619.practice_week_3.yellow_tripdata_non_partitoned`
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';


-- Scanning ~106 MB of DATA
SELECT DISTINCT(VendorID)
FROM `utility-logic-375619.practice_week_3.yellow_tripdata_partitoned`
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';


-- Let's look into the partitons
-- SELECT table_name, partition_id, total_rows
SELECT *
FROM practice_week_3.INFORMATION_SCHEMA.PARTITIONS
WHERE table_name = 'yellow_tripdata_partitoned'
ORDER BY total_rows DESC;

-- Creating a partition and cluster table
CREATE OR REPLACE TABLE practice_week_3.yellow_tripdata_partitoned_clustered
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM practice_week_3.external_yellow_tripdata;


-- Query scans 1.1 GB
SELECT count(*) as trips
FROM practice_week_3.yellow_tripdata_partitoned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2020-12-31'
  AND VendorID=1;

-- Query scans 864.5 MB
SELECT count(*) as trips
FROM practice_week_3.yellow_tripdata_partitoned_clustered
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2020-12-31'
  AND VendorID=1;