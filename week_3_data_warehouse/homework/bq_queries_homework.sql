-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `ringed-enigma-376110.dezoomcamp.fhv_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://dezoomcamptaxibucket/data/fhv/fhv_tripdata_2019-*.csv.gz']
);

-- Question 1 Answer
SELECT COUNT(*) FROM `ringed-enigma-376110.dezoomcamp.fhv_tripdata`;

-- Create BQ Table
CREATE OR REPLACE TABLE `ringed-enigma-376110.dezoomcamp.fhv_tripdata_non_partitioned` AS
SELECT * FROM `ringed-enigma-376110.dezoomcamp.fhv_tripdata`;

-- Question 2
-- external table
SELECT COUNT(DISTINCT(Affiliated_base_number))
FROM `ringed-enigma-376110.dezoomcamp.fhv_tripdata`;
-- result 0MB

-- bq table
SELECT COUNT(DISTINCT(Affiliated_base_number))
FROM `ringed-enigma-376110.dezoomcamp.fhv_tripdata_non_partitioned`;
-- result 317.94MB

-- Question 3
SELECT COUNT(*)
FROM `ringed-enigma-376110.dezoomcamp.fhv_tripdata_non_partitioned`
WHERE PUlocationID IS NULL AND DOlocationID IS NULL;
-- 717748

-- Question 4
-- Creating a partition (pickup) and cluster (Affiliate_base_number) table
CREATE OR REPLACE TABLE `ringed-enigma-376110.dezoomcamp.fhv_tripdata_partitioned_and_clustered`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY Affiliated_base_number AS
SELECT * FROM `ringed-enigma-376110.dezoomcamp.fhv_tripdata`;

-- Question 5
-- Write a query to retrieve the distinct affiliated_base_number between pickup_datetime 2019/03/01 and 2019/03/31 (inclusive)

-- a with bq table
SELECT COUNT(DISTINCT Affiliated_base_number) as number_of_distinct_affiliates
FROM `ringed-enigma-376110.dezoomcamp.fhv_tripdata_non_partitioned`
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';

-- result 725

-- with partitioned and clustered table
SELECT COUNT(DISTINCT Affiliated_base_number) as number_of_distinct_affiliates
FROM `ringed-enigma-376110.dezoomcamp.fhv_tripdata_partitioned_and_clustered`
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';

-- result 725

-- Question 8
-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `ringed-enigma-376110.dezoomcamp.fhv_tripdata_parquet`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dezoomcamptaxibucket/data/fhv/fhv_tripdata_2019-*.parquet']
);

-- query count on 
SELECT COUNT(pickup_datetime)
FROM `ringed-enigma-376110.dezoomcamp.fhv_tripdata_parquet`;