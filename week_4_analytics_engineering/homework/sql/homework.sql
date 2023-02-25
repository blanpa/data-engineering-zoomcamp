-- Question 1
SELECT COUNT(*) as number_of_records
FROM `utility-logic-375619.production.fact_trips`
WHERE EXTRACT(YEAR FROM pickup_datetime) BETWEEN 2019 and 2020;
-- Result:
-- 61635416

-- Question 3
SELECT COUNT(*) as record_count
FROM `utility-logic-375619.production.stg_fhv_tripdata` 
WHERE EXTRACT(YEAR FROM pickup_datetime) = 2019;
-- Result:
-- 43244696

-- Question 4
SELECT COUNT(*) as record_count
FROM `utility-logic-375619.production.fact_fhv_trips` 
WHERE EXTRACT(YEAR FROM pickup_datetime) = 2019;
-- Result:
-- 22998722

-- Question 5
SELECT
EXTRACT(MONTH FROM pickup_datetime) as month,
COUNT(*) as record_count_month
FROM `utility-logic-375619.production.fact_fhv_trips` 
WHERE EXTRACT(YEAR FROM pickup_datetime) = 2019
GROUP BY month
ORDER BY month;
-- Result:
-- month record_count_month
-- 1 19849151
-- 2 187899
-- 3 190132
-- 4 256108
-- 5 262536
-- 6 278248
-- 7 290680
-- 8 327553
-- 9 311295
-- 10 349988
-- 11 340663
-- 12 354469