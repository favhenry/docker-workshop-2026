# Data Engineering Zoomcamp 2026 – Data-warehouse Quiz

This repository contains my solution for **Module 3** of the [Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp) (2026 cohort) — focusing on **BigQuery**, external tables, partitioning, clustering, and cost estimation.

## Assignment Overview

We work with **Yellow Taxi Trip Records** from January 2024 to June 2024 (Parquet format) from the official NYC TLC site:  
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

Tasks include:
- Loading data to Google Cloud Storage (GCS)
- Creating external and materialized tables in BigQuery
- Running analytical queries
- Comparing costs (bytes processed)
- Applying partitioning & clustering best practices

## Dataset

- **Source**: NYC Taxi & Limousine Commission (TLC)
- **Period**: January 2024 – June 2024
- **Format**: Parquet files (one per month)
- **Files loaded**: yellow_tripdata_2024-01.parquet to yellow_tripdata_2024-06.parquet
- **Total records**: ~20 million (exact count found in Question 1)


Where applicable, answers are supported with **SQL queries** and **execution screenshots**.


## Steps Performed

1. **Data Loading**
   - Downloaded 6 Parquet files (Jan–Jun 2024) from the NYC TLC website
   - Used the provided Python script (`load_yellow_taxi_data.py`) to upload them to my GCS bucket
   - Bucket path example: `gs://my-taxi-bucket/yellow_tripdata_2024-*.parquet`
   - **Important**: Used PARQUET format for external table creation

2. **BigQuery Setup**
   - Created **external table**:
     ```sql
     CREATE OR REPLACE EXTERNAL TABLE `kestra-sandbox-485710.ny_taxi.external_yellow_tripdata`
     OPTIONS (
       format = 'PARQUET',
       uris = ['gs://henry_dezoomcamp_hw3_2026/yellow_tripdata_2024-*.parquet']
     );
     
     ```
> _Screenshot proof below:_

![External_yellow_tripdata](https://github.com/favhenry/docker-workshop-2026/blob/main/03-Data-warehouse-Assignment/images/Module%203%20Assignment%20Creation%20of%20external%20table.PNG)

   - Created **Materialized table**:
     ```sql
     CREATE OR REPLACE TABLE `kestra-sandbox-485710.ny_taxi.yellow_tripdata_non_partitioned` AS
     SELECT * 
     FROM `kestra-sandbox-485710.ny_taxi.external_yellow_tripdata`;
      ```
> _Screenshot proof below:_

![ny_taxi.yellow_tripdata_non_partitioned](https://github.com/favhenry/docker-workshop-2026/blob/main/03-Data-warehouse-Assignment/images/Module%203%20Assignment%20Creation%20of%20Materialized%20non%20partition%20table.PNG)


Optimized Table (Question 5)
Created **partitioned & clustered table**:

     ```sql
     CREATE OR REPLACE TABLE `kestra-sandbox-485710.ny_taxi.yellow_tripdata_optimized`
     PARTITION BY DATE(tpep_dropoff_datetime)
     CLUSTER BY VendorID
     AS
     SELECT * 
     FROM `kestra-sandbox-485710.ny_taxi.yellow_tripdata_non_partitioned`;
      ```
---

## 1. Question 1. Counting records

**Question:**  
**What is count of records for the 2024 Yellow Taxi Data?**

**Correct Answer:**  
✅ **20,332,093 rows**

**Evidence:**  
This value is obtained directly from the **Details of the Yellow Taxi Data that is not partitioned** 


> _Screenshot proof below:_

![ny_taxi.yellow_tripdata_non_partitioned](https://github.com/favhenry/docker-workshop-2026/blob/main/03-Data-warehouse-Assignment/images/Module%203%20Assignment%20Question%201%20Answer.PNG)

---

## 2. Question 2: Data read estimation

**Question:**  
Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.

What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

**Correct Answer:**  
✅ **`0 MB for the External Table and 155.12 MB for the Materialized Table`**

**Explanation:**  
Query:

```sql
SELECT COUNT(DISTINCT PULocationID) AS distinct_pickup_locations
FROM `kestra-sandbox-485710.ny_taxi.external_yellow_tripdata`;
```
```sql
SELECT COUNT(DISTINCT PULocationID) AS distinct_pickup_locations
FROM kestra-sandbox-485710.ny_taxi.yellow_tripdata_non_partitioned;

```
> _Screenshot proof below:_

![ny_taxi.external_yellow_tripdata](https://github.com/favhenry/docker-workshop-2026/blob/main/03-Data-warehouse-Assignment/images/Module%203%20Assignment%20Question%202a%20Answer.PNG)
![ny_taxi.yellow_tripdata_non_partitioned](https://github.com/favhenry/docker-workshop-2026/blob/main/03-Data-warehouse-Assignment/images/Module%203%20Assignment%20Question%202b%20Answer.PNG)
---

## 3. Question 3. Understanding columnar storage

**Question:**  
Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table.

**Correct Answer:**  
✅ **`BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.`**

```sql
SELECT  PULocationID
FROM kestra-sandbox-485710.ny_taxi.yellow_tripdata_non_partitioned;
```
```sql
SELECT  PULocationID,DOLocationID
FROM kestra-sandbox-485710.ny_taxi.yellow_tripdata_non_partitioned;
```
> _Screenshot proof below:_

![ny_taxi.yellow_tripdata_non_partitioned](https://github.com/favhenry/docker-workshop-2026/blob/main/03-Data-warehouse-Assignment/images/Module%203%20Assignment%20Question%203a%20Answer.PNG)
![ny_taxi.yellow_tripdata_non_partitioned](https://github.com/favhenry/docker-workshop-2026/blob/main/03-Data-warehouse-Assignment/images/Module%203%20Assignment%20Question%203b%20Answer.PNG)


---

## 4. Question 4. Counting zero fare trips

**Question:**  
How many records have a fare_amount of 0?

**Correct Answer:**  
✅ **`8,333 rows`**

```sql
SELECT  COUNT(*) AS zero_fare_trips
FROM kestra-sandbox-485710.ny_taxi.yellow_tripdata_non_partitioned
WHERE fare_amount=0;

```

> _Screenshot proof below:_

![ny_taxi.yellow_tripdata_non_partitioned](https://github.com/favhenry/docker-workshop-2026/blob/main/03-Data-warehouse-Assignment/images/Module%203%20Assignment%20Question%204%20Answer.PNG)

---

## 5. Question 5. Partitioning and clustering

**Question:**  
What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID (Create a new table with this strategy)

**Correct Answer:**  
✅ **`Partition by tpep_dropoff_datetime and Cluster on VendorID`**

```sql
CREATE OR REPLACE TABLE `kestra-sandbox-485710.ny_taxi.yellow_tripdata_optimized`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID
AS
SELECT * 
FROM `kestra-sandbox-485710.ny_taxi.yellow_tripdata_non_partitioned`;
```

---

## 6. Question 6. Partition benefits

**Question:**  
Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive)

Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 5 and note the estimated bytes processed. What are these values?

**Correct Answer:**  
✅ **`310.24 MB for non-partitioned table and 26.84 MB for the partitioned table`**

```sql
SELECT DISTINCT VendorID
FROM `kestra-sandbox-485710.ny_taxi.yellow_tripdata_non_partitioned`
WHERE tpep_dropoff_datetime >= '2024-03-01'
  AND tpep_dropoff_datetime < '2024-03-16';
```
```sql
SELECT DISTINCT VendorID
FROM `kestra-sandbox-485710.ny_taxi.yellow_tripdata_optimized`
WHERE tpep_dropoff_datetime >= '2024-03-01'
  AND tpep_dropoff_datetime < '2024-03-16';

```

> _Screenshot proof below:_

![ny_taxi.yellow_tripdata_non_partitioned](https://github.com/favhenry/docker-workshop-2026/blob/main/03-Data-warehouse-Assignment/images/Module%203%20Assignment%20Question%206a%20Answer.PNG)
![ny_taxi.yellow_tripdata_non_partitioned](https://github.com/favhenry/docker-workshop-2026/blob/main/03-Data-warehouse-Assignment/images/Module%203%20Assignment%20Question%206b%20Answer.PNG)
---

## 7. Question 7. External table storage

**Question:**  
Where is the data stored in the External Table you created?

**Correct Answer:**  
✅ **`GCP Bucket`**

---

## 8. Question 8. Clustering best practices

**Question:**  
It is best practice in Big Query to always cluster your data:

**Correct Answer:**  
✅ **`False`**

Cluster only when it aligns with your common filter/sort patterns; unnecessary clustering can increase costs.
---

## 9. Question 9. Understanding table scans

**Question:**  
No Points: Write a SELECT count(*) query FROM the materialized table you created. How many bytes does it estimate will be read? Why?

**Correct Answer:**  
✅ **`Usually 0 MB (BigQuery uses metadata/table statistics to answer count(*) without scanning data in many cases, especially for non-partitioned tables with up-to-date stats)`**

```sql
SELECT COUNT(*)
FROM kestra-sandbox-485710.ny_taxi.yellow_tripdata_non_partitioned;

```
---
