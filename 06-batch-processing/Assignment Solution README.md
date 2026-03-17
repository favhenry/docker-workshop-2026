# Data Engineering Zoomcamp 2026 – Module 6 Homework (Apache Spark – NYC Taxi Data) 

This repository contains my solution for **Module 6** of the [Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp) (2026 cohort) — focusing on **Apache Spark fundamentals and data processing** using NYC Taxi data.

## Assignment Overview

In this module, we applied **Apache Spark** to process large-scale taxi trip data efficiently.

Tasks included:

-Installing **Apache Spark** and **PySpark**
-Creating a local Spark session
-Reading Parquet datasets into Spark DataFrames
-Repartitioning and writing data
-Performing aggregations and filtering
-Using Spark SQL and temporary views
-Exploring Spark UI
-Joining datasets for analytical queries

Dataset used:

Source: NYC TLC Taxi Data

File: Yellow Taxi Trips (November 2025)

Format: Parquet

After setup, the pipeline processes NYC Taxi data and builds a full dependency graph from ingestion to final reporting tables

## Dataset used

- **Source**: NYC TLC Taxi Data
- **File**: Yellow Taxi Trips (November 2025)
- **Format**: Parquet


## 1. Question 1 Install Spark and PySpark

**Question:**  
Install Spark, run PySpark, create a local Spark session, and execute spark.version

**Correct Answer:**  
 **✅ 4.1.1**

**Explanation:**  
After installing Spark and initializing a session:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("test") \
    .getOrCreate()

spark
```

This returns the installed Spark version. 

> _Screenshot proof below:_

![Data_tripdata](https://github.com/favhenry/docker-workshop-2026/blob/main/06-batch-processing/Images/Week%205%20Question%201.PNG)



---

## 2. Question 2: Yellow November 2025 – File Size

**Question:**  
Repartition the DataFrame into 4 partitions and save as Parquet. What is the average file size?

**Correct Answer:**  
✅ **25MB**

**Explanation:**  
Steps performed:

```python

df_yellow_repart = df_yellow.repartition(4)
output_path = "data/pq/yellow/2025-11/"
df_yellow_repart.write.mode("overwrite").parquet(output_path)

import os
parquet_files = [f for f in os.listdir(output_path) if f.endswith(".parquet")]
sizes_bytes = [os.path.getsize(os.path.join(output_path, f)) for f in parquet_files]

avg_size_mb = sum(sizes_bytes) / len(sizes_bytes) / (1024*1024)
print(f"Average Parquet file size: {avg_size_mb:.2f} MB")

```

This shows each file is approximately **25.33MB**.

> _Screenshot proof below:_

![Data_tripdata](https://github.com/favhenry/docker-workshop-2026/blob/main/06-batch-processing/Images/Week%205%20Question%202.PNG)


---

## 3. Question 3. Count Records

**Question:**  
How many taxi trips started on ***15th November 2025**?


**Correct Answer:**  
✅ **162,604 trips**

**Explanation:**  

```python

from pyspark.sql.functions import col, to_date
# Filter for trips starting on 2025-11-15
trips_nov15 = df_yellow.filter(
    to_date(col("tpep_pickup_datetime")) == "2025-11-15"
    )

# Count the trips
num_trips_nov15 = trips_nov15.count()
print(num_trips_nov15)

```
This filters trips by pickup date and counts the records.

> _Screenshot proof below:_

![Data_tripdata](https://github.com/favhenry/docker-workshop-2026/blob/main/06-batch-processing/Images/Week%205%20Question%203.PNG)


---

## 4. Question 4. Longest Trip

**Question:**  
What is the longest trip duration in hours?


**Correct Answer:**  
✅ **The maximum trip duration is approximately 90.6 hours**

**Explanation:**  


```python

from pyspark.sql.functions import col, unix_timestamp, max
   
# Compute trip duration in hours
df_yellow_with_duration = df_yellow.withColumn(
  "trip_duration_hours"
  (unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime"))) / 3600
    )
 
# Find the maximum duration\n",
longest_trip = df_yellow_with_duration.select(max("trip_duration_hours")).collect()[0][0]
print(longest_trip)

```
The maximum trip duration is approximately 90.6 hours.

> _Screenshot proof below:_

![Data_tripdata](https://github.com/favhenry/docker-workshop-2026/blob/main/06-batch-processing/Images/Week%205%20Question%204.PNG)


---

## 5. Question 5. User Interface

**Question:**  
Spark UI runs on which local port?


**Correct Answer:**  
✅ **4040**

**Explanation:**  
By default, Spark launches its web UI at: 

```
http://localhost:4040
```
This interface shows jobs, stages, tasks, and execution details.

> _Screenshot proof below:_

![Data_tripdata](https://github.com/favhenry/docker-workshop-2026/blob/main/06-batch-processing/Images/Week%205%20Question%205.PNG)


---

## 6. Question 6. Least Frequent Pickup Location Zone

**Question:**  
Which pickup location zone is the least frequent?

**Correct Answer:**  
✅ **Governor's Island/Ellis Island/Liberty Island**


**Explanation:**  
Steps:

1. Load zone lookup data:

```python

df_zones = spark.read.option("header", True).csv("taxi_zone_lookup.csv")
df_zones.createOrReplaceTempView("zones")

```
2. Run SQL query:
```python

from pyspark.sql.functions import col, count

# Count trips per pickup location
pickup_counts = df_yellow.groupBy("PULocationID").agg(count("*").alias("trip_count"))

# Join with zone names
pickup_with_names = pickup_counts.join(df_zones, pickup_counts.PULocationID == df_zones.LocationID,"left")

# Find the zone with the least trips
least_frequent_zone = pickup_with_names.orderBy(col("trip_count").asc()).select("Zone").first()[0]
print(least_frequent_zone)

```
> _Screenshot proof below:_

![Data_tripdata](https://github.com/favhenry/docker-workshop-2026/blob/main/06-batch-processing/Images/Week%205%20Question%206%20.PNG)

---
