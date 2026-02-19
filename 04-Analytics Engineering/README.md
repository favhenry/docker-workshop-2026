# Data Engineering Zoomcamp 2026 – Module 4 Assignment on dbt analytics engineering with NYC taxi data 

This repository contains my solution for **Module 4** of the [Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp) (2026 cohort) — focusing on **dbt analytics engineering**.

## Assignment Overview

In this module, we used dbt **(Data Build Tool)** to transform and model NYC Taxi data in BigQuery.

Tasks included:
-Loading Green and Yellow Taxi data (2019–2020) into Google Cloud Storage (GCS), BigQuery
-Running dbt build --target prod to create production models
-Exploring dbt lineage and model selection
-Implementing dbt tests
-Querying fact and dimension models
-Creating an additional staging model for FHV trip data

We work with **Records** from January 2019 to December 2020 (csv format) from the DataClub NYC TLC repo:  
[https://github.com/DataTalksClub/nyc-tlc-data/releases]

After a successful build, the following models were available in the production dataset:
-stg_green_tripdata
-stg_yellow_tripdata
-stg_fhv_tripdata
-dim_zones
-dim_vendors
-int_trips_unioned
-int_trips
-fct_trips
-fct_monthly_zone_revenue

## Dataset

- **Source**: DataClub NYC TLC repo
- **Green Taxi Data**: January 2019 – December 2020
- **Yellow Taxi Data**: January 2019 – December 2020
- **Additional Data**: FHV Trip Data (January 2019 – December 2019)
- **Warehouse**: BigQuery
- **Transformation tools**: dbt


Where applicable, answers are supported with **SQL queries** and **execution screenshots**.


## Steps Performed

1. **Data Loading**
   - Downloaded 60 CSV files from the DataClub NYC TLC repo
   - Used the provided Python script (`load_data.py`) to download the csv.gz, convert to parquet file and upload them to my GCS bucket and to BigQuery
   - Bucket path example: `gs://my-taxi-bucket/yellow_tripdata_2024-*.parquet`


2. **BigQuery Setup**
   - Created **table**:
     ```sql
     CREATE OR REPLACE TABLE `kestra-sandbox-485710.ny_taxi1.yellow_tripdata`
     OPTIONS (
       format = 'PARQUET',
       uris = ['gs://henry_dezoomcamp_hw4_2026/yellow_tripdata_20*.parquet']
     );
     
     ```
     ```sql
     CREATE OR REPLACE TABLE `kestra-sandbox-485710.ny_taxi1.green_tripdata`
     OPTIONS (
       format = 'PARQUET',
       uris = ['gs://henry_dezoomcamp_hw4_2026/green_tripdata_20*.parquet']
     );
     
     ```

      ```sql
     CREATE OR REPLACE TABLE `kestra-sandbox-485710.ny_taxi1.fhv_tripdata`
     OPTIONS (
       format = 'PARQUET',
       uris = ['gs://henry_dezoomcamp_hw4_2026/fhv_tripdata_20*.parquet']
     );
     
     ```
> _Screenshot proof below:_

![Data_tripdata](https://github.com/favhenry/docker-workshop-2026/blob/main/04-Analytics%20Engineering/images/Upload%20confirmation%20and%20combine%20table%20for%20yellow%20.PNG)
![Data_tripdata](https://github.com/favhenry/docker-workshop-2026/blob/main/04-Analytics%20Engineering/images/Upload%20confirmation%20and%20combine%20table%20for%20green.PNG)
![Data_tripdata](https://github.com/favhenry/docker-workshop-2026/blob/main/04-Analytics%20Engineering/images/Upload%20confirmation%20and%20combine%20table%20for%20fhv%20.PNG)


3. **dbt cloud Setup and model creation**
Set up the dbt cloud and from the studio, creation of the various model was done. In all Nine (9) models was created, run, tested and build with all their dependencies properly done 
      ```
> _Screenshot proof below:_

![ Model dbt build](https://github.com/favhenry/docker-workshop-2026/blob/main/04-Analytics%20Engineering/images/dbt%20model%20creation%20and%20build.PNG)

---

## 1. Question 1. dbt Lineage and Execution

**Question:**  
Given a dbt project with the following structure:

```
models/
├── staging/
│   ├── stg_green_tripdata.sql
│   └── stg_yellow_tripdata.sql
└── intermediate/
    └── int_trips_unioned.sql (depends on stg_green_tripdata & stg_yellow_tripdata)
```

If you run `dbt run --select int_trips_unioned`, what models will be built?

**Correct Answer:**  
 **✅ stg_green_tripdata, stg_yellow_tripdata, and int_trips_unioned (upstream dependencies)**

**Explanation:**  
By default, dbt builds the selected model and all of its upstream dependencies.
It does not build downstream models unless explicitly requested using selection operators like + 

---

## 2. Question 2: dbt Tests

**Question:**  
You've configured a generic test like this in your `schema.yml`:

```yaml
columns:
  - name: payment_type
    data_tests:
      - accepted_values:
          arguments:
            values: [1, 2, 3, 4, 5]
            quote: false
```

Your model `fct_trips` has been running successfully for months. A new value `6` now appears in the source data.

What happens when you run `dbt test --select fct_trips`?

**Correct Answer:**  
✅ **dbt will fail the test, returning a non-zero exit code**

**Explanation:**  
The generic accepted_values test checks that all column values match the allowed list.
Since value 6 is not included, dbt will:
Mark the test as failed
Return a non-zero exit code
Show failing records in the test output

---

## 3. Question 3. Counting Records in fct_monthly_zone_revenue

**Question:**  
After running your dbt project, query the `fct_monthly_zone_revenue` model.

What is the count of records in the `fct_monthly_zone_revenue` model?

**Correct Answer:**  
✅ **12,184 records**

```sql
SELECT COUNT(*) AS total_records
FROM {{ ref('fct_monthly_zone_revenue') }}

```

> _Screenshot proof below:_

![Query Proof](https://github.com/favhenry/docker-workshop-2026/blob/main/04-Analytics%20Engineering/images/Question%203.PNG)



---

## 4.### Question 4. Best Performing Zone for Green Taxis (2020)

**Question:**  
Using the `fct_monthly_zone_revenue` table, find the pickup zone with the **highest total revenue** (`revenue_monthly_total_amount`) for **Green** taxi trips in 2020.

Which zone had the highest revenue?


**Correct Answer:**  
✅ **`East Harlem North`** with **total revenue as 1,817,909.85**

```sql
SELECT
    pickup_zone,
    SUM(revenue_monthly_total_amount) AS total_revenue_2020
FROM {{ ref('fct_monthly_zone_revenue') }}
WHERE service_type = 'Green'
  AND EXTRACT(YEAR FROM revenue_month) = 2020
GROUP BY pickup_zone
ORDER BY total_revenue_2020 DESC
LIMIT 1;

```

> _Screenshot proof below:_

![ny_taxi.query proof](https://github.com/favhenry/docker-workshop-2026/blob/main/04-Analytics%20Engineering/images/Question%204.PNG)

---

## 5. ### Question 5. Green Taxi Trip Counts (October 2019)
**Question:**  
Using the `fct_monthly_zone_revenue` table, what is the **total number of trips** (`total_monthly_trips`) for Green taxis in October 2019

**Correct Answer:**  
✅ **384,624 trips**

```sql
SELECT 
    SUM(total_monthly_trips) AS total_trips_oct_2019
FROM {{ ref('fct_monthly_zone_revenue') }}
WHERE service_type = 'Green'
  AND EXTRACT(YEAR FROM revenue_month) = 2019
  AND EXTRACT(MONTH FROM revenue_month) = 10;
```

> _Screenshot proof below:_

![ny_taxi.query proof](https://github.com/favhenry/docker-workshop-2026/blob/main/04-Analytics%20Engineering/images/Question%205.PNG)
---

## 6. Question 6. Build a Staging Model for FHV Data

**Question:**  
1. Load the [FHV trip data for 2019](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv) into your data warehouse
2. Create a staging model `stg_fhv_tripdata` with these requirements:
   - Filter out records where `dispatching_base_num IS NULL`
   - Rename fields to match your project's naming conventions (e.g., `PUlocationID` → `pickup_location_id`)

What is the count of records in `stg_fhv_tripdata`

Step 1 – Create stg_fhv_tripdata.sql

```sql
with source as (
    select * from {{ source('raw_data', 'fhv_tripdata') }}
),

renamed as (
    select
        -- identifiers
        cast(dispatching_base_num as string) as dispatching_base_num,
        cast(pulocationid as integer) as pickup_location_id,
        cast(dolocationid as integer) as dropoff_location_id,

        -- timestamps
        cast(pickup_datetime as timestamp) as pickup_datetime,
        cast(dropOff_datetime as timestamp) as dropoff_datetime,

        -- trip info
        cast(SR_Flag as integer) as sr_flag,
        cast(Affiliated_base_number as string) as affiliated_base_number,

    from source
    -- Filter out records with null vendor_id (data quality requirement)
    where dispatching_base_num IS NOT NULL
)

select * from renamed

-- Sample records for dev environment using deterministic date filter
{% if target.name == 'dev' %}
where pickup_datetime >= '2019-01-01' and pickup_datetime < '2019-02-01'
{% endif %}
```
Key Requirements Implemented:
-Filtered out dispatching_base_num IS NULL
-Renamed fields to match snake_case naming conventions
-Standardized column structure with existing staging models

Step 2 – Update the schema.yml to reflect the new source then run, test and build the dbt model. 
**Correct Answer:**  
✅ **`43,244,693 records`**

```sql
SELECT COUNT(*) AS total_records
FROM {{ ref('stg_fhv_tripdata') }};
```


> _Screenshot proof below:_

![ny_taxi.yellow_tripdata_non_partitioned](https://github.com/favhenry/docker-workshop-2026/blob/main/04-Analytics%20Engineering/images/Question%206a.PNG)
![ny_taxi.yellow_tripdata_non_partitioned](https://github.com/favhenry/docker-workshop-2026/blob/main/04-Analytics%20Engineering/images/Question%206b.PNG)
---


---
