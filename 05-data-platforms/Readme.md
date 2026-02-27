# Data Engineering Zoomcamp 2026 – Module 5 Assignment on Data Platforms with Bruin (NYC Taxi Data Pipeline) 

This repository contains my solution for **Module 5** of the [Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp) (2026 cohort) — focusing on **Data Platforms with Bruin**.

## Assignment Overview

In this module, we used **Bruin** to build a complete data pipeline for NYC Taxi data — from ingestion to transformation, quality checks, and reporting.

Tasks included:

-Installing Bruin CLI
-Initializing the Zoomcamp template
-Configuring .bruin.yml with a DuckDB connection
-Defining assets and their dependencies
-Running incremental pipelines
-Implementing quality checks
-Overriding pipeline variables
-Visualizing pipeline lineage

After setup, the pipeline processes NYC Taxi data and builds a full dependency graph from ingestion to final reporting tables

## Dataset

- **Source**: NYC TLC Taxi Data
- **Taxi Types**: Yellow & Green
- **Processing Strategy**: Monthly (based on pickup_datetime)
- **Warehouse**: DuckDB
- **Transformation tools**: Bruin

> _Screenshot proof below:_

![Data_tripdata](https://github.com/favhenry/docker-workshop-2026/blob/main/04-Analytics%20Engineering/images/Module%205%20Successful%20run%20diagram.PNG)
![Data_tripdata](https://github.com/favhenry/docker-workshop-2026/blob/main/04-Analytics%20Engineering/images/Module%205%20lineage%20diagram.PNG)



## 1. Question 1 Bruin Pipeline Structure

**Question:**  
In a Bruin project, what are the required files/directories?

**Correct Answer:**  
 **✅ .bruin.yml and pipeline/ with pipeline.yml and assets/**

**Explanation:**  
A Bruin project requires:

- A `bruin.yml` file for project configuration (connections, environments, etc.)`
- A``pipeline/` directory
- A `pipeline.yml` file inside the pipeline directory
- An `assets/` directory containing asset definitions

These components define the execution logic and dependency structure. 

---

## 2. Question 2: Materialization Strategies

**Question:**  
You're building a pipeline that processes NYC taxi data organized by month based on pickup_datetime. Which incremental strategy is best for processing a specific interval period by deleting and inserting data for that time period ?

**Correct Answer:**  
✅ **time_interval**

**Explanation:**  
A `time_interval` is designed for incremental processing based on a time column.
It deletes and reloads data for a specific time window — ideal for monthly taxi data partitioned by pickup date.

---

## 3. Question 3. Pipeline Variables

**Question:**  
You have the following variable defined in `pipeline.yml`:

```yaml
variables:
  taxi_types:
    type: array
    items:
      type: string
    default: ["yellow", "green"]
```

How do you override this when running the pipeline to only process yellow taxis?

**Correct Answer:**  
✅ **`bruin run --var 'taxi_types=["yellow"]'`**

**Explanation:**  
Array variables must be passed in JSON format.
Using `--var 'taxi_types=["yellow"]'` correctly overrides the default array value.

---

## 4. Question 4. Running with Dependencies

**Question:**  
You've modified the `ingestion/trips.py` asset and want to run it plus all downstream assets. Which command should you use?


**Correct Answer:**  
✅ **`bruin run ingestion/trips.py --downstream`**

**Explanation:**  
The `--downstream` flag on the `bruin run` command runs the targeted asset and then everything that depends on it in the pipeline, which is exactly what you need after modifying `ingestion/trips.py`.

---

## 5. Question 5. Quality Checks

**Question:**  
You want to ensure the `pickup_datetime` column in your trips table never has NULL values. Which quality check should you add to your asset definition?


**Correct Answer:**  
✅ **`name: not_null`**

**Explanation:**  
The `not_null` quality check ensures that the column contains no NULL values.
This is essential for timestamp fields used in incremental strategies

---

## 6. Question 6. Lineage and Dependencies

**Question:**  
After building your pipeline, you want to visualize the dependency graph between assets. Which Bruin command should you use?

**Correct Answer:**  
✅ **`bruin lineage`**


**Explanation:**  
`bruin lineage` generates a visualization of the asset dependency DAG, helping verify upstream and downstream relationships.

---

## 7. Question 7.First-Time Run

**Question:**  
You're running a Bruin pipeline for the first time on a new DuckDB database. What flag should you use to ensure tables are created from scratch?

**Correct Answer:**  
✅ **`--full-refresh`**


**Explanation:**  
`--full-refresh` rebuilds all tables from scratch, bypassing incremental logic.
This is recommended for first-time executions or when resetting the warehouse state.


---
