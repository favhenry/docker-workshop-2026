# Data Engineering Zoomcamp 2026 – Workflow Orchestration Quiz (Kestra)

This repository documents my answers to the **Workflow Orchestration Quiz** focused on **Kestra**, **ETL pipelines**, and **NYC Taxi data** as part of the DataTalksClub Data Engineering Zoomcamp.

The quiz consists of six multiple-choice questions validating understanding of:
- Kestra execution context and variables
- File extraction and sizing
- Data loading and row counts
- Timezone configuration in scheduled workflows

Where applicable, answers are supported with **SQL queries** and **execution screenshots**.

---

## 1. Yellow Taxi – December 2020 Uncompressed File Size

**Question:**  
Within the execution for Yellow Taxi data for the year **2020** and month **12**, what is the uncompressed file size of  
`yellow_tripdata_2020-12.csv`?

**Correct Answer:**  
✅ **134.5 MiB**

**Evidence:**  
This value is obtained directly from the **Kestra execution logs** of the `extract` task after decompression.

> _Screenshot proof below:_

![Yellow Taxi Dec 2020 File Size](https://github.com/favhenry/docker-workshop-2026/blob/main/02-Workflow-orcestration/images/Module%202%20question%201.PNG))

---

## 2. Rendered Value of the `file` Variable

**Question:**  
What is the rendered value of the variable `file` when:
- `taxi = green`
- `year = 2020`
- `month = 04`

**Correct Answer:**  
✅ **`green_tripdata_2020-04.csv`**

**Explanation:**  
Kestra renders variables using the `{{inputs.variable}}` syntax.  
Leading zeros in the month are preserved when passed as input.

```yaml
file: "{{inputs.taxi}}_tripdata_{{inputs.year}}-{{inputs.month}}.csv"

```
---

## 3. Total Rows – Yellow Taxi (All of 2020)

**Question:**  
How many rows are there for the Yellow Taxi data for all CSV files in 2020?

**Correct Answer:**  
✅ **`24,648,499 rows`**

```sql
SELECT 
  COUNT(*) AS total_rows_2020
FROM `kestra-sandbox-485710.zoomcamp.yellow_tripdata`
WHERE filename LIKE 'yellow_tripdata_2020-%';
```
> _Screenshot proof below:_

![Yellow Taxi Total 2020 Rows](https://github.com/favhenry/docker-workshop-2026/blob/main/02-Workflow-orcestration/images/Module%202%20question%203.PNG)


---

## 4. Total Rows – Green Taxi (All of 2020)

**Question:**  
How many rows are there for the Green Taxi data for all CSV files in 2020?

**Correct Answer:**  
✅ **`1,734,051 rows`**

```sql
SELECT 
  COUNT(*) AS total_rows_2020
FROM `kestra-sandbox-485710.zoomcamp.green_tripdata`
WHERE filename LIKE 'green_tripdata_2020-%';

```

> _Screenshot proof below:_

![Green Taxi Total 2020 Rows](https://github.com/favhenry/docker-workshop-2026/blob/main/02-Workflow-orcestration/images/Module%202%20question%204.PNG)
---

## 5. Yellow Taxi – March 2021 Row Count

**Question:**  
How many rows are there for the Yellow Taxi data for March 2021?

**Correct Answer:**  
✅ **`1,925,152 rows`**

```sql
SELECT 
  COUNT(*) AS total_rows_March2021
FROM `kestra-sandbox-485710.zoomcamp.yellow_tripdata`
WHERE filename = 'yellow_tripdata_2021-03.csv';
```
> _Screenshot proof below:_

![Green Taxi Total 2020 Rows](https://github.com/favhenry/docker-workshop-2026/blob/main/02-Workflow-orcestration/images/Module%202%20question%205.PNG)


---

## 6. Setting Timezone in a Kestra Schedule Trigger

**Question:**  
How would you configure the timezone to New York in a Schedule trigger?

**Correct Answer:**  
✅ **`Add a timezone property set to America/New_York`**

```yaml
triggers:
  - id: green_schedule
    type: io.kestra.plugin.core.trigger.Schedule
    cron: "0 9 1 * *"
    timezone: America/New_York
    inputs:
      taxi: green
```

---
