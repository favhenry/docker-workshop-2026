import os
import sys
import urllib.request
import gzip
import shutil
from concurrent.futures import ThreadPoolExecutor
from typing import List, Tuple, Optional
import time

import pandas as pd
from google.cloud import storage, bigquery
from google.api_core.exceptions import NotFound, Forbidden

# ────────────────────────────────────────────────
# CONFIGURATION
# ────────────────────────────────────────────────

PROJECT_ID = "kestra-sandbox-485710"
BUCKET_NAME = "henry_dezoomcamp_hw4_2026"
DATASET_ID = "nyc_taxi1"

CREDENTIALS_FILE = "Key.json"  # set to None if using ADC

BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"

DATASETS = [
    {"type": "yellow", "years": [2020]},
    {"type": "green",  "years": [2019, 2020]},
    {"type": "fhv",    "years": [2019]},
]

DOWNLOAD_DIR = "./nyc_taxi_data"
CHUNK_SIZE = 8 * 1024 * 1024
MAX_WORKERS = 4

# ────────────────────────────────────────────────
# CLIENTS
# ────────────────────────────────────────────────

if CREDENTIALS_FILE and os.path.exists(CREDENTIALS_FILE):
    storage_client = storage.Client.from_service_account_json(CREDENTIALS_FILE)
    bq_client = bigquery.Client.from_service_account_json(CREDENTIALS_FILE)
else:
    storage_client = storage.Client()
    bq_client = bigquery.Client()

bucket = storage_client.bucket(BUCKET_NAME)

# ────────────────────────────────────────────────
# HELPERS
# ────────────────────────────────────────────────

def get_csv_url(dataset_type: str, year: int, month: int) -> str:
    month_str = f"{month:02d}"
    return f"{BASE_URL}/{dataset_type}/{dataset_type}_tripdata_{year}-{month_str}.csv.gz"


def get_csv_path(dataset_type: str, year: int, month: int) -> str:
    return os.path.join(DOWNLOAD_DIR, f"{dataset_type}_tripdata_{year}-{month:02d}.csv.gz")


def get_parquet_path(dataset_type: str, year: int, month: int) -> str:
    return os.path.join(DOWNLOAD_DIR, f"{dataset_type}_tripdata_{year}-{month:02d}.parquet")


# ────────────────────────────────────────────────
# DOWNLOAD CSV.GZ
# ────────────────────────────────────────────────

def download_csv(args: Tuple[str, int, int]) -> Optional[Tuple[str, str]]:
    dataset_type, year, month = args
    url = get_csv_url(dataset_type, year, month)
    csv_path = get_csv_path(dataset_type, year, month)

    try:
        print(f"Downloading {url}")
        urllib.request.urlretrieve(url, csv_path)
        return (dataset_type, csv_path)
    except Exception:
        print(f"File not found: {url}")
        return None


# ────────────────────────────────────────────────
# CONVERT TO PARQUET
# ────────────────────────────────────────────────

def convert_to_parquet(dataset_type: str, csv_path: str) -> str:
    parquet_path = csv_path.replace(".csv.gz", ".parquet")

    print(f"Converting {csv_path} → {parquet_path}")

    df = pd.read_csv(csv_path, compression="gzip")
    df.to_parquet(parquet_path, engine="pyarrow", index=False)

    os.remove(csv_path)  # optional: remove raw file

    return parquet_path


# ────────────────────────────────────────────────
# UPLOAD TO GCS
# ────────────────────────────────────────────────

def upload_to_gcs(parquet_path: str):
    blob_name = os.path.basename(parquet_path)
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE

    print(f"Uploading {parquet_path} → gs://{BUCKET_NAME}/{blob_name}")
    blob.upload_from_filename(parquet_path)


# ────────────────────────────────────────────────
# LOAD TO BIGQUERY
# ────────────────────────────────────────────────

def load_to_bigquery(parquet_path: str):
    table_name = os.path.basename(parquet_path).replace(".parquet", "")
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
    )

    uri = f"gs://{BUCKET_NAME}/{os.path.basename(parquet_path)}"

    print(f"Loading {uri} → {table_id}")

    load_job = bq_client.load_table_from_uri(
        uri,
        table_id,
        job_config=job_config
    )
    load_job.result()


# ────────────────────────────────────────────────
# CREATE BQ DATASET IF NOT EXISTS
# ────────────────────────────────────────────────

def create_bq_dataset():
    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
    try:
        bq_client.get_dataset(dataset_ref)
    except NotFound:
        print(f"Creating dataset {DATASET_ID}")
        bq_client.create_dataset(dataset_ref)


# ────────────────────────────────────────────────
# MAIN
# ────────────────────────────────────────────────

def main():
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    create_bq_dataset()

    tasks = []
    for ds in DATASETS:
        for year in ds["years"]:
            for month in range(1, 13):
                tasks.append((ds["type"], year, month))

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = list(executor.map(download_csv, tasks))

    for result in results:
        if result:
            dataset_type, csv_path = result

            parquet_path = convert_to_parquet(dataset_type, csv_path)
            upload_to_gcs(parquet_path)
            load_to_bigquery(parquet_path)

    print("Pipeline completed successfully.")


if __name__ == "__main__":
    main()
