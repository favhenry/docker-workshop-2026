#!/usr/bin/env python
# coding: utf-8

import click
import pandas as pd
from sqlalchemy import create_engine

# -----------------------------
# Yellow Taxi Configuration
# -----------------------------

YELLOW_DTYPES = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64",
}

YELLOW_PARSE_DATES = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
]

# -----------------------------
# External Data URLs
# -----------------------------

GREEN_PARQUET_URL = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    "green_tripdata_2025-11.parquet"
)

ZONES_CSV_URL = (
    "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/"
    "misc/taxi_zone_lookup.csv"
)

# -----------------------------
# CLI
# -----------------------------

@click.command()
@click.option("--pg_user", default="root", help="Postgres username")
@click.option("--pg_pass", default="root", help="Postgres password")
@click.option("--pg_host", default="localhost", help="Postgres host")
@click.option("--pg_port", default=5432, type=int, help="Postgres port")
@click.option("--pg_db", default="ny_taxi", help="Postgres database")
@click.option("--year", default=2021, type=int, help="Yellow taxi year")
@click.option("--month", default=1, type=int, help="Yellow taxi month")
@click.option("--yellow_table", default="yellow_taxi_trips")
@click.option("--green_table", default="green_taxi_trips")
@click.option("--zones_table", default="taxi_zones")
@click.option("--chunksize", default=100000, type=int)
def run(
    pg_user,
    pg_pass,
    pg_host,
    pg_port,
    pg_db,
    year,
    month,
    yellow_table,
    green_table,
    zones_table,
    chunksize,
):
    engine = create_engine(
        f"postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"
    )
    print("Connected to PostgreSQL")

    # =====================================================
    # 1. Yellow Taxi Ingestion (CSV, Chunked)
    # =====================================================

    yellow_url = (
        "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/"
        f"yellow_tripdata_{year}-{month:02d}.csv.gz"
    )

    print(f"Downloading Yellow Taxi data: {yellow_url}")

    yellow_iter = pd.read_csv(
        yellow_url,
        dtype=YELLOW_DTYPES,
        parse_dates=YELLOW_PARSE_DATES,
        iterator=True,
        chunksize=chunksize,
    )

    first_chunk = next(yellow_iter)

    first_chunk.head(0).to_sql(
        name=yellow_table,
        con=engine,
        if_exists="replace",
        index=False,
    )

    first_chunk.to_sql(
        name=yellow_table,
        con=engine,
        if_exists="append",
        index=False,
    )

    print(f"Inserted Yellow rows: {len(first_chunk)}")

    for chunk in yellow_iter:
        chunk.to_sql(
            name=yellow_table,
            con=engine,
            if_exists="append",
            index=False,
        )
        print(f"Inserted Yellow rows: {len(chunk)}")

    # =====================================================
    # 2. Green Taxi Ingestion (Parquet)
    # =====================================================

    print(f"Downloading Green Taxi data: {GREEN_PARQUET_URL}")

    green_df = pd.read_parquet(GREEN_PARQUET_URL)

    green_df.head(0).to_sql(
        name=green_table,
        con=engine,
        if_exists="replace",
        index=False,
    )

    for start in range(0, len(green_df), chunksize):
        chunk = green_df.iloc[start : start + chunksize]
        chunk.to_sql(
            name=green_table,
            con=engine,
            if_exists="append",
            index=False,
        )
        print(f"Inserted Green rows: {len(chunk)}")

    # =====================================================
    # 3. Taxi Zones Ingestion (Lookup Table)
    # =====================================================

    print(f"Downloading Taxi Zones data: {ZONES_CSV_URL}")

    zones_df = pd.read_csv(ZONES_CSV_URL)

    zones_df.to_sql(
        name=zones_table,
        con=engine,
        if_exists="replace",
        index=False,
    )

    print("Taxi zones table created")
    print("All datasets successfully ingested")

# -----------------------------
# Entry Point
# -----------------------------
if __name__ == "__main__":
    run()
