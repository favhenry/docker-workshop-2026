"""@bruin
name: ingestion.trips
type: python
image: python:3.11

connection: motherduck-default

materialization:
  type: table
  strategy: append

columns:
  - name: pickup_datetime
    type: timestamp
    description: "When the meter was engaged"
  - name: dropoff_datetime
    type: timestamp
    description: "When the meter was disengaged"
@bruin"""
import os
import json
import pandas as pd
from datetime import datetime

def materialize():
    start_date_str = os.environ["BRUIN_START_DATE"]
    end_date_str = os.environ["BRUIN_END_DATE"]
    vars_dict = json.loads(os.environ.get("BRUIN_VARS", "{}"))
    taxi_types = vars_dict.get("taxi_types", ["yellow"])

    start = datetime.fromisoformat(start_date_str)
    end = datetime.fromisoformat(end_date_str)

    # Generate monthly ranges (MS = month start)
    months = pd.date_range(start, end, freq='MS')

    dfs = []
    for month in months:
        # Use the first taxi_type (usually 'yellow')
        taxi_type = taxi_types[0]
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{month:%Y-%m}.parquet"
        
        try:
            print(f"Loading {url}...")
            monthly_df = pd.read_parquet(url)
            # Add taxi_type column if needed (your staging expects it)
            monthly_df['taxi_type'] = taxi_type
            # Select only columns your staging asset uses (to avoid schema mismatches)
            # Core columns from NYC Yellow Taxi schema:
            # tpep_pickup_datetime → rename to pickup_datetime
            # tpep_dropoff_datetime → dropoff_datetime
            # PULocationID → pickup_location_id
            # DOLocationID → dropoff_location_id
            # payment_type
            # fare_amount
            monthly_df = monthly_df.rename(columns={
                'tpep_pickup_datetime': 'pickup_datetime',
                'tpep_dropoff_datetime': 'dropoff_datetime',
                'PULocationID': 'pickup_location_id',
                'DOLocationID': 'dropoff_location_id'
            })
            # Keep only needed columns
            needed_cols = [
                'pickup_datetime', 'dropoff_datetime',
                'pickup_location_id', 'dropoff_location_id',
                'fare_amount', 'payment_type', 'taxi_type'
            ]
            monthly_df = monthly_df[needed_cols]
            dfs.append(monthly_df)
        except Exception as e:
            print(f"Failed to load {url}: {e}")
            # Continue to next month

    if dfs:
        df = pd.concat(dfs, ignore_index=True)
        print(f"Loaded {len(df)} rows total.")
    else:
        df = pd.DataFrame()  # Empty if all failed
        print("No data loaded.")

    return df