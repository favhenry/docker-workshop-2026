import json
import dataclasses

from dataclasses import dataclass


@dataclass
class Ride:
    PULocationID: int
    DOLocationID: int
    trip_distance: float
    total_amount: float
    tpep_pickup_datetime: int  # epoch milliseconds

@dataclass
class trips:
    lpep_pickup_datetime: int   # epoch milliseconds
    lpep_dropoff_datetime: int  # epoch milliseconds
    PULocationID: int
    DOLocationID: int
    passenger_count: float
    trip_distance: float
    tip_amount: float
    total_amount: float
    


def ride_from_row(row):
    return Ride(
        PULocationID=int(row['PULocationID']),
        DOLocationID=int(row['DOLocationID']),
        trip_distance=float(row['trip_distance']),
        total_amount=float(row['total_amount']),
        tpep_pickup_datetime=int(row['tpep_pickup_datetime'].timestamp() * 1000),
    )

def trip_from_row(row):
    return trips(
        lpep_pickup_datetime=int(row['lpep_pickup_datetime'].timestamp() * 1000),  
        lpep_dropoff_datetime=int(row['lpep_dropoff_datetime'].timestamp()*1000),
        PULocationID=int(row['PULocationID']),
        DOLocationID=int(row['DOLocationID']),
        passenger_count=float(row['passenger_count']),
        trip_distance=float(row['trip_distance']),
        tip_amount=float(row['tip_amount']),
        total_amount=float(row['total_amount']),
    )

def ride_serializer(ride):
    ride_dict = dataclasses.asdict(ride)
    ride_json = json.dumps(ride_dict).encode('utf-8')
    return ride_json

def trip_serializer(trips):
    trip_dict = dataclasses.asdict(trips)
    trip_json = json.dumps(trip_dict).encode('utf-8')
    return trip_json

def ride_deserializer(data):
    json_str = data.decode('utf-8')
    ride_dict = json.loads(json_str)
    return Ride(**ride_dict)

def trip_deserializer(data):
    json_str = data.decode('utf-8')
    trip_dict = json.loads(json_str)
    return trips(**trip_dict)