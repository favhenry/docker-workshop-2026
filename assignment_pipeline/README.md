ASSIGNMENT SOLUTION SQL CODE 


Question 3. Counting short trips

For the trips in November 2025 (lpep_pickup_datetime between '2025-11-01' and '2025-12-01', exclusive of the upper bound), how many trips had a trip_distance of less than or equal to 1 mile?


SELECT COUNT(*) 
FROM green_taxi_trips
WHERE lpep_pickup_datetime >= '2025-11-01 00:00:00'
  AND lpep_pickup_datetime <  '2025-12-01 00:00:00'
  AND trip_distance <= 1;

Question 4. Longest trip for each day

Which was the pick up day with the longest trip distance? Only consider trips with trip_distance less than 100 miles (to exclude data errors).

SELECT 
    DATE(lpep_pickup_datetime) AS pickup_day,
    MAX(trip_distance) AS max_trip_distance
FROM green_taxi_trips
WHERE 
    trip_distance <= 100
    AND trip_distance > 0  -- optional: exclude 0-distance trips if they are errors
    AND lpep_pickup_datetime >= '2025-11-01'
    AND lpep_pickup_datetime < '2025-12-01'
GROUP BY DATE(lpep_pickup_datetime)
ORDER BY max_trip_distance DESC
LIMIT 1;

Question 5. Biggest pickup zone

Which was the pickup zone with the largest total_amount (sum of all trips) on November 18th, 2025?

SELECT 
    tz."Zone" AS pickup_zone,
    ROUND(SUM(gt.total_amount)::numeric, 2) AS total_revenue_usd
FROM green_taxi_trips gt
JOIN taxi_zones tz ON gt."PULocationID" = tz."LocationID"
WHERE DATE(gt.lpep_pickup_datetime) = '2025-11-18'
  AND gt.total_amount > 0
GROUP BY tz."Zone"
ORDER BY total_revenue_usd DESC
LIMIT 1;

Question 6. Largest tip

For the passengers picked up in the zone named "East Harlem North" in November 2025, which was the drop off zone that had the largest tip?

Note: it's tip , not trip. We need the name of the zone, not the ID

SELECT 
    dz."Zone" AS dropoff_zone,
    ROUND(MAX(gt.tip_amount)::numeric, 2) AS max_tip_usd
FROM green_taxi_trips gt
JOIN taxi_zones dz ON gt."DOLocationID" = dz."LocationID"
WHERE gt."PULocationID" = 74                      -- faster if you know East Harlem North = 74
  AND gt.lpep_pickup_datetime >= '2025-11-01'
  AND gt.lpep_pickup_datetime <  '2025-12-01'
  AND gt.tip_amount > 0
GROUP BY dz."Zone"
ORDER BY max_tip_usd DESC
LIMIT 1;
