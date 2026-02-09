CREATE OR REFRESH STREAMING TABLE city_travels_medallion.b_silver.silver_city;

APPLY CHANGES INTO city_travels_medallion.b_silver.silver_city
FROM (
  SELECT 
    city_id,
    city_name,
    ingestion_timestamp, -- Must be here for SEQUENCE BY
    current_timestamp() AS silver_ingestion_timestamp 
  FROM STREAM(city_travels_medallion.a_bronze.bronze_city)
  WHERE city_id IS NOT NULL
)
KEYS (city_id)
SEQUENCE BY ingestion_timestamp
STORED AS SCD TYPE 2;


CREATE OR REFRESH STREAMING TABLE city_travels_medallion.b_silver.silver_trips (
  CONSTRAINT valid_distance EXPECT (distance_travelled_km > 0) ON VIOLATION DROP ROW,
  CONSTRAINT valid_trip_id EXPECT (trip_id IS NOT NULL) ON VIOLATION FAIL UPDATE
)
AS SELECT 
  trip_id, 
  CAST(t.date AS DATE) as trip_date, -- Ensure proper Date type
  city_id,
  TRIM(UPPER(passenger_type)) AS passenger_type,
  CAST(distance_travelled_km AS DOUBLE) AS distance_travelled_km,
  fare_amount,
  ROUND((fare_amount / distance_travelled_km), 2) AS cost_per_km,
  DATE_FORMAT(t.date, 'EEEE') AS day_of_week,
  COALESCE(passenger_rating, 0) AS passenger_rating,
  COALESCE(driver_rating, 0) AS driver_rating 
FROM STREAM(city_travels_medallion.a_bronze.bronze_trips) t;
