CREATE OR REFRESH MATERIALIZED VIEW city_travels_medallion.c_gold.gold_city_performance
COMMENT "Aggregated city metrics for BI dashboards"
AS
SELECT 
  c.city_name,
  t.trip_date,
  t.passenger_type,
  COUNT(t.trip_id) AS total_trips,
  SUM(t.fare_amount) AS total_revenue,
  AVG(t.passenger_rating) AS avg_passenger_rating,
  AVG(t.driver_rating) AS avg_driver_rating,
  SUM(t.distance_travelled_km) AS total_distance_km
FROM city_travels_medallion.b_silver.silver_trips t
JOIN city_travels_medallion.b_silver.silver_city c 
  ON t.city_id = c.city_id
WHERE c.__END_AT IS NULL
GROUP BY c.city_name,  t.trip_date, t.passenger_type;


CREATE OR REFRESH MATERIALIZED VIEW city_travels_medallion.c_gold.gold_city_travel_details
COMMENT "City travel details for BI dashboards"
AS
SELECT 
  c.city_id,
  c.city_name,
  t.trip_date,
  t.passenger_type,
  t.distance_travelled_km,
  t.fare_amount,
  t.cost_per_km,
  t.day_of_week,
  t.passenger_rating,
  t.driver_rating
FROM city_travels_medallion.b_silver.silver_trips t
JOIN city_travels_medallion.b_silver.silver_city c 
  ON t.city_id = c.city_id
WHERE c.__END_AT IS NULL;
