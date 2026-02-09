from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp

# Configuration
CITY_SOURCE_PATH = "/Volumes/city_travels_medallion/city_travels_raw_data/raw_data/city/"
TRIPS_SOURCE_PATH = "/Volumes/city_travels_medallion/city_travels_raw_data/raw_data/trips/Full Load/"

@dp.table(
    name="city_travels_medallion.a_bronze.bronze_trips",
    comment="Streaming ingestion of raw trips data with Auto Loader",
    table_properties={
        "quality": "bronze",
        "layer": "bronze",
        "source_format": "csv",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
)
# Optional: Only Warn to track quality metrics in the UI dashboard
@dp.expect("raw_data_quality_check", "trip_id IS NOT NULL")
def trips_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .load(TRIPS_SOURCE_PATH)
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("file_name", col("_metadata.file_path"))
        .withColumnRenamed("distance_travelled(km)", "distance_travelled_km")
    )

@dp.table(
    name="city_travels_medallion.a_bronze.bronze_city",
    comment="Streaming ingestion of raw city data with Auto Loader",
    table_properties={
        "quality": "bronze",
        "layer": "bronze",
        "source_format": "csv",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
)
@dp.expect("raw_data_quality_check", "city_id IS NOT NULL")
def city_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .load(CITY_SOURCE_PATH)
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("file_name", col("_metadata.file_path"))
    )
