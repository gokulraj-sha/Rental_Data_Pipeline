from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, year, month

spark = SparkSession.builder.appName("rentalpipeline").getOrCreate()

# Paths
raw = "s3://rental-data-pipeline-vk-bucket01/raw/"
out = "s3://rental-data-pipeline-vk-bucket01/processed/"
db_name = "rental_analytics"

# Read datasets
vehicles = spark.read.option("header", True).option("inferSchema", True).csv(f"{raw}/vehicles/")
users = spark.read.option("header", True).option("inferSchema", True).csv(f"{raw}/users/")
locations = spark.read.option("header", True).option("inferSchema", True).csv(f"{raw}/locations/")
rentals = spark.read.option("header", True).option("inferSchema", True).csv(f"{raw}/rental_transactions/")


# Cleaning
rentals = (
    rentals
    .dropDuplicates(["rental_id"])
    .filter(col("rental_id").isNotNull())
    .withColumn("rental_start_ts", to_timestamp(col("rental_start_time"), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("rental_end_ts", to_timestamp(col("rental_end_time"), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("year", year(col("rental_start_ts")))
    .withColumn("month", month(col("rental_start_ts")))
)


users = users.dropDuplicates(["user_id"]).filter(col("user_id").isNotNull())
vehicles = vehicles.dropDuplicates(["vehicle_id"]).filter(col("vehicle_id").isNotNull())
locations = locations.dropDuplicates(["location_id"]).filter(col("location_id").isNotNull())


joined = (
    rentals.alias("r")
    .join(users.alias("u"), col("r.user_id") == col("u.user_id"), "left")
    .join(vehicles.alias("v"), col("r.vehicle_id") == col("v.vehicle_id"), "left")
    .join(locations.alias("pl"), col("r.pickup_location") == col("pl.location_id"), "left")
    .join(locations.alias("dl"), col("r.dropoff_location") == col("dl.location_id"), "left")
)


rental_processed = (
    joined
    .filter(col("r.rental_start_ts").isNotNull())
    .select(
        col("r.rental_id"),
        col("r.user_id"),
        col("u.first_name"),
        col("u.last_name"),
        col("r.vehicle_id"),
        col("v.brand"),
        col("v.vehicle_type"),
        col("r.rental_start_ts").alias("rental_start_time"),
        col("r.rental_end_ts").alias("rental_end_time"),
        col("r.total_amount").cast("double").alias("total_amount"),
        col("pl.location_name").alias("pickup_location_name"),
        col("dl.location_name").alias("dropoff_location_name"),
        col("r.year"),
        col("r.month"),
    )
)

rental_processed = rental_processed.fillna({
    "first_name": "Unknown",
    "last_name": "Unknown",
    "brand": "Unknown",
    "vehicle_type": "Unknown",
    "pickup_location_name": "Unknown",
    "dropoff_location_name": "Unknown"
})

print(f" Data Successfully Cleaned !!!")



spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name} LOCATION '{out}'")


rental_processed.write.format("parquet") \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .option("path", f"{out}/rental_processed") \
    .saveAsTable(f"{db_name}.rental_processed")

print(f"Glue Database '{db_name}' created and table 'rental_processed' registered successfully!")

spark.stop()
