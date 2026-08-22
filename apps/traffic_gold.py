from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Spark Session Config

spark = (
    SparkSession.builder
    .appName("TrafficGoldLayer")
    # cluster master
    .master("spark://spark-master:7077")
    # delta lake
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .enableHiveSupport()
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Read Silver Silver

silver_stream = (
    spark.readStream
    .format("delta")
    .load("/opt/spark/warehouse/traffic_silver")
)

# Dimension Zone

dim_zone = silver_stream.select(
    "city_zone"
).dropDuplicates() \
.withColumn(
    "zone_type",
    when(col("city_zone") == "CBD", "Commercial")
    .when(col("city_zone") == "TECHPARK", "IT HUB")
    .when(col("city_zone").isin("AIRPORT", "TRAINSTATION"), "Transit Hub")
    .otherwise("Residential")
) \
.withColumn(
    "traffic_risk",
    when(col("city_zone").isin("CBD", "AIRPORT", "TRAINSTATION"), "HIGH")
    .when(col("city_zone") == "TECHPARK", "MEDIUM")
    .otherwise("LOW")
)

zone_query = (
    dim_zone.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/opt/spark/warehouse/chk/dim_zone")
    .option("path", "/opt/spark/warehouse/dim_zone")
    .start()
)

# Dimesion Road

dim_road = silver_stream.select(
    "road_id"
).dropDuplicates() \
.withColumn(
    "road_type" ,
    when(col("road_id").isin("R100", "R200"), "Highway")
    .otherwise("City Road")
) \
.withColumn(
    "speed_limit",
    when(col("road_id").isin("R100", "R200"), 100)
    .otherwise(60)
)

road_query = (
    dim_road.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/opt/spark/warehouse/chk/dim_road")
    .option("path", "/opt/spark/warehouse/dim_road")
    .start()
)

# Fact Table

fact_stream = silver_stream.select(
    "vehicle_id",
    "road_id",
    "city_zone",
    "speed_int",
    "congestion_level",
    "event_ts",
    "peak_flag",
    "speed_band",
    "hour",
    "weather"
)

fact_enriched = fact_stream.withColumn("date", to_date("event_ts"))

fact_query = (
    fact_enriched.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/opt/spark/warehouse/chk/fact_traffic")
    .option("path", "/opt/spark/warehouse/fact_traffic")
    .start()
)

spark.streams.awaitAnyTermination()