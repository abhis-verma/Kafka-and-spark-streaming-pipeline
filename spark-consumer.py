import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType

# 1. Initialize Spark
# Note: "local[*]" uses all available cores.
# We also add the Kafka package configuration explicitly here for safety.
spark = SparkSession.builder \
    .appName("WeatherAlertSystem") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# 2. Define Schema
# Kafka sends data as bytes. We need to tell Spark that the 'value' is JSON
# and looks like: {"city": "London", "temperature": 32, ...}
weather_schema = StructType() \
    .add("city", StringType()) \
    .add("temp", FloatType()) \
    .add("timestamp", StringType())
    # .add("humidity", IntegerType()) \


# 3. Connect to Kafka Stream
# distinct change: format is 'kafka', not 'socket'
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "weather_data") \
    .option("startingOffsets", "latest") \
    .load()

# 4. Parse the Data
# Kafka stores the message in a column named 'value' (as binary).
# We cast it to String, then parse the JSON using our schema.
json_df = df.select(from_json(col("value").cast("string"), weather_schema).alias("data")).select("data.*")

# 5. The Logic: Filter for "High Heat" (> 30 degrees)
alert_df = json_df.filter(col("temp") > 30)

# 6. Output to Console
print("🚀 Spark Streaming is connected to Kafka... Waiting for high temps (>30)...")

query = alert_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()