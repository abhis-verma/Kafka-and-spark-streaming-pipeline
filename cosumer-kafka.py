from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType

# 1. Initialize Spark Session
print("🚀 Starting Spark Session...")
spark = SparkSession.builder \
    .appName("WeatherAggregator") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# 2. Define Schema (Must match the Producer's JSON)
schema = StructType() \
    .add("city", StringType()) \
    .add("temp", IntegerType()) \
    .add("timestamp", TimestampType())

# 3. Read Stream from Kafka
# Note: We use 'kafka:9092' because we are inside the Docker network
print("🔗 Connecting to Kafka...")
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "weather_data") \
    .option("startingOffsets", "latest") \
    .load()

# 4. Parse JSON Data
# Kafka sends data as bytes in the 'value' column. We cast to string and parse.
parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")) \
              .select("data.*")

# 5. Transformation: Sliding Window Aggregation
# Calculate average temperature per city every 5 seconds
windowed_counts = parsed_df.groupBy(
    window(col("timestamp"), "10 seconds", "5 seconds"),
    col("city")
).agg(avg("temp").alias("avg_temp"))

# 6. Output to Console (for debugging)
query = windowed_counts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

print("✅ Streaming started! Waiting for data...")
query.awaitTermination()