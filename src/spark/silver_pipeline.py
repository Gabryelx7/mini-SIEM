from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from session_builder import get_spark_session

# Define the JSON Payload schemas for each log type
auth_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("source_ip", StringType(), True),
    StructField("username", StringType(), True),
    StructField("status", StringType(), True),
    StructField("port", IntegerType(), True),
    StructField("message", StringType(), True)
])
firewall_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("traffic_class", StringType(), True),
    StructField("action", StringType(), True),
    StructField("source_ip", StringType(), True),
    StructField("source_port", IntegerType(), True),
    StructField("destination_ip", StringType(), True),
    StructField("destination_port", IntegerType(), True),
    StructField("protocol", StringType(), True),
    StructField("threat_level", StringType(), True),
    StructField("bytes_transferred", LongType(), True),
    StructField("rule_name", StringType(), True),
    StructField("firewall_id", StringType(), True)
])
api_gateway_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("method", StringType(), True),
    StructField("endpoint", StringType(), True),
    StructField("status", IntegerType(), True),
    StructField("client_ip", StringType(), True),
    StructField("username", StringType(), True),
    StructField("response_time_ms", IntegerType(), True),
    StructField("user_agent", StringType(), True),
    StructField("request_id", StringType(), True)
])

# Map topics to their respective schemas
SCHEMA_REGISTRY = {
    "auth_logs": auth_schema,
    "firewall_events": firewall_schema,
    "api_gateway_logs": api_gateway_schema
}

def run_silver_pipeline(topic):
    spark = get_spark_session(f"Silver_Cleaning_{topic.capitalize()}")
    schema = SCHEMA_REGISTRY[topic]

    bronze_path = f"s3a://siem-lakehouse/bronze/{topic}"
    print(f"Reading Bronze stream from {bronze_path}...")
    
    bronze_df = spark.readStream \
        .format("delta") \
        .load(bronze_path)

    silver_df = bronze_df.select(
        col("kafka_timestamp"),
        col("ingestion_timestamp"),
        from_json(col("raw_json"), schema).alias("data")
    ).select(
        col("kafka_timestamp"),
        col("ingestion_timestamp"),
        col("data.*")
    )

    silver_path = f"s3a://siem-lakehouse/silver/{topic}"
    checkpoint_path = f"s3a://siem-lakehouse/checkpoints/silver_{topic}"

    print(f"Starting Silver stream. Writing to {silver_path}...")

    query = silver_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_path) \
        .start(silver_path)

    query.awaitTermination()

if __name__ == "__main__":
    import sys
    topic = sys.argv[1] if len(sys.argv) > 1 else "auth_logs"
    run_silver_pipeline(topic)