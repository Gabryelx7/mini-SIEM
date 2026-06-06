from pyspark.sql.functions import current_timestamp
from session_builder import get_spark_session

def run_bronze_pipeline(topic):
    spark = get_spark_session(f"Bronze_Ingestion_{topic.capitalize()}")

    # Read from Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load()

    # Cast the 'value' to string to store the raw JSON.
    bronze_df = kafka_df.selectExpr(
        "CAST(key AS STRING)",
        "CAST(value AS STRING) as raw_json",
        "topic",
        "partition",
        "offset",
        "timestamp as kafka_timestamp"
    ).withColumn("ingestion_timestamp", current_timestamp())

    # Write to Delta Lake
    bronze_path = f"s3a://siem-lakehouse/bronze/{topic}"
    checkpoint_path = f"s3a://siem-lakehouse/checkpoints/bronze_{topic}"

    print(f"Starting Bronze stream for {topic}. Writing to {bronze_path}...")

    query = bronze_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_path) \
        .start(bronze_path)

    # Block the thread so the stream stays alive
    query.awaitTermination()

if __name__ == "__main__":
    import sys
    topic = sys.argv[1] if len(sys.argv) > 1 else "auth_logs"
    run_bronze_pipeline(topic)