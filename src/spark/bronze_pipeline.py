from pyspark.sql.functions import current_timestamp
from session_builder import get_spark_session

def run_bronze_pipeline():
    spark = get_spark_session("Bronze_Ingestion_Auth")

    # Read from Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "auth_logs") \
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

    # Write to Delta Lake (MinIO)
    bronze_path = "s3a://siem-lakehouse/bronze/auth_logs"
    checkpoint_path = "s3a://siem-lakehouse/checkpoints/bronze_auth_logs"

    print(f"Starting Bronze stream for auth_logs. Writing to {bronze_path}...")

    query = bronze_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_path) \
        .start(bronze_path)

    # Block the thread so the stream stays alive
    query.awaitTermination()

if __name__ == "__main__":
    run_bronze_pipeline()