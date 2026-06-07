import os
from pyspark.sql.functions import current_timestamp
from session_builder import get_spark_session

def create_bronze_stream(spark_session, topic):
    # Read from Kafka
    kafka_df = spark_session.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", os.getenv("KAFKA_PORT")) \
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

    return query

def run_unified_bronze_pipeline():
    spark = get_spark_session("Unified_Bronze_Ingestion")

    auth_query = create_bronze_stream(spark, "auth_logs")
    fw_query = create_bronze_stream(spark, "firewall_events")
    api_query = create_bronze_stream(spark, "api_gateway_logs")

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    run_unified_bronze_pipeline()