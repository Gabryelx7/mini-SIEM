from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from session_builder import get_spark_session

def run_silver_pipeline():
    spark = get_spark_session("Silver_Cleaning_Auth")

    bronze_path = "s3a://siem-lakehouse/bronze/auth_logs"
    print(f"Reading Bronze stream from {bronze_path}...")
    
    bronze_df = spark.readStream \
        .format("delta") \
        .load(bronze_path)

    # Define the schema of the JSON payload
    auth_schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("source_ip", StringType(), True),
        StructField("username", StringType(), True),
        StructField("status", StringType(), True),
        StructField("port", IntegerType(), True),
        StructField("message", StringType(), True)
    ])

    silver_df = bronze_df.select(
        col("kafka_timestamp"),
        col("ingestion_timestamp"),
        from_json(col("raw_json"), auth_schema).alias("data")
    ).select(
        col("kafka_timestamp"),
        col("ingestion_timestamp"),
        col("data.*")
    )

    silver_path = "s3a://siem-lakehouse/silver/auth_logs"
    checkpoint_path = "s3a://siem-lakehouse/checkpoints/silver_auth_logs"

    print(f"Starting Silver stream. Writing to {silver_path}...")

    query = silver_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_path) \
        .start(silver_path)

    query.awaitTermination()

if __name__ == "__main__":
    run_silver_pipeline()