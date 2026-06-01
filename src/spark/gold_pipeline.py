from pyspark.sql.functions import col, window
from session_builder import get_spark_session

def run_gold_pipeline():
    spark = get_spark_session("Gold_Aggregations_Auth")

    silver_path = "s3a://siem-lakehouse/silver/auth_logs"
    
    silver_df = spark.readStream \
        .format("delta") \
        .load(silver_path)

    processed_df = silver_df.withColumn("event_time", col("timestamp").cast("timestamp"))

    # Detect Brute Force Attempts: multiple failed logins from the same IP within a 5-minute sliding window.
    gold_df = processed_df \
        .filter(col("status") == "failed") \
        .withWatermark("event_time", "10 minutes") \
        .groupBy(
            window(col("event_time"), "5 minutes", "1 minute"),
            col("source_ip")
        ) \
        .count() \
        .withColumnRenamed("count", "failed_attempts")

    gold_path = "s3a://siem-lakehouse/gold/brute_force_metrics"
    checkpoint_path = "s3a://siem-lakehouse/checkpoints/gold_brute_force"

    print(f"Starting Gold aggregation stream. Writing to {gold_path}...")

    query = gold_df.writeStream \
        .format("delta") \
        .outputMode("complete") \
        .option("checkpointLocation", checkpoint_path) \
        .start(gold_path)

    query.awaitTermination()

if __name__ == "__main__":
    run_gold_pipeline()