from pyspark.sql.functions import col, window
from session_builder import get_spark_session

def aggregate_auth_logs(silver_df):
    # Detect Brute Force Attempts: multiple failed logins
    # from the same IP within a 5-minute sliding window.
    gold_df = silver_df \
        .filter(col("status") == "failed") \
        .withWatermark("event_time", "10 minutes") \
        .groupBy(
            window(col("event_time"), "5 minutes", "1 minute"),
            col("source_ip")
        ) \
        .count() \
        .withColumnRenamed("count", "failed_attempts")

    return gold_df

def aggregate_firewall_events(silver_df):
    # Detect IPs that are constantly hitting the firewall and
    # getting blocked (potential scanners or DoS).
    gold_df = silver_df \
        .filter(col("action") == "blocked") \
        .withWatermark("event_time", "5 minutes") \
        .groupBy(
            window(col("event_time"), "5 minutes", "1 minute"),
            col("source_ip"),
            col("threat_level")
        ) \
        .count() \
        .filter(col("count") > 50) \
        .withColumnRenamed("count", "blocked_connections")
    
    return gold_df

def aggregate_api_gateway_logs(silver_df):
    # Detect if a specific client IP is generating an abnormal amount of 4xx or 5xx errors
    gold_df = silver_df \
        .filter(col("status") >= 400) \
        .withWatermark("event_time", "5 minutes") \
        .groupBy(
            window(col("event_time"), "5 minutes", "1 minute"),
            col("client_ip"),
            col("endpoint")
        ) \
        .count() \
        .withColumnRenamed("count", "error_count")

    return gold_df

AGG_FUNCTIONS_REGISTRY = {
    "auth_logs": aggregate_auth_logs,
    "firewall_events": aggregate_firewall_events,
    "api_gateway_logs": aggregate_api_gateway_logs
}

GOLD_KEY_REGISTRY = {
    "auth_logs": "brute_force",
    "firewall_events": "firewall_blocked",
    "api_gateway_logs": "api_error"
}

def run_gold_pipeline(topic):
    spark = get_spark_session(f"Gold_Aggregations_{topic}")

    silver_path = f"s3a://siem-lakehouse/silver/{topic}"
    
    silver_df = spark.readStream \
        .format("delta") \
        .load(silver_path)

    processed_df = silver_df.withColumn("event_time", col("timestamp").cast("timestamp"))
    
    gold_df = AGG_FUNCTIONS_REGISTRY[topic](processed_df) 

    gold_path = f"s3a://siem-lakehouse/gold/{GOLD_KEY_REGISTRY[topic]}_metrics"
    checkpoint_path = f"s3a://siem-lakehouse/checkpoints/gold_{GOLD_KEY_REGISTRY[topic]}"

    print(f"Starting Gold aggregation stream. Writing to {gold_path}...")

    query = gold_df.writeStream \
        .format("delta") \
        .outputMode("complete") \
        .option("checkpointLocation", checkpoint_path) \
        .start(gold_path)

    query.awaitTermination()

if __name__ == "__main__":
    import sys
    topic = sys.argv[1] if len(sys.argv) > 1 else "auth_logs"
    run_gold_pipeline(topic)