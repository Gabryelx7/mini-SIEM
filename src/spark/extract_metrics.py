import json
import sys
from pyspark.sql.functions import sum as _sum, col
from session_builder import get_spark_session

def get_auth_metrics(spark):
    gold_path = "s3a://siem-lakehouse/gold/brute_force_metrics"
    df = spark.read.format("delta").load(gold_path)

    total = df.select(_sum("failed_attempts")).collect()[0][0] or 0
    top_row = df.orderBy(col("failed_attempts").desc()).first()
    top_ip = top_row["source_ip"] if top_row else "None"

    metrics = {
        "total_attempts": int(total),
        "top_ip": top_ip
    }
    return metrics

def get_firewall_metrics(spark):
    gold_path = "s3a://siem-lakehouse/gold/firewall_blocked_metrics"
    df = spark.read.format("delta").load(gold_path)

    total = df.select(_sum("blocked_connections")).collect()[0][0] or 0
    top_row = df.orderBy(col("blocked_connections").desc()).first()
    top_ip = top_row["source_ip"] if top_row else "None"

    metrics = {
        "total_blocks": int(total),
        "top_ip": top_ip
    }

    return metrics

def get_api_gateway_metrics(spark):
    gold_path = "s3a://siem-lakehouse/gold/api_error_metrics"
    df = spark.read.format("delta").load(gold_path)

    total = df.select(_sum("error_count")).collect()[0][0] or 0
    top_row = df.orderBy(col("error_count").desc()).first()
    top_ip = top_row["source_ip"] if top_row else "None"

    metrics = {
        "total_errors": int(total),
        "top_ip": top_ip
    }

    return metrics

METRICS_FUNCTIONS_REGISTRY = {
    "auth_logs": get_auth_metrics,
    "firewall_events": get_firewall_metrics,
    "api_gateway_logs": get_api_gateway_metrics
}

def extract_daily_metrics(topic):
    spark = get_spark_session(f"Batch_Metrics_{topic}")
    metrics ={}

    try:
        metrics = METRICS_FUNCTIONS_REGISTRY[topic](spark)
        print(json.dumps(metrics))

    except Exception as e:
        print(json.dumps({"error": str(e), "total_attempts": 0, "total_blocks": 0, "total_errors": 0, "top_ip": "None"}))
        sys.exit(1)

    finally:
        spark.stop()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Error: Must provide topic name (e.g., auth_logs)")
        sys.exit(1)
    extract_daily_metrics(sys.argv[1])