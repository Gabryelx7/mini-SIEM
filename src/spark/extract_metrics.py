import json
from pyspark.sql.functions import sum as _sum
from session_builder import get_spark_session

def extract_daily_metrics():
    spark = get_spark_session("Batch_Metrics_Extraction")
    gold_path = "s3a://siem-lakehouse/gold/brute_force_metrics"

    try:
        df = spark.read.format("delta").load(gold_path)

        total_attempts = df.select(_sum("failed_attempts")).collect()[0][0] or 0

        top_ip_row = df.orderBy(df.failed_attempts.desc()).first()
        top_ip = top_ip_row["source_ip"] if top_ip_row else "None"

        metrics = {
            "total_brute_force_attempts": int(total_attempts),
            "top_flagged_ip": top_ip
        }

        print(json.dumps(metrics))

    except Exception as e:
        print(json.dumps({"total_brute_force_attempts": 0, "top_flagged_ip": "None", "error": str(e)}))

    finally:
        spark.stop()

if __name__ == "__main__":
    extract_daily_metrics()