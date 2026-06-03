import sys
from session_builder import get_spark_session

def run_compaction():
    print("Initializing Spark Compaction Job...")
    spark = get_spark_session("Batch_Compaction_Job")

    tables_to_compact = [
        "s3a://siem-lakehouse/bronze/auth_logs",
        "s3a://siem-lakehouse/silver/auth_logs"
    ]

    for table_path in tables_to_compact:
        print(f"Running OPTIMIZE on {table_path}...")
        try:
            spark.sql(f"OPTIMIZE delta.`{table_path}`")
            print(f"Successfully compacted {table_path}")
        except Exception as e:
            print(f"Failed to compact {table_path}: {str(e)}")
            sys.exit(1)

    spark.stop()

if __name__ == "__main__":
    run_compaction()