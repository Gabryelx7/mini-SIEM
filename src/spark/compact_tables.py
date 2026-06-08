import sys
from session_builder import get_spark_session

def run_compaction(topic):
    print(f"Initializing Spark Compaction Job for {topic}")
    spark = get_spark_session(f"Batch_Compaction_{topic}")

    tables_to_compact = [
        f"s3a://siem-lakehouse/bronze/{topic}",
        f"s3a://siem-lakehouse/silver/{topic}"
    ]

    for table_path in tables_to_compact:
        print(f"Running OPTIMIZE on {table_path}...")
        try:
            spark.sql(f"OPTIMIZE delta.`{table_path}`")
            print(f"Successfully compacted {table_path}")
        except Exception as e:
            print(f"Failed to compact {table_path}: {str(e)}")
            spark.stop()
            sys.exit(1)

    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Error: Must provide topic name (e.g., auth_logs)")
        sys.exit(1)
    run_compaction(sys.argv[1])