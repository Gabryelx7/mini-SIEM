import sys
from delta.tables import DeltaTable
from session_builder import get_spark_session

def run_vacuum(topic):
    print(f"Initializing Spark Vacuum Job for {topic}")
    spark = get_spark_session(f"Batch_Vacuum_{topic}")

    tables_to_vacuum = [
        f"s3a://siem-lakehouse/bronze/{topic}",
        f"s3a://siem-lakehouse/silver/{topic}"
    ]

    retention_hours = 168 

    for table_path in tables_to_vacuum:
        print(f"Running VACUUM on {table_path} (Retention: {retention_hours} hours)...")
        try:
            dt = DeltaTable.forPath(spark, table_path)
            dt.vacuum(retention_hours)
            print(f"Successfully vacuumed {table_path}")
        except Exception as e:
            print(f"Failed to vacuum {table_path}: {str(e)}")
            spark.stop()
            sys.exit(1)

    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Error: Must provide topic name (e.g., auth_logs)")
        sys.exit(1)
    run_vacuum(sys.argv[1])