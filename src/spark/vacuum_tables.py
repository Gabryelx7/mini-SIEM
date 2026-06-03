import sys
from delta.tables import DeltaTable
from session_builder import get_spark_session

def run_vacuum():
    print("Initializing Spark Vacuum Job...")
    spark = get_spark_session("Batch_Vacuum_Job")

    # spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

    tables_to_vacuum = [
        "s3a://siem-lakehouse/bronze/auth_logs",
        "s3a://siem-lakehouse/silver/auth_logs"
    ]

    # Standard retention -> 168 hours (7 days)
    retention_hours = 168 

    for table_path in tables_to_vacuum:
        print(f"Running VACUUM on {table_path} (Retention: {retention_hours} hours)...")
        try:
            dt = DeltaTable.forPath(spark, table_path)
            dt.vacuum(retention_hours)
            print(f"Successfully vacuumed {table_path}")
        except Exception as e:
            print(f"Failed to vacuum {table_path}: {str(e)}")
            sys.exit(1)

    spark.stop()

if __name__ == "__main__":
    run_vacuum()