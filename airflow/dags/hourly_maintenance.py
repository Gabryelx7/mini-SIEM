from airflow.sdk import task, dag
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

LOG_TYPES = ["auth_logs", "firewall_events", "api_gateway_logs"]

@dag(
    dag_id='hourly_lakehouse_maintenance',
    default_args=default_args,
    description='Validates logs and compacts Delta files hourly',
    schedule='@hourly',
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=['siem', 'bronze', 'silver'],
)
def hourly_maintenance():

    @task
    def validate_logs():
        """
        Simulates a data quality check on the Bronze layer.
        In production, this could query MinIO to ensure files exist 
        and sizes are greater than 0 bytes.
        """
        print("Checking Bronze layer for malformed JSON thresholds...")
        # Simulated check passing
        return {"status": "passed", "log_type": log_type}

    @task
    def compact_files(validation_result: dict):
        """
        Triggers a Spark batch job to run OPTIMIZE on Delta tables.
        """
        import subprocess

        log_type = validation_result["log_type"]
        if validation_result["status"] != "passed":
            raise ValueError("Data validation failed, aborting compaction.")
        
        print(f"Triggering Spark compaction script for {log_type}")
        subprocess.run(
            ["python3", "/opt/airflow/spark_scripts/compact_tables.py", log_type],
        )

        print("Compaction Complete")
        return log_type

    @task
    def refresh_aggregates(log_type: str):
        """
        Updates any batch-based Gold metrics after files are compacted.
        """
        print(f"Refreshing threat intelligence tables for {log_type}.")

    for log_type in LOG_TYPES:
        val_output = validate_logs.override(task_id=f"validate_{log_type}")(log_type)
        comp_output = compact_files.override(task_id=f"compact_{log_type}")(val_output)
        refresh_aggregates.override(task_id=f"refresh_{log_type}")(comp_output)

hourly_maintenance()