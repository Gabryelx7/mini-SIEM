from airflow.sdk import task, dag
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

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
        return {"status": "passed", "malformed_ratio": 0.01}

    @task
    def compact_files(validation_result: dict):
        """
        Triggers a Spark batch job to run OPTIMIZE on Delta tables.
        Notice how TaskFlow automatically unpacks the dict from the previous task.
        """
        if validation_result["status"] != "passed":
            raise ValueError("Data validation failed, aborting compaction.")
        
        print("Validation passed. Triggering Spark compaction script...")
        
        # TO-DO: Add Spark batch jobs via bash/subprocess:
        # e.g., subprocess.run(["python", "/opt/airflow/spark_scripts/compact_tables.py"], check=True)
        
        return "Compaction Complete"

    @task
    def refresh_aggregates(compaction_status: str):
        """
        Updates any batch-based Gold metrics after files are compacted.
        """
        print(f"Status: {compaction_status}. Refreshing threat intelligence tables.")


    validation_output = validate_logs()
    compaction_output = compact_files(validation_output)
    refresh_aggregates(compaction_output)

hourly_maintenance()