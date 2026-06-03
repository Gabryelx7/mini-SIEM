from datetime import datetime, timedelta
from airflow.sdk import task, dag

default_args = {
    'owner': 'security_operations',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='daily_security_summary',
    default_args=default_args,
    description='Generates daily security reports and manages data retention',
    schedule='@daily',
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=['siem', 'gold', 'reporting'],
)
def daily_security_summary():

    @task
    def retention_cleanup():
        """
        Removes logs older than 30 days from the Bronze layer to save MinIO storage.
        In Delta Lake, this is done using the VACUUM command.
        """
        import subprocess
        print("Running VACUUM command on Bronze Delta tables (Retention: 7 days)...")
        subprocess.run(
            ["python3", "/opt/airflow/spark_scripts/vacuum_tables.py"], 
            check=True
        )
        return "Cleanup successful"

    @task
    def generate_security_reports():
        """
        Extracts aggregate metrics from the Gold layer to build a summary.
        """
        import subprocess
        import json

        print("Querying Gold layer for metrics...")
        result = subprocess.run(
            ["python3", "/opt/airflow/spark_scripts/extract_metrics.py"],
            capture_output=True,
            text=True,
            check=True
        )

        output_lines = result.stdout.strip().split('\n')
        metrics = json.loads(output_lines[-1])

        return metrics

    @task
    def send_anomaly_summaries(metrics: dict, cleanup_status: str):
        """
        Acts as the final notification layer.
        Could integrate with Slack or Email operators.
        """
        print(f"Cleanup Status: {cleanup_status}")
        print(f"Daily Summary Alert: {metrics['total_brute_force_attempts']} attempts detected today.")
        print(f"Primary Threat IP: {metrics['top_flagged_ip']}")

    cleanup = retention_cleanup()
    metrics = generate_security_reports()
    
    send_anomaly_summaries(metrics, cleanup)

summary_dag = daily_security_summary()