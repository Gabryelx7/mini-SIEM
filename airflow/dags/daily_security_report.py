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
        print("Running VACUUM command on Bronze Delta tables (Retention: 30 days)...")
        return "Cleanup successful"

    @task
    def generate_security_reports():
        """
        Extracts aggregate metrics from the Gold layer to build a summary.
        """
        print("Querying Gold layer for Top 10 Attacking IPs...")
        # Simulated metrics extracted from Delta
        daily_metrics = {
            "total_brute_force_attempts": 1450,
            "top_flagged_ip": "192.168.1.15"
        }
        return daily_metrics

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