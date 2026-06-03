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
    def export_metrics_to_postgres(metrics: dict):
        """
        Pushes the parsed Gold metrics into Postgres so Grafana can query them.
        """
        import os
        import psycopg2
        from datetime import datetime
        
        print("Connecting to Postgres database...")
        conn = psycopg2.connect(
            host="postgres",
            database=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD")
        )
        cursor = conn.cursor()

        create_table_query = """
        CREATE TABLE IF NOT EXISTS daily_siem_metrics (
            id SERIAL PRIMARY KEY,
            report_date DATE NOT NULL UNIQUE,
            total_brute_force_attempts INT,
            top_flagged_ip VARCHAR(50),
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        cursor.execute(create_table_query)

        insert_query = """
        INSERT INTO daily_siem_metrics (report_date, total_brute_force_attempts, top_flagged_ip)
        VALUES (%s, %s, %s)
        ON CONFLICT (report_date) 
        DO UPDATE SET 
            total_brute_force_attempts = EXCLUDED.total_brute_force_attempts,
            top_flagged_ip = EXCLUDED.top_flagged_ip,
            updated_at = CURRENT_TIMESTAMP;
        """
        
        today = datetime.now().date()
        cursor.execute(insert_query, (
            today, 
            metrics.get('total_brute_force_attempts', 0), 
            metrics.get('top_flagged_ip', 'None')
        ))

        conn.commit()
        cursor.close()
        conn.close()
        
        print("Successfully exported metrics to Postgres for Grafana.")
        return "Export Complete"

    @task
    def send_anomaly_summaries(cleanup_status: dict, metrics: dict, export_status: str):
        """
        Acts as the final notification layer.
        Could integrate with Slack or Email operators.
        """
        print(f"Cleanup Status: {cleanup_status}")
        print(f"Daily Summary Alert: {metrics['total_brute_force_attempts']} attempts detected today.")
        print(f"Primary Threat IP: {metrics['top_flagged_ip']}")
        print(f"Database Export Status: {export_status}")

    cleanup = retention_cleanup()
    metrics = generate_security_reports()

    export_status = export_metrics_to_postgres(metrics)
    
    send_anomaly_summaries(cleanup, metrics, export_status)

summary_dag = daily_security_summary()