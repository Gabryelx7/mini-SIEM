from datetime import datetime, timedelta
from airflow.sdk import task, dag

default_args = {
    'owner': 'security_operations',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

LOG_TYPES = ["auth_logs", "firewall_events", "api_gateway_logs"]

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
    def retention_cleanup(log_type: str):
        import subprocess
        print(f"Running VACUUM on {log_type}...")
        subprocess.run(
            ["python3", "/opt/airflow/spark_scripts/vacuum_tables.py", log_type], 
            check=True
        )
        return f"{log_type} vacuumed"

    @task
    def generate_security_reports(log_type: str):
        import subprocess, json

        print(f"Querying Gold layer for {log_type} metrics...")
        result = subprocess.run(
            ["python3", "/opt/airflow/spark_scripts/extract_metrics.py", log_type],
            capture_output=True, text=True, check=True
        )

        output_lines = result.stdout.strip().split('\n')
        metrics = json.loads(output_lines[-1])
        metrics['log_type'] = log_type
        
        return metrics
    
    @task
    def export_metrics_to_postgres(metrics: dict):
        import os
        import psycopg2
        from datetime import datetime
        
        conn = psycopg2.connect(
            host="postgres", database=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"), password=os.getenv("POSTGRES_PASSWORD")
        )
        cursor = conn.cursor()
        log_type = metrics.pop('log_type')
        today = datetime.now().date()

        if log_type == "auth_logs":
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS daily_auth (
                report_date DATE PRIMARY KEY, total_attempts INT, top_ip VARCHAR(50)
            );
            """)
            cursor.execute("""
            INSERT INTO daily_auth (report_date, total_attempts, top_ip)
            VALUES (%s, %s, %s) ON CONFLICT (report_date) DO UPDATE SET 
            total_attempts = EXCLUDED.total_attempts, top_ip = EXCLUDED.top_ip;
            """, (today, metrics.get('total_attempts', 0), metrics.get('top_ip', 'None')))

        elif log_type == "firewall_events":
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS daily_firewall (
                report_date DATE PRIMARY KEY, total_blocks INT, top_ip VARCHAR(50)
            );
            """)
            cursor.execute("""
            INSERT INTO daily_firewall (report_date, total_blocks, top_ip)
            VALUES (%s, %s, %s) ON CONFLICT (report_date) DO UPDATE SET 
            total_blocks = EXCLUDED.total_blocks, top_ip = EXCLUDED.top_ip;
            """, (today, metrics.get('total_blocks', 0), metrics.get('top_ip', 'None')))

        elif log_type == "api_gateway_logs":
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS daily_api (
                report_date DATE PRIMARY KEY, total_errors INT, top_ip VARCHAR(50)
            );
            """)
            cursor.execute("""
            INSERT INTO daily_api (report_date, total_errors, top_ip)
            VALUES (%s, %s, %s) ON CONFLICT (report_date) DO UPDATE SET 
            total_errors = EXCLUDED.total_errors, top_ip = EXCLUDED.top_ip;
            """, (today, metrics.get('total_errors', 0), metrics.get('top_ip', 'None')))

        conn.commit()
        cursor.close()
        conn.close()
        return f"Exported {log_type}"

    for log_type in LOG_TYPES:
        cleanup = retention_cleanup.override(task_id=f"cleanup_{log_type}")(log_type)
        report = generate_security_reports.override(task_id=f"extract_{log_type}")(log_type)
        export = export_metrics_to_postgres.override(task_id=f"export_{log_type}")(report)
        
        # Ensure cleanup runs before the extraction
        cleanup >> report >> export

summary_dag = daily_security_summary()