from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import subprocess
import duckdb
import smtplib
from email.mime.text import MIMEText
import json


# Path configs (update these!)

DBT_PROJECT_DIR = "/opt/airflow/Healthcare_pipeline/dbt_project"
OUTPUT_DIR = "/opt/airflow/Healthcare_pipeline/output"
DUCKDB_FILE = "/opt/airflow/Healthcare_pipeline/data/medallion.duckdb"

ALERT_EMAIL = "kunal@teqfocus.com"

def run_ingestion():
    script_path = "/opt/airflow/Healthcare_pipeline/ingestion/ingest.py"
    subprocess.run(["python", script_path], check=True)

# 2️⃣ Data quality check
def data_quality_check():
    con = duckdb.connect(DUCKDB_FILE)
    issues = []

    # Tables to check
    tables_df = con.execute("SHOW TABLES").fetchdf()
    tables = tables_df['name'].tolist()  # List of table names

    for table in tables:
        # Check nulls
        null_counts = con.execute(f"""
            SELECT COUNT(*) AS cnt
            FROM {table}
            WHERE {" OR ".join([f"{col} IS NULL" for col in con.execute(f"PRAGMA table_info('{table}')").fetchdf()['name']])}
        """).fetchone()[0]
        if null_counts > 0:
            issues.append(f"{null_counts} nulls found in {table}")

        # Check duplicates on primary id column (assumes first column)
        pk_col = con.execute(f"PRAGMA table_info('{table}')").fetchdf().iloc[0]['name']
        dup_count = con.execute(f"""
            SELECT COUNT(*) - COUNT(DISTINCT {pk_col}) AS cnt
            FROM {table}
        """).fetchone()[0]
        if dup_count > 0:
            issues.append(f"{dup_count} duplicates found in {table}.{pk_col}")

    con.close()
    return issues

def run_dbt_tests():
    result = subprocess.run(
        ["dbt", "test", "--project-dir", DBT_PROJECT_DIR, "--profiles-dir", DBT_PROJECT_DIR],
        capture_output=True,
        text=True
    )

    failures = []
    if result.returncode != 0 or "FAIL" in result.stdout:
        failures.append(result.stdout)

    return failures

def monitor_pipeline():
    issues = data_quality_check()
    dbt_failures = run_dbt_tests()

    all_issues = issues + dbt_failures

    if all_issues:
        body = "Pipeline Issues Detected:\n\n" + "\n".join(all_issues)
        send_email(
            to=ALERT_EMAIL,
            subject="Airflow Pipeline Alert: Data Quality or DBT Test Failure",
            html_content=body
        )
    else:
        print("No issues detected. All data quality checks and dbt tests passed!")


def send_email(to: str, subject: str, html_content: str):
    # Replace with your SMTP server details
    SMTP_SERVER = "sandbox.smtp.mailtrap.io"
    SMTP_PORT = 587
    SMTP_USER = "8045dcbe0c4023"
    SMTP_PASSWORD = "dfb01929227b5d"

    msg = MIMEText(html_content, "html")
    msg["Subject"] = subject
    msg["From"] = SMTP_USER
    msg["To"] = to

    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(SMTP_USER, SMTP_PASSWORD)
        server.sendmail(SMTP_USER, [to], msg.as_string())
    print(f"Email sent to {to}")



# 2️⃣ CSV export task
def export_gold_to_csv():
    # Connect in read-only mode to avoid locking issues
    con = duckdb.connect(DUCKDB_FILE, read_only=True)
    df = con.execute("SELECT * FROM raw_analytic.all_data").fetchdf()
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    df.to_csv(os.path.join(OUTPUT_DIR, "all_data.csv"), index=False)
    con.close()
    print(f"CSV exported to {OUTPUT_DIR}/all_data.csv")

with DAG(
    dag_id="medallion_pipeline",
    start_date=datetime(2025, 9, 18),
    schedule_interval="@daily",
    catchup=False,
    tags=["medallion", "healthcare"],
) as dag:

    ingestion_task = PythonOperator(
        task_id="run_ingestion",
        python_callable=run_ingestion,
    )

    dbt_task = BashOperator(
        task_id="run_dbt",
        bash_command=f"dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}"
        
    )

    dbt_test_task = BashOperator(
    task_id="dbt_test",
    bash_command=f"dbt test --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} || true",
    )
    
    monitor_task = PythonOperator(
        task_id="monitor_pipeline",
        python_callable=monitor_pipeline
    )

    export_csv_task = PythonOperator(
        task_id="export_gold_csv",
        python_callable=export_gold_to_csv,
    )

    
    # ✅ Set DAG dependencies to prevent concurrent access
    ingestion_task >> dbt_task >> dbt_test_task >> monitor_task  >> export_csv_task
