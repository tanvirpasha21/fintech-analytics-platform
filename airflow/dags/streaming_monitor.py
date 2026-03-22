"""
airflow/dags/streaming_monitor.py
===================================
Runs every 5 minutes. Watches the Kafka → DuckDB stream for:
  - Freshness: alerts if no new data in last 10 minutes
  - Anomalies: fraud rate spike, unusually high amounts
  - Summary: rolling stats printed to Airflow logs
"""

import os
from datetime import datetime, timedelta

import duckdb
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

BASE      = os.environ.get("AIRFLOW_HOME", os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
STREAM_DB = os.environ.get("STREAM_DB_PATH", os.path.join(BASE, "data", "fintech_stream.duckdb"))

default_args = {
    "owner": "tanvir",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=4),
}

with DAG(
    dag_id="streaming_monitor",
    description="Every 5 min: check stream freshness and detect anomalies",
    schedule="*/5 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["fintech", "streaming", "monitoring"],
) as dag:

    def check_freshness(**ctx):
        if not os.path.exists(STREAM_DB):
            print("Stream DB not found — consumer not started yet.")
            ctx["ti"].xcom_push(key="status", value="not_started")
            return
        con = duckdb.connect(STREAM_DB, read_only=True)
        try:
            r = con.execute("""
                SELECT count(*),
                       max(cast(event_time as timestamp)),
                       datediff('minute', max(cast(event_time as timestamp)), current_timestamp)
                FROM stream.raw_transactions
            """).fetchone()
            total, latest, lag = r
            print(f"Stream events: {total:,}  |  Latest: {latest}  |  Lag: {lag} min")
            if lag and lag > 10:
                raise Exception(f"ALERT: No new stream data for {lag} minutes — consumer may be down!")
            ctx["ti"].xcom_push(key="status", value={"total": total, "lag_min": lag})
        finally:
            con.close()

    def detect_anomalies(**ctx):
        if not os.path.exists(STREAM_DB):
            return
        con = duckdb.connect(STREAM_DB, read_only=True)
        try:
            r = con.execute("""
                SELECT count(*),
                       sum(case when is_flagged_fraud then 1 else 0 end),
                       round(avg(amount_gbp), 2)
                FROM (SELECT * FROM stream.raw_transactions ORDER BY consumed_at DESC LIMIT 100)
            """).fetchone()
            total, fraud, avg_amt = r
            if total > 0:
                fraud_rate = fraud / total * 100
                print(f"Last 100 events — fraud rate: {fraud_rate:.1f}%  avg amount: £{avg_amt:.2f}")
                if fraud_rate > 10:
                    print(f"🚨 ANOMALY: fraud rate {fraud_rate:.1f}% in last 100 events!")
                if avg_amt and avg_amt > 500:
                    print(f"⚠ ANOMALY: avg amount £{avg_amt:.2f} is unusually high!")
        finally:
            con.close()

    def stream_summary(**ctx):
        if not os.path.exists(STREAM_DB):
            print("Stream not started.")
            return
        con = duckdb.connect(STREAM_DB, read_only=True)
        try:
            r = con.execute("""
                SELECT count(*),
                       round(sum(amount_gbp), 2),
                       round(avg(amount_gbp), 2),
                       count(case when is_flagged_fraud then 1 end),
                       count(distinct customer_id)
                FROM stream.raw_transactions
            """).fetchone()
            print("\n" + "="*50)
            print(f"  Stream events    : {r[0]:,}")
            print(f"  Total volume     : £{r[1]:,.2f}")
            print(f"  Avg transaction  : £{r[2]:.2f}")
            print(f"  Fraud flags      : {r[3]:,}")
            print(f"  Unique customers : {r[4]:,}")
            print("="*50)
        finally:
            con.close()

    t1 = PythonOperator(task_id="check_freshness",    python_callable=check_freshness)
    t2 = PythonOperator(task_id="detect_anomalies",   python_callable=detect_anomalies, trigger_rule=TriggerRule.ALL_DONE)
    t3 = PythonOperator(task_id="stream_summary",     python_callable=stream_summary,   trigger_rule=TriggerRule.ALL_DONE)

    t1 >> t2 >> t3
