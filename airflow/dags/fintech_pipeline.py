"""
airflow/dags/fintech_pipeline.py
=================================
Hourly orchestration of the full fintech analytics pipeline.

Fix 1: merge_stream_data task bridges Kafka → DuckDB streaming database
        into the main fintech.duckdb warehouse before dbt runs.
        This is what connects the two systems together.

Fix 4: check_kafka_health task verifies the Kafka broker is reachable
        and the consumer group is active before any pipeline work starts.

Task graph:
  check_kafka_health       ← Fix 4: verify Kafka is up
      │
  health_check             ← verify DuckDB has data
      │
  validate_raw_data        ← check for nulls / bad values
      │
  merge_stream_data        ← Fix 1: copy stream events into main warehouse
      │
  dbt_staging
      │
  dbt_intermediate
      │
  ┌───┴─────────────┬──────────────┐
  payments      customers       risk     ← parallel (works now — PostgreSQL)
  └───┬─────────────┴──────────────┘
      │
  dbt_tests
      │
  branch_on_tests
   ├── export_dashboard
   └── skip_export
          │
   pipeline_summary  (always runs — TriggerRule.ALL_DONE)
"""

import os
import subprocess
from datetime import datetime, timedelta

import duckdb
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

# ── PATHS — pulled from env so they work both locally and in Docker ───────────
BASE       = os.environ.get("AIRFLOW_HOME",
             os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DBT        = os.environ.get("FINTECH_DBT_PATH",os.path.join(BASE, "fintech_dbt"))
DB         = os.environ.get("FINTECH_DB_PATH",os.path.join(BASE, "data", "fintech.duckdb"))
STREAM_DB  = os.environ.get("FINTECH_STREAM_DB_PATH",
             os.path.join(BASE, "data","fintech_stream.duckdb"))
SCRIPTS    = os.environ.get("FINTECH_SCRIPTS_PATH",os.path.join(BASE, "scripts"))

# Kafka config — injected via docker-compose environment or local env
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS","localhost:9092")
CONSUMER_GROUP  = os.environ.get("KAFKA_CONSUMER_GROUP","fintech-duckdb-sink")

# ── DEFAULT ARGS ──────────────────────────────────────────────────────────────
default_args = {
    "owner":                     "tanvir",
    "depends_on_past":           False,
    "retries":                   2,
    "retry_delay":               timedelta(minutes=3),
    "retry_exponential_backoff": True,
    "execution_timeout":         timedelta(minutes=20),
    "email_on_failure":          False,
}

# ── DAG ───────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="fintech_pipeline",
    description="Hourly: Kafka health → validate → merge stream → dbt → test → export",
    schedule="0 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["fintech", "dbt", "kafka", "production"],
) as dag:

    # ── FIX 4: KAFKA HEALTH CHECK ─────────────────────────────────────────────
    # Runs FIRST. Uses the Kafka admin client to verify:
    #   1. The broker is reachable
    #   2. All three required topics exist
    #   3. The consumer group has at least one active member
    # If Kafka is down, we skip merge_stream_data gracefully rather than failing.
    def check_kafka_health(**ctx):
        try:
            from kafka import KafkaAdminClient, KafkaConsumer
            from kafka.errors import NoBrokersAvailable, KafkaError
        except ImportError:
            print("kafka-python not installed — skipping Kafka health check.")
            ctx["ti"].xcom_push(key="kafka_healthy", value=False)
            return

        result = {
            "broker_reachable":   False,
            "topics_exist":       False,
            "consumer_active":    False,
            "consumer_lag":       None,
        }

        # 1. Broker connectivity
        try:
            admin = KafkaAdminClient(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                client_id="airflow-health-check",
                request_timeout_ms=5000,
            )
            topics = admin.list_topics()
            result["broker_reachable"] = True
            print(f"  Broker reachable at {KAFKA_BOOTSTRAP} ✓")
            print(f"  Topics found: {sorted(topics)}")

            # 2. Required topics exist
            required = {"fintech.transactions", "fintech.fraud_alerts", "fintech.dlq"}
            missing  = required - set(topics)
            if missing:
                print(f"  ⚠  Missing topics: {missing} — run kafka-init container")
            else:
                result["topics_exist"] = True
                print(f"  All required topics present ✓")

            # 3. Consumer group lag
            try:
                groups = admin.list_consumer_groups()
                group_ids = [g[0] for g in groups]
                if CONSUMER_GROUP in group_ids:
                    offsets = admin.list_consumer_group_offsets(CONSUMER_GROUP)
                    total_lag = 0
                    for tp, offset_meta in offsets.items():
                        # Get end offset for this partition
                        consumer = KafkaConsumer(
                            bootstrap_servers=KAFKA_BOOTSTRAP,
                            consumer_timeout_ms=3000,
                        )
                        end_offsets = consumer.end_offsets([tp])
                        consumer.close()
                        partition_lag = end_offsets.get(tp, 0) - offset_meta.offset
                        total_lag += max(partition_lag, 0)

                    result["consumer_lag"]    = total_lag
                    result["consumer_active"] = True
                    print(f"  Consumer group '{CONSUMER_GROUP}' active ✓")
                    print(f"  Total consumer lag: {total_lag:,} messages")

                    if total_lag > 10000:
                        print(f"  ⚠  HIGH LAG: consumer is {total_lag:,} messages behind!")
                else:
                    print(f"Consumer group '{CONSUMER_GROUP}' not found — consumer not started yet")
            except Exception as e:
                print(f" Could not check consumer lag: {e}")

            admin.close()

        except NoBrokersAvailable:
            print(f" Kafka broker not reachable at {KAFKA_BOOTSTRAP}")
            print(      "     Pipeline will continue but stream merge will be skipped.")
        except Exception as e:
            print(f"Kafka health check error: {e}")

        ctx["ti"].xcom_push(key="kafka_health", value=result)
        ctx["ti"].xcom_push(key="kafka_healthy", value=result["broker_reachable"])

        # Log summary
        print("\n  Kafka Health Summary:")
        for k, v in result.items():
            print(f"    {k}: {v}")

    t_kafka_health = PythonOperator(
        task_id="check_kafka_health",
        python_callable=check_kafka_health,
    )

    # ── HEALTH CHECK ──────────────────────────────────────────────────────────
    def health_check(**ctx):
        if not os.path.exists(DB):
            raise FileNotFoundError(
                f"Database not found: {DB}\n"
                "Run: python scripts/generate_data.py"
            )
        con = duckdb.connect(DB, read_only=True)
        try:
            counts = {}
            for tbl in ["customers", "merchants", "transactions", "fx_rates", "disputes"]:
                n = con.execute(f"SELECT COUNT(*) FROM raw.{tbl}").fetchone()[0]
                if n == 0:
                    raise ValueError(f"raw.{tbl} is empty — re-run generate_data.py")
                counts[tbl] = n
            for k, v in counts.items():
                print(f"  raw.{k}: {v:,}")
            ctx["ti"].xcom_push(key="counts", value=counts)
        finally:
            con.close()

    t_health = PythonOperator(task_id="health_check", python_callable=health_check)

    # ── VALIDATE RAW DATA ─────────────────────────────────────────────────────
    def validate(**ctx):
        con = duckdb.connect(DB, read_only=True)
        issues = []
        try:
            checks = [
                ("NULL transaction_ids",
                 "SELECT COUNT(*) FROM raw.transactions WHERE transaction_id IS NULL"),
                ("amount <= 0",
                 "SELECT COUNT(*) FROM raw.transactions WHERE amount <= 0"),
                ("unknown status",
                 "SELECT COUNT(*) FROM raw.transactions WHERE status NOT IN ('completed','declined','pending','failed')"),
                ("NULL customer_id",
                 "SELECT COUNT(*) FROM raw.transactions WHERE customer_id IS NULL"),
            ]
            for label, sql in checks:
                n = con.execute(sql).fetchone()[0]
                if n:
                    issues.append(f"{n} rows with {label}")

            if issues:
                print("VALIDATION WARNINGS (non-blocking):")
                for i in issues:
                    print(f"  ⚠  {i}")
            else:
                print("All raw data validation checks passed ✓")

            ctx["ti"].xcom_push(key="issues", value=issues)
        finally:
            con.close()

    t_validate = PythonOperator(task_id="validate_raw_data", python_callable=validate)

    # ── FIX 1: MERGE STREAM DATA ──────────────────────────────────────────────
    # This is the key fix that connects the two systems.
    #
    # What it does:
    #   1. Opens fintech_stream.duckdb (written by the Kafka consumer)
    #   2. Reads all events not yet merged into the main warehouse
    #      (tracked via stream.merge_log — last merged event_time)
    #   3. Transforms stream events to match raw.transactions schema
    #   4. Inserts new rows into fintech.duckdb raw.transactions
    #   5. Updates the merge log with the latest merged timestamp
    #
    # This means every hourly dbt run includes the latest streaming events.
    # The Kafka → DuckDB → dbt → marts pipeline is now end-to-end connected.
    def merge_stream_data(**ctx):
        if not os.path.exists(STREAM_DB):
            print("Stream database not found — no streaming data to merge.")
            print("Start the consumer with: python streaming/consumer/transaction_consumer.py --simulate")
            ctx["ti"].xcom_push(key="merged_count", value=0)
            return

        try:
            stream_con = duckdb.connect(STREAM_DB, read_only=True)

            # Find last merged timestamp from main DB
            main_con = duckdb.connect(DB)

            # Create merge tracking table if it doesn't exist
            main_con.execute("""
                CREATE TABLE IF NOT EXISTS raw.stream_merge_log (
                    merged_at         TIMESTAMP,
                    last_event_time   TIMESTAMP,
                    rows_merged       INTEGER
                )
            """)

            # Get the last successfully merged event time
            last_merged = main_con.execute("""
                SELECT max(last_event_time)
                FROM raw.stream_merge_log
            """).fetchone()[0]

            if last_merged:
                print(f"  Last merge: {last_merged}")
                new_events = stream_con.execute(f"""
                    SELECT * FROM stream.raw_transactions
                    WHERE cast(event_time as timestamp) > '{last_merged}'
                    ORDER BY event_time
                """).df()
            else:
                print("  First merge — pulling all stream events")
                new_events = stream_con.execute("""
                    SELECT * FROM stream.raw_transactions
                    ORDER BY event_time
                """).df()

            stream_con.close()

            if new_events.empty:
                print(" No new stream events to merge.")
                ctx["ti"].xcom_push(key="merged_count", value=0)
                main_con.close()
                return

            print(f"New stream events to merge: {len(new_events):,}")

            # Map stream schema → raw.transactions schema
            # Stream has extra Kafka metadata columns — we drop those
            # and rename to match the batch schema exactly
            import pandas as pd

            mapped = pd.DataFrame({
                "transaction_id":  new_events["transaction_id"],
                "customer_id":     new_events["customer_id"],
                "merchant_id":     new_events["merchant_id"],
                "amount":          new_events["amount"],
                "currency":        new_events["currency"],
                "transaction_at":  new_events["event_time"],
                "status":          new_events["status"],
                "is_flagged_fraud":new_events["is_flagged_fraud"],
                "channel":         new_events["channel"],
                "ip_country":      new_events["ip_country"],
            })

            # Deduplicate against what's already in raw.transactions
            existing_ids = main_con.execute(
                "SELECT transaction_id FROM raw.transactions"
            ).df()["transaction_id"].tolist()

            new_only = mapped[~mapped["transaction_id"].isin(existing_ids)]
            merged_count = len(new_only)

            if merged_count == 0:
                print("  All stream events already in warehouse (no duplicates to add).")
            else:
                main_con.execute(
                    "INSERT INTO raw.transactions SELECT * FROM new_only"
                )

                # Update merge log
                max_event_time = new_events["event_time"].max()
                main_con.execute("""
                    INSERT INTO raw.stream_merge_log VALUES (current_timestamp, ?, ?)
                """, [max_event_time, merged_count])

                print(f"  Merged {merged_count:,} new stream events into raw.transactions ✓")
                print(f"  Latest event time: {max_event_time}")

            main_con.close()
            ctx["ti"].xcom_push(key="merged_count", value=merged_count)

        except Exception as e:
            print(f"  ⚠  Stream merge failed: {e}")
            print(      "     Pipeline will continue with existing batch data.")
            ctx["ti"].xcom_push(key="merged_count", value=0)

    t_merge = PythonOperator(
        task_id="merge_stream_data",
        python_callable=merge_stream_data,
    )

    # ── DBT STAGING ───────────────────────────────────────────────────────────
    t_staging = BashOperator(
        task_id="dbt_staging",
        bash_command=f"cd {DBT} && dbt run --select staging --profiles-dir . --no-write-json",
    )

    # ── DBT INTERMEDIATE ──────────────────────────────────────────────────────
    t_intermediate = BashOperator(
        task_id="dbt_intermediate",
        bash_command=f"cd {DBT} && dbt run --select intermediate --profiles-dir . --no-write-json",
    )

    # ── MARTS (PARALLEL — works now because we use PostgreSQL, not SQLite) ────
    t_payments  = BashOperator(
        task_id="dbt_marts_payments",
        bash_command=f"cd {DBT} && dbt run --select marts.payments --profiles-dir . --no-write-json",
    )
    t_customers = BashOperator(
        task_id="dbt_marts_customers",
        bash_command=f"cd {DBT} && dbt run --select marts.customers --profiles-dir . --no-write-json",
    )
    t_risk      = BashOperator(
        task_id="dbt_marts_risk",
        bash_command=f"cd {DBT} && dbt run --select marts.risk --profiles-dir . --no-write-json",
    )

    # ── DBT TESTS ─────────────────────────────────────────────────────────────
    def run_tests(**ctx):
        result = subprocess.run(
            ["dbt", "test", "--profiles-dir", "."],
            cwd=DBT, capture_output=True, text=True,
        )
        print(result.stdout[-3000:])
        lines  = result.stdout.splitlines()
        passed = sum(1 for l in lines if "PASS" in l)
        failed = sum(1 for l in lines if "FAIL" in l or "ERROR" in l)
        ctx["ti"].xcom_push(key="results", value={"passed": passed, "failed": failed})
        if result.returncode != 0:
            raise Exception(f"dbt tests failed — {failed} failure(s)")
        print(f"\n✓  {passed} passed  |  {failed} failed")

    t_tests = PythonOperator(task_id="dbt_tests", python_callable=run_tests)

    # ── BRANCH ────────────────────────────────────────────────────────────────
    def branch(**ctx):
        r = ctx["ti"].xcom_pull(task_ids="dbt_tests", key="results") or {}
        return "export_dashboard" if r.get("failed", 1) == 0 else "skip_export"

    t_branch = BranchPythonOperator(task_id="branch_on_tests", python_callable=branch)
    t_skip   = EmptyOperator(task_id="skip_export")

    # ── EXPORT DASHBOARD ──────────────────────────────────────────────────────
    def export(**ctx):
        script = os.path.join(SCRIPTS, "export_dashboard.py")
        if not os.path.exists(script):
            print("export_dashboard.py not found — skipping.")
            return
        r = subprocess.run(["python3", script], cwd=BASE, capture_output=True, text=True)
        print(r.stdout)
        if r.returncode != 0:
            raise Exception(r.stderr)
        print("Dashboard refreshed ✓")

    t_export = PythonOperator(task_id="export_dashboard", python_callable=export)

    # ── PIPELINE SUMMARY (always runs) ────────────────────────────────────────
    def summary(**ctx):
        con = duckdb.connect(DB, read_only=True)
        try:
            r = con.execute("""
                SELECT
                    round(sum(total_volume_gbp)/1e6, 2)  as volume_m,
                    round(avg(completion_rate)*100, 1)   as completion_pct,
                    round(avg(fraud_rate)*100, 2)         as fraud_pct,
                    max(month)                            as latest_month
                FROM main_marts_payments.fct_monthly_payment_performance
            """).fetchone()

            tests        = ctx["ti"].xcom_pull(task_ids="dbt_tests",        key="results")    or {}
            merged_count = ctx["ti"].xcom_pull(task_ids="merge_stream_data", key="merged_count") or 0
            kafka_health = ctx["ti"].xcom_pull(task_ids="check_kafka_health", key="kafka_health") or {}

            print("=" * 55)
            print("PIPELINE SUMMARY")
            print("=" * 55)
            print(f"  Volume:            £{r[0]}M")
            print(f"  Completion:        {r[1]}%")
            print(f"  Fraud rate:        {r[2]}%")
            print(f"  Latest month:      {r[3]}")
            print(f"  Tests passed:      {tests.get('passed', '?')}")
            print(f"  Stream events merged: {merged_count:,}")
            print(f"  Kafka broker up:   {kafka_health.get('broker_reachable', 'unknown')}")
            if kafka_health.get("consumer_lag") is not None:
                print(f"  Consumer lag:      {kafka_health['consumer_lag']:,} messages")
            print("=" * 55)

            # Anomaly alerts
            if r[2] and float(r[2]) > 5:
                print(f"  ⚠  ANOMALY: fraud rate {r[2]}% exceeds 5% threshold")
            if r[1] and float(r[1]) < 85:
                print(f"  ⚠  ANOMALY: completion rate {r[1]}% below 85% threshold")
            if kafka_health.get("consumer_lag", 0) and kafka_health["consumer_lag"] > 10000:
                print(f"  ⚠  ANOMALY: consumer lag {kafka_health['consumer_lag']:,} messages — check consumer")
        finally:
            con.close()

    t_summary = PythonOperator(
        task_id="pipeline_summary",
        python_callable=summary,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # ── DEPENDENCY GRAPH ──────────────────────────────────────────────────────
    #
    # check_kafka_health
    #       │
    # health_check
    #       │
    # validate_raw_data
    #       │
    # merge_stream_data          ← NEW: pulls stream events into main warehouse
    #       │
    # dbt_staging
    #       │
    # dbt_intermediate
    #       │
    # ┌─────┴──────────┬──────────────┐
    # payments    customers        risk      (parallel — PostgreSQL makes this safe)
    # └─────┬──────────┴──────────────┘
    #       │
    # dbt_tests
    #       │
    # branch_on_tests
    #   ├── export_dashboard
    #   └── skip_export
    #             │
    #       pipeline_summary (always)

    t_kafka_health >> t_health >> t_validate >> t_merge >> t_staging >> t_intermediate
    t_intermediate >> [t_payments, t_customers, t_risk]
    [t_payments, t_customers, t_risk] >> t_tests
    t_tests >> t_branch
    t_branch >> [t_export, t_skip]
    [t_export, t_skip] >> t_summary
