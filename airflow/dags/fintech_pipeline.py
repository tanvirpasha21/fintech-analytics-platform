"""
airflow/dags/fintech_pipeline.py
=================================
Hourly orchestration of the full fintech analytics pipeline.

Task graph:
  health_check
      │
  validate_raw_data
      │
  dbt_staging
      │
  dbt_intermediate
      │
  ┌───┴───────────┬──────────────┐
  payments    customers       risk          ← parallel
  └───┬───────────┴──────────────┘
      │
  dbt_tests
      │
  branch ──── export_dashboard
      └────── skip_export
                   │
             pipeline_summary  (always runs)
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

# ── PATHS ─────────────────────────────────────────────────────────────────────
# Works whether running locally or inside the Docker container
BASE   = os.environ.get("AIRFLOW_HOME", os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DBT    = os.path.join(BASE, "fintech_dbt")
DB     = os.environ.get("FINTECH_DB_PATH", os.path.join(BASE, "data", "fintech.duckdb"))
SCRIPTS = os.path.join(BASE, "scripts")

# ── DEFAULT ARGS ──────────────────────────────────────────────────────────────
default_args = {
    "owner":            "tanvir",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=3),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=20),
    "email_on_failure": False,   # set True + add email in production
}

# ── DAG ───────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="fintech_pipeline",
    description="Hourly: validate → dbt → test → export dashboard",
    schedule="0 * * * *",        # every hour — change to @daily for local dev
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["fintech", "dbt", "production"],
) as dag:

    # ── 1. HEALTH CHECK ───────────────────────────────────────────────────────
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
            print("Health check passed:")
            for t, n in counts.items():
                print(f"  raw.{t}: {n:,}")
            ctx["ti"].xcom_push(key="raw_counts", value=counts)
        finally:
            con.close()

    t_health = PythonOperator(task_id="health_check", python_callable=health_check)

    # ── 2. VALIDATE RAW DATA ──────────────────────────────────────────────────
    def validate_raw(**ctx):
        con = duckdb.connect(DB, read_only=True)
        issues = []
        try:
            checks = [
                ("SELECT COUNT(*) FROM raw.transactions WHERE transaction_id IS NULL",
                 "null transaction_ids"),
                ("SELECT COUNT(*) FROM raw.transactions WHERE amount <= 0",
                 "amount <= 0"),
                ("SELECT COUNT(*) FROM raw.transactions WHERE status NOT IN ('completed','declined','pending','failed')",
                 "invalid status values"),
                ("SELECT 6 - COUNT(DISTINCT currency) FROM raw.fx_rates",
                 "missing FX currencies"),
            ]
            for sql, label in checks:
                n = con.execute(sql).fetchone()[0]
                if n > 0:
                    issues.append(f"{n} rows with {label}")

            if issues:
                for i in issues:
                    print(f"  ⚠ {i}")
            else:
                print("All validation checks passed ✓")
            ctx["ti"].xcom_push(key="issues", value=issues)
        finally:
            con.close()

    t_validate = PythonOperator(task_id="validate_raw_data", python_callable=validate_raw)

    # ── 3-4. DBT STAGING + INTERMEDIATE ──────────────────────────────────────
    t_staging = BashOperator(
        task_id="dbt_staging",
        bash_command=f"cd {DBT} && dbt run --select staging --profiles-dir . 2>&1",
        sla=timedelta(minutes=5),
    )

    t_intermediate = BashOperator(
        task_id="dbt_intermediate",
        bash_command=f"cd {DBT} && dbt run --select intermediate --profiles-dir . 2>&1",
        sla=timedelta(minutes=5),
    )

    # ── 5-7. DBT MARTS — run in parallel ──────────────────────────────────────
    t_payments = BashOperator(
        task_id="dbt_marts_payments",
        bash_command=f"cd {DBT} && dbt run --select marts.payments --profiles-dir . 2>&1",
    )
    t_customers = BashOperator(
        task_id="dbt_marts_customers",
        bash_command=f"cd {DBT} && dbt run --select marts.customers --profiles-dir . 2>&1",
    )
    t_risk = BashOperator(
        task_id="dbt_marts_risk",
        bash_command=f"cd {DBT} && dbt run --select marts.risk --profiles-dir . 2>&1",
    )

    # ── 8. DBT TESTS ──────────────────────────────────────────────────────────
    def run_tests(**ctx):
        result = subprocess.run(
            ["dbt", "test", "--profiles-dir", "."],
            cwd=DBT, capture_output=True, text=True
        )
        print(result.stdout)
        if result.returncode != 0:
            raise Exception(f"dbt tests failed:\n{result.stdout[-2000:]}")
        lines  = result.stdout.splitlines()
        passed = sum(1 for l in lines if "PASS" in l)
        failed = sum(1 for l in lines if "FAIL" in l or "ERROR" in l)
        ctx["ti"].xcom_push(key="test_results", value={"passed": passed, "failed": failed})
        print(f"\nTests: {passed} passed, {failed} failed")

    t_tests = PythonOperator(task_id="dbt_tests", python_callable=run_tests)

    # ── 9. BRANCH — skip export if tests failed ───────────────────────────────
    def branch_fn(**ctx):
        res = ctx["ti"].xcom_pull(task_ids="dbt_tests", key="test_results") or {}
        return "export_dashboard" if res.get("failed", 1) == 0 else "skip_export"

    t_branch  = BranchPythonOperator(task_id="branch_on_tests", python_callable=branch_fn)
    t_skip    = EmptyOperator(task_id="skip_export")

    # ── 10. EXPORT DASHBOARD ──────────────────────────────────────────────────
    def export_dashboard(**ctx):
        script = os.path.join(SCRIPTS, "export_dashboard.py")
        if not os.path.exists(script):
            print(f"export_dashboard.py not found at {script} — skipping.")
            return
        r = subprocess.run(["python3", script], cwd=BASE, capture_output=True, text=True)
        print(r.stdout)
        if r.returncode != 0:
            raise Exception(f"Export failed: {r.stderr}")

    t_export = PythonOperator(task_id="export_dashboard", python_callable=export_dashboard)

    # ── 11. SUMMARY — always runs ─────────────────────────────────────────────
    def summary(**ctx):
        con = duckdb.connect(DB, read_only=True)
        try:
            r = con.execute("""
                SELECT round(sum(total_volume_gbp),2),
                       round(avg(completion_rate)*100,1),
                       round(avg(fraud_rate)*100,2),
                       max(month)
                FROM main_marts_payments.fct_monthly_payment_performance
            """).fetchone()
            res   = ctx["ti"].xcom_pull(task_ids="dbt_tests", key="test_results") or {}
            rawc  = ctx["ti"].xcom_pull(task_ids="health_check", key="raw_counts") or {}
            print("\n" + "="*55)
            print("PIPELINE SUMMARY")
            print("="*55)
            print(f"  Raw transactions : {rawc.get('transactions','?'):,}")
            print(f"  Total volume     : £{r[0]:,.2f}")
            print(f"  Completion rate  : {r[1]}%")
            print(f"  Fraud rate       : {r[2]}%")
            print(f"  Latest month     : {r[3]}")
            print(f"  Tests passed     : {res.get('passed','?')}")
            print(f"  Tests failed     : {res.get('failed','?')}")
            print("="*55)
            if r[2] and float(r[2]) > 5:
                print(f"⚠ ANOMALY: Fraud rate {r[2]}% exceeds 5% threshold!")
        finally:
            con.close()

    t_summary = PythonOperator(
        task_id="pipeline_summary",
        python_callable=summary,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # ── DEPENDENCY GRAPH ──────────────────────────────────────────────────────
    t_health >> t_validate >> t_staging >> t_intermediate
    t_intermediate >> [t_payments, t_customers, t_risk]
    [t_payments, t_customers, t_risk] >> t_tests
    t_tests >> t_branch
    t_branch >> [t_export, t_skip]
    [t_export, t_skip] >> t_summary
