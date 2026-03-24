"""
fintech_analytics.pipeline.dbt_engine
=======================================
DbtEngine — runs the bundled production dbt SQL models on any user data.

This is the core differentiator: the package ships real dbt models
(the same .sql files from github.com/tanvirpasha21/fintech-analytics-platform)
and runs them programmatically on any normalised DataFrame.

How it works:
  1. User provides CSV / DataFrame — schema auto-detected and normalised
  2. DbtEngine copies the bundled dbt project to a temp working directory
  3. Writes a profiles.yml pointing at the user's DuckDB file
  4. Generates stub tables for any missing sources (customers, merchants, etc.)
  5. Runs dbt deps + dbt build — all 13 models, 30 tests
  6. Analytics accessors query the real dbt mart tables

Usage (via Pipeline):
    p = Pipeline.from_csv("transactions.csv")
    p.run(engine="dbt")     # uses real .sql dbt models
    p.run(engine="native")  # uses built-in Python SQL (default, no dbt needed)
    p.run()                 # auto: tries dbt, falls back to native
"""
from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd
from rich.console import Console

console = Console()

# Bundled dbt project — shipped inside this package
DBT_PROJECT_DIR = Path(__file__).parent / "dbt_project"


def dbt_available() -> bool:
    """Return True if dbt is installed and callable."""
    return shutil.which("dbt") is not None


class DbtEngine:
    """
    Runs the bundled dbt models on normalised user data.

    The bundled dbt project at fintech_analytics/pipeline/dbt_project/
    contains the 13 production .sql models from the fintech-analytics-platform
    repo, shipped verbatim so users can inspect, fork, and extend them.
    """

    def __init__(
        self,
        con: duckdb.DuckDBPyConnection,
        db_path: str,
        normalised_df: pd.DataFrame,
        verbose: bool = True,
    ):
        self._con         = con
        self._db_path     = db_path
        self._df          = normalised_df
        self._verbose     = verbose
        self._work_dir:   Optional[Path] = None
        self._prof_dir:   Optional[Path] = None

    # ── PUBLIC API ────────────────────────────────────────────────────────────

    def run(self) -> dict:
        """
        Run the full dbt pipeline end to end.

        Returns:
            dict: {models_run, tests_passed, tests_failed, success}
        """
        self._setup_work_dir()
        self._load_raw_data()
        self._write_profiles()
        return self._run_dbt_build()

    def cleanup(self):
        if self._work_dir and self._work_dir.exists():
            shutil.rmtree(self._work_dir)

    # ── SETUP ─────────────────────────────────────────────────────────────────

    def _setup_work_dir(self):
        """Copy bundled dbt project to a fresh temp directory."""
        self._work_dir = Path(tempfile.mkdtemp(prefix="fintech_dbt_"))
        dest = self._work_dir / "dbt_project"
        shutil.copytree(DBT_PROJECT_DIR, dest)
        self._prof_dir = self._work_dir / "profiles"
        self._prof_dir.mkdir()
        if self._verbose:
            console.print(f"  [dim]Temp dbt project: {dest}[/dim]")

    def _load_raw_data(self):
        """
        Load normalised data into DuckDB as raw.* tables.

        dbt staging models need:
          raw.transactions  ← from user data (required)
          raw.customers     ← stub generated from transaction customer_ids
          raw.merchants     ← stub generated from transaction merchant data
          raw.fx_rates      ← stub with GBP/USD/EUR daily rates
          raw.disputes      ← empty stub (no dispute data in most CSVs)
        """
        self._con.execute("CREATE SCHEMA IF NOT EXISTS raw")
        df = self._df.copy()

        # ── Map column names to what dbt models expect ────────────────────────
        if "is_fraud" in df.columns and "is_flagged_fraud" not in df.columns:
            df = df.rename(columns={"is_fraud": "is_flagged_fraud"})
        if "is_flagged_fraud" not in df.columns:
            df["is_flagged_fraud"] = False
        if "country" in df.columns and "ip_country" not in df.columns:
            df["ip_country"] = df["country"]
        if "ip_country" not in df.columns:
            df["ip_country"] = "GB"
        # Add every column that dbt staging models require
        # so any sparse dataset (including Kaggle) works without errors
        if "status" not in df.columns:
            df["status"] = "completed"
        if "channel" not in df.columns:
            df["channel"] = "card"
        if "merchant_category" not in df.columns:
            df["merchant_category"] = "Other"
        if "merchant_name" not in df.columns:
            df["merchant_name"] = "Unknown Merchant"
        if "currency" not in df.columns:
            df["currency"] = "USD"
        if "customer_id" not in df.columns:
            import uuid as _uuid
            df["customer_id"] = [str(_uuid.uuid4()) for _ in range(len(df))]
        if "merchant_id" not in df.columns:
            import uuid as _uuid2
            df["merchant_id"] = df["merchant_name"].apply(
                lambda x: str(_uuid2.uuid5(_uuid2.NAMESPACE_DNS, str(x)))
            )

        self._con.execute("DROP TABLE IF EXISTS raw.transactions")
        self._con.execute("CREATE TABLE raw.transactions AS SELECT * FROM df")
        if self._verbose:
            console.print(f"  raw.transactions: {len(df):,} rows ✓")

        # ── customers stub ────────────────────────────────────────────────────
        self._con.execute("DROP TABLE IF EXISTS raw.customers")
        if "customer_id" in df.columns:
            cids = df["customer_id"].drop_duplicates()
            ccy  = df["currency"].iloc[0] if "currency" in df.columns else "GBP"
            min_date = pd.to_datetime(df["transaction_at"]).min() if "transaction_at" in df.columns else pd.Timestamp("2020-01-01")
            cust = pd.DataFrame({
                "customer_id":  cids,
                "full_name":    "Customer " + cids.astype(str),
                "email":        cids.astype(str) + "@example.com",
                "phone":        "000-000-0000",
                "country":      "GB",
                "account_tier": "standard",
                "signup_date":  str(min_date.date()),
                "kyc_status":   "verified",
                "date_of_birth":"1990-01-01",
                "currency":     ccy,
            })
        else:
            cust = pd.DataFrame(columns=[
                "customer_id","full_name","email","phone","country",
                "account_tier","signup_date","kyc_status","date_of_birth","currency"
            ])
        self._con.execute("CREATE TABLE raw.customers AS SELECT * FROM cust")
        if self._verbose:
            console.print(f"  raw.customers:    {len(cust):,} rows (stub) ✓")

        # ── merchants stub ────────────────────────────────────────────────────
        self._con.execute("DROP TABLE IF EXISTS raw.merchants")
        if "merchant_id" in df.columns:
            by_mid = df.groupby("merchant_id").first().reset_index()
            merc = pd.DataFrame({
                "merchant_id":   by_mid["merchant_id"],
                "merchant_name": by_mid["merchant_name"] if "merchant_name" in by_mid else "Merchant " + by_mid["merchant_id"].astype(str),
                "category":      by_mid["merchant_category"] if "merchant_category" in by_mid else "Other",
                "mcc_code":      "5999",
                "country":       "GB",
                "is_high_risk":  False,
                "onboarded_date":"2020-01-01",
            })
        else:
            merc = pd.DataFrame(columns=[
                "merchant_id","merchant_name","category","mcc_code",
                "country","is_high_risk","onboarded_date"
            ])
        self._con.execute("CREATE TABLE raw.merchants AS SELECT * FROM merc")
        if self._verbose:
            console.print(f"  raw.merchants:    {len(merc):,} rows (stub) ✓")

        # ── fx_rates stub ─────────────────────────────────────────────────────
        self._con.execute("DROP TABLE IF EXISTS raw.fx_rates")
        currencies = list(set(df["currency"].dropna().tolist())) if "currency" in df.columns else ["GBP"]
        fx_base    = {"GBP":1.0,"USD":1.27,"EUR":1.17,"SGD":1.70,"AED":4.66,"CAD":1.73}

        if "transaction_at" in df.columns:
            min_d = pd.to_datetime(df["transaction_at"]).min().date()
            max_d = pd.to_datetime(df["transaction_at"]).max().date()
        else:
            from datetime import date
            min_d, max_d = date(2023,1,1), date(2024,12,31)

        dates = pd.date_range(min_d, max_d, freq="D")
        fx_rows = [
            {"rate_date": d.strftime("%Y-%m-%d"), "currency": c, "rate_to_gbp": fx_base.get(c, 1.0)}
            for d in dates for c in set(currencies) | {"GBP","USD","EUR"}
        ]
        fx_df = pd.DataFrame(fx_rows)
        self._con.execute("CREATE TABLE raw.fx_rates AS SELECT * FROM fx_df")
        if self._verbose:
            console.print(f"  raw.fx_rates:     {len(fx_df):,} rows (stub) ✓")

        # ── disputes stub ─────────────────────────────────────────────────────
        self._con.execute("DROP TABLE IF EXISTS raw.disputes")
        self._con.execute("""
            CREATE TABLE raw.disputes (
                dispute_id     VARCHAR,
                transaction_id VARCHAR,
                customer_id    VARCHAR,
                opened_at      VARCHAR,
                reason         VARCHAR,
                status         VARCHAR,
                amount         DOUBLE,
                currency       VARCHAR
            )
        """)
        if self._verbose:
            console.print(f"  raw.disputes:     0 rows (stub) ✓")

    def _write_profiles(self):
        """
        Write profiles.yml with profile name matching dbt_project.yml.

        Written to two locations:
          1. self._prof_dir/profiles.yml   — passed via --profiles-dir
          2. dbt_project/profiles.yml      — dbt fallback lookup location
        Both must match the profile name in dbt_project.yml: 'fintech_platform'
        """
        abs_path = os.path.abspath(self._db_path) if self._db_path != ":memory:" else ":memory:"
        prof_content = f"""fintech_platform:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: "{abs_path}"
      threads: 4
"""
        # Write to the dedicated profiles directory (passed via --profiles-dir)
        (self._prof_dir / "profiles.yml").write_text(prof_content)

        # Also write directly into the dbt project dir as a fallback
        # dbt always checks the project directory for profiles.yml
        dbt_project_dir = self._work_dir / "dbt_project"
        (dbt_project_dir / "profiles.yml").write_text(prof_content)

        if self._verbose:
            console.print(f"  [dim]profiles.yml (fintech_platform) → {abs_path}[/dim]")

    def _run_dbt_build(self) -> dict:
        """
        Run dbt deps then dbt build.

        Critical: DuckDB only allows one writer at a time.
        We must close the Python connection before dbt subprocess opens
        the same file, then reopen it after dbt finishes.
        """
        dbt_dir = str(self._work_dir / "dbt_project")
        # Use the dbt project dir itself as --profiles-dir
        # profiles.yml is written there by _write_profiles() so dbt always finds it
        prof = dbt_dir

        # ── CLOSE Python connection so dbt can acquire the lock ───────────────
        if self._db_path != ":memory:":
            self._con.close()

        try:
            # dbt deps
            subprocess.run(
                ["dbt", "deps", "--profiles-dir", prof],
                cwd=dbt_dir, capture_output=True, text=True,
            )

            # dbt build
            if self._verbose:
                console.print("  [cyan]Running dbt build (13 models + 30 tests)...[/cyan]")

            proc = subprocess.run(
                ["dbt", "build", "--profiles-dir", prof],
                cwd=dbt_dir, capture_output=True, text=True,
            )

        finally:
            # ── REOPEN connection after dbt releases the lock ─────────────────
            if self._db_path != ":memory:":
                self._con = duckdb.connect(self._db_path)

        stdout = proc.stdout
        lines  = stdout.splitlines()

        # Print summary lines
        if self._verbose:
            for line in lines[-10:]:
                if line.strip():
                    console.print(f"  [dim]{line}[/dim]")

        passed = sum(1 for l in lines if "PASS" in l)
        failed = sum(1 for l in lines if "FAIL" in l or "ERROR" in l)

        if proc.returncode != 0:
            err_lines = [l for l in lines if "Error" in l or "FAIL" in l][:8]
            raise RuntimeError(
                "dbt build failed.\n"
                + "\n".join(f"  {l}" for l in err_lines)
                + f"\n\nFull output (last 2000 chars):\n{stdout[-2000:]}"
            )

        return {
            "models_run":    sum(1 for l in lines if "OK created" in l or "OK" in l),
            "tests_passed":  passed,
            "tests_failed":  failed,
            "success":       True,
        }

    # ── MART QUERIES ─────────────────────────────────────────────────────────

    def query_rfm(self) -> pd.DataFrame:
        return self._con.execute(
            "SELECT * FROM main_marts_customers.fct_rfm_segmentation"
        ).df()

    def query_payment_performance(self) -> pd.DataFrame:
        return self._con.execute(
            "SELECT * FROM main_marts_payments.fct_monthly_payment_performance"
        ).df()

    def query_cohort_retention(self) -> pd.DataFrame:
        return self._con.execute(
            "SELECT * FROM main_marts_customers.fct_cohort_retention"
        ).df()

    def query_merchant_scorecard(self) -> pd.DataFrame:
        return self._con.execute(
            "SELECT * FROM main_marts_risk.dim_merchant_risk_scorecard"
        ).df()

    def query_customers_360(self) -> pd.DataFrame:
        return self._con.execute(
            "SELECT * FROM main_marts_customers.dim_customers_360"
        ).df()

    def query_fraud_events(self) -> pd.DataFrame:
        return self._con.execute(
            "SELECT * FROM main_marts_risk.fct_fraud_events"
        ).df()

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        """Return the current DuckDB connection (may be reopened after dbt run)."""
        return self._con
