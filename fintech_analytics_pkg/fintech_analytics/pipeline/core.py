"""
fintech_analytics.pipeline.core
=================================
The Pipeline class — main entry point for the library.

Usage:
    from fintech_analytics import Pipeline

    # From CSV
    p = Pipeline.from_csv("transactions.csv")

    # From Kaggle dataset
    p = Pipeline.from_kaggle("mlg-ulb/creditcardfraud")

    # From DataFrame
    p = Pipeline.from_dataframe(df)

    # Run the pipeline
    p.run()                # auto: tries dbt, falls back to native
    p.run(engine="dbt")    # always use real dbt models
    p.run(engine="native") # always use built-in Python SQL

    # Access analytics
    print(p.metrics)
    print(p.segment.rfm())
    print(p.fraud.detect())
    print(p.cohorts.retention())
    print(p.merchants.risk_scorecard())
    print(p.compliance.aml_flags())

    # Open dashboard
    p.dashboard()

    # Export
    p.export("results/")
"""

from __future__ import annotations

import os
import tempfile
import uuid
import webbrowser
from pathlib import Path
from typing import Optional, Union

import duckdb
import pandas as pd
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeElapsedColumn

console = Console()


class Pipeline:
    """
    The main fintech_analytics pipeline.

    Takes raw financial transaction data from any source,
    auto-detects the schema, normalises it, runs analytics,
    and serves ML predictions — all in one object.
    """

    def __init__(
        self,
        df: pd.DataFrame,
        schema_mapping: Optional[dict] = None,
        db_path: Optional[str] = None,
        currency: str = "USD",
        verbose: bool = True,
        engine: str = "auto",
    ):
        """
        Args:
            df:             Raw transaction DataFrame
            schema_mapping: Manual column mapping override {canonical: user_col}
            db_path:        Path to DuckDB file. None = in-memory.
            currency:       Base currency for normalisation
            verbose:        Print progress and reports
            engine:         "auto" = use dbt if installed, else built-in Python
                            "dbt"  = always use bundled dbt SQL models
                            "python" = always use built-in Python engine
        """
        self._raw_df      = df
        self._db_path     = db_path or ":memory:"
        self._currency    = currency
        self._verbose     = verbose
        self._engine      = engine
        self._con:        Optional[duckdb.DuckDBPyConnection] = None
        self._normalised: Optional[pd.DataFrame] = None
        self._mapping     = None
        self._is_run      = False
        self._run_id      = str(uuid.uuid4())[:8]
        self._dbt_result: Optional[dict] = None

        # Analytics accessors — lazy-loaded
        self._segment    = None
        self._fraud      = None
        self._cohorts    = None
        self._merchants  = None
        self._compliance = None
        self._metrics    = None

        # dbt engine (set when engine="dbt" is used)
        self._dbt_engine  = None

        # Apply schema detection
        self._detect_schema(schema_mapping)

    # ── CONSTRUCTORS ──────────────────────────────────────────────────────────

    @classmethod
    def from_csv(
        cls,
        path: Union[str, Path],
        schema_mapping: Optional[dict] = None,
        db_path: Optional[str] = None,
        currency: str = "USD",
        verbose: bool = True,
        engine: str = "auto",
        **pandas_kwargs,
    ) -> "Pipeline":
        """
        Load from a CSV file.

        Args:
            path:           Path to CSV file
            schema_mapping: Manual overrides {canonical: user_col}
            db_path:        DuckDB path (None = in-memory)
            currency:       Base currency
            verbose:        Print progress
            engine:         "auto" | "dbt" | "python"
            **pandas_kwargs: Passed to pd.read_csv()

        Example:
            p = Pipeline.from_csv("transactions.csv")
            p = Pipeline.from_csv("transactions.csv", engine="dbt")
        """
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"CSV not found: {path}")

        if verbose:
            console.print(f"[cyan]Loading[/cyan] {path.name}...")

        df = pd.read_csv(path, **pandas_kwargs)

        if verbose:
            console.print(f"[green]✓[/green] Loaded {len(df):,} rows × {len(df.columns)} columns")

        return cls(df, schema_mapping, db_path, currency, verbose, engine)

    @classmethod
    def from_dataframe(
        cls,
        df: pd.DataFrame,
        schema_mapping: Optional[dict] = None,
        db_path: Optional[str] = None,
        currency: str = "USD",
        verbose: bool = True,
        engine: str = "auto",
    ) -> "Pipeline":
        """Load from an existing pandas DataFrame."""
        return cls(df.copy(), schema_mapping, db_path, currency, verbose, engine)

    @classmethod
    def from_kaggle(
        cls,
        dataset: str,
        filename: Optional[str] = None,
        schema_mapping: Optional[dict] = None,
        db_path: Optional[str] = None,
        currency: str = "USD",
        verbose: bool = True,
        kaggle_username: Optional[str] = None,
        kaggle_key: Optional[str] = None,
    ) -> "Pipeline":
        """
        Download and load a Kaggle dataset.

        Built-in presets — no schema_mapping needed for popular datasets:
            mlg-ulb/creditcardfraud
            kartik2112/fraud-detection
            ybifoundation/credit-card-fraud-detection-prediction

        Authentication (pick any one):
            1. Pass directly:
               Pipeline.from_kaggle(dataset, kaggle_username="u", kaggle_key="key")
            2. Environment variables:
               export KAGGLE_USERNAME=your_username
               export KAGGLE_KEY=your_api_key
            3. File: ~/.kaggle/kaggle.json
               {"username": "your_username", "key": "your_api_key"}

            Get your API key at: https://www.kaggle.com/settings/account

        Args:
            dataset:          "owner/dataset-name" from the Kaggle URL
            filename:         Specific CSV if dataset has multiple. None = largest CSV.
            kaggle_username:  Kaggle username (or set KAGGLE_USERNAME env var)
            kaggle_key:       Kaggle API key  (or set KAGGLE_KEY env var)
        """
        import os
        from fintech_analytics.schema.detector import KAGGLE_PRESETS

        # ── Apply built-in preset if available ────────────────────────────────
        if schema_mapping is None and dataset in KAGGLE_PRESETS:
            schema_mapping = KAGGLE_PRESETS[dataset]
            if verbose:
                console.print(f"[dim]  Using built-in preset for {dataset}[/dim]")

        # ── Inject credentials into environment ───────────────────────────────
        if kaggle_username:
            os.environ["KAGGLE_USERNAME"] = kaggle_username
        if kaggle_key:
            os.environ["KAGGLE_KEY"] = kaggle_key

        # ── Check credentials exist ───────────────────────────────────────────
        has_json = Path.home().joinpath(".kaggle", "kaggle.json").exists()
        has_env  = bool(
            os.environ.get("KAGGLE_USERNAME") and os.environ.get("KAGGLE_KEY")
        )

        if not has_json and not has_env:
            raise RuntimeError(
                "Kaggle credentials not found.\n\n"
                "Option 1 — Pass directly to from_kaggle():\n"
                "  Pipeline.from_kaggle(\n"
                "      \'mlg-ulb/creditcardfraud\',\n"
                "      kaggle_username=\'your_username\',\n"
                "      kaggle_key=\'your_api_key\',\n"
                "  )\n\n"
                "Option 2 — Environment variables:\n"
                "  export KAGGLE_USERNAME=your_username\n"
                "  export KAGGLE_KEY=your_api_key\n\n"
                "Option 3 — Create ~/.kaggle/kaggle.json:\n"
                "  {\'username\': \'your_username\', \'key\': \'your_api_key\'}\n\n"
                "Get your API key: https://www.kaggle.com/settings/account"
            )

        # ── Import kaggle package ─────────────────────────────────────────────
        try:
            import kaggle
        except ImportError:
            raise ImportError(
                "kaggle package not installed.\n"
                "Run: pip install fintech-analytics[kaggle]"
            )

        # ── Authenticate ──────────────────────────────────────────────────────
        try:
            kaggle.api.authenticate()
        except Exception as e:
            raise RuntimeError(
                f"Kaggle authentication failed: {e}\n"
                "Check credentials at https://www.kaggle.com/settings/account"
            )

        if verbose:
            console.print(f"[cyan]Downloading[/cyan] {dataset} from Kaggle...")

        # ── Download and read ─────────────────────────────────────────────────
        with tempfile.TemporaryDirectory() as tmpdir:
            try:
                kaggle.api.dataset_download_files(dataset, path=tmpdir, unzip=True)
            except Exception as e:
                raise RuntimeError(
                    f"Download failed for \'{dataset}\': {e}\n"
                    "Check the dataset name matches the Kaggle URL."
                )

            csv_files = list(Path(tmpdir).glob("**/*.csv"))
            if not csv_files:
                raise ValueError(f"No CSV files found in dataset \'{dataset}\'")

            if filename:
                target = next((f for f in csv_files if f.name == filename), None)
                if not target:
                    names = [f.name for f in csv_files]
                    raise ValueError(
                        f"File \'{filename}\' not found. Available: {names}"
                    )
            else:
                # Use largest CSV — it's almost always the main data file
                target = max(csv_files, key=lambda f: f.stat().st_size)
                if verbose and len(csv_files) > 1:
                    others = [f.name for f in csv_files if f != target]
                    console.print(
                        f"[dim]  Multiple CSVs — using largest: {target.name}[/dim]\n"
                        f"[dim]  Others: {others}[/dim]"
                    )

            df = pd.read_csv(target)
            if verbose:
                console.print(
                    f"[green]✓[/green] {len(df):,} rows × {len(df.columns)} columns"
                )

        return cls(df, schema_mapping, db_path, currency, verbose)


    # ── SCHEMA DETECTION ──────────────────────────────────────────────────────

    def _detect_schema(self, overrides: Optional[dict]):
        """Run schema detection and normalise the DataFrame."""
        from fintech_analytics.schema.detector import SchemaDetector

        detector = SchemaDetector()
        mapping  = detector.detect(self._raw_df)

        if overrides:
            mapping = mapping.override(overrides)

        valid, missing = mapping.is_valid()

        if self._verbose:
            detector.print_report(mapping)

        # Only hard-fail if amount is missing — everything else is auto-generated
        if "amount" not in mapping.mapping:
            cols = list(self._raw_df.columns)
            raise ValueError(
                f"Cannot detect amount column in {cols}. "
                "Specify it: Pipeline.from_csv(f, schema_mapping={'amount': 'your_col'})"
            )

        self._mapping    = mapping
        self._normalised = mapping.normalise(self._raw_df)

        if self._verbose:
            console.print(
                f"[green]✓[/green] Schema detected — "
                f"{len(self._normalised):,} clean transactions ready\n"
            )

    # ── RUN PIPELINE ──────────────────────────────────────────────────────────

    def run(self, engine: Optional[str] = None) -> "Pipeline":
        """
        Run the full analytics pipeline.

        Args:
            engine: Override engine for this run.
                    "auto"   = use dbt if installed, else built-in (default)
                    "dbt"    = use bundled dbt SQL models (.sql files ship in package)
                    "python" = use built-in Python/SQL engine

        Returns self for chaining.

        Examples:
            p.run()              # auto-detects best engine
            p.run(engine="dbt") # force dbt — uses actual .sql files
        """
        use_engine = engine or self._engine or "auto"

        # Validate engine choice
        valid_engines = {"auto", "dbt", "python", "native"}
        if use_engine not in valid_engines:
            raise ValueError(
                f"Unknown engine: '{use_engine}'. "
                f"Valid options: {sorted(valid_engines)}"
            )

        # Resolve "auto" — use dbt if available, else fall back
        if use_engine == "auto":
            use_engine = "dbt" if self._dbt_available() else "python"
            if self._verbose:
                console.print(
                    f"[dim]  Engine: {use_engine}"
                    f"{'  (dbt not installed — using built-in)' if use_engine == 'python' else '  (bundled dbt SQL models)'}[/dim]"
                )

        self._con = duckdb.connect(self._db_path)

        if use_engine == "dbt":
            self._run_dbt_engine()
        else:
            self._run_python_engine()

        self._is_run = True
        self._metrics = self._build_metrics()

        if self._verbose:
            self._print_summary()

        return self

    def _dbt_available(self) -> bool:
        """Check if dbt-duckdb is installed."""
        try:
            import importlib
            importlib.import_module("dbt.adapters.duckdb")
            return True
        except ImportError:
            return False

    def _run_dbt_engine(self):
        """
        Run using the bundled dbt SQL models.

        DuckDB file locking strategy:
        - core.py opens a connection to write raw data
        - DbtEngine closes that connection before running dbt subprocess
        - dbt subprocess opens the file, builds all models, closes it
        - DbtEngine reopens the connection inside engine._con
        - core.py syncs self._con to the engine's reopened connection
        """
        from fintech_analytics.pipeline.dbt_engine import DbtEngine
        import tempfile, uuid as _uuid

        if self._verbose:
            console.print(
                "\n[bold cyan]Running bundled dbt models[/bold cyan] "
                "(production-grade SQL pipeline)\n"
            )

        # dbt requires a real file path — cannot use :memory:
        if self._db_path == ":memory:":
            self._db_path = os.path.join(
                tempfile.gettempdir(),
                f"fintech_analytics_{_uuid.uuid4().hex}.duckdb"
            )
            # Close in-memory connection — data will be re-loaded by DbtEngine
            self._con.close()
            # Open fresh file-backed connection for DbtEngine to use
            self._con = duckdb.connect(self._db_path)

        # Pass our open connection to DbtEngine.
        # DbtEngine will close it before running the dbt subprocess,
        # then reopen it after dbt finishes and expose it as engine.connection.
        engine = DbtEngine(
            con=self._con,
            db_path=self._db_path,
            normalised_df=self._normalised,
            verbose=self._verbose,
        )
        self._dbt_result = engine.run()

        # Sync back: engine has reopened the connection after dbt finished.
        # self._con was closed by dbt_engine during the subprocess run.
        self._con = engine.connection

        if self._verbose:
            r = self._dbt_result
            console.print(
                f"\n[green]✓[/green] dbt complete: "
                f"{r.get('models_run', 0)} models built | "
                f"tests: {r.get('tests_passed', 0)} passed | "
                f"{r.get('tests_failed', 0)} failed"
            )

        # Bridge dbt mart tables → analytics.* views so all accessors work
        self._bridge_dbt_marts(engine)

        # Fill any analytics views that dbt didn't create (graceful fallback)
        self._run_python_engine(fill_gaps_only=True)

    def _run_python_engine(self, fill_gaps_only: bool = False):
        """
        Run using the built-in Python/SQL engine.
        If fill_gaps_only=True, only create tables that don't already exist.
        """
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TimeElapsedColumn(),
            console=console,
            disable=not self._verbose or fill_gaps_only,
        ) as progress:

            task = progress.add_task("Running pipeline...", total=5)

            if not fill_gaps_only:
                progress.update(task, description="Loading transactions...")
                self._load_to_duckdb()
            progress.advance(task)

            progress.update(task, description="Computing analytics...")
            self._run_analytics_if_missing()
            progress.advance(task)

            progress.update(task, description="Segmenting customers...")
            self._compute_rfm_if_missing()
            progress.advance(task)

            progress.update(task, description="Scoring merchants...")
            self._compute_merchant_analytics_if_missing()
            progress.advance(task)

            progress.update(task, description="Building cohort matrix...")
            self._compute_cohorts_if_missing()
            progress.advance(task)

    def _table_exists(self, table_name: str) -> bool:
        """Check if a table/view already exists in DuckDB."""
        try:
            self._con.execute(f"SELECT 1 FROM {table_name} LIMIT 1")
            return True
        except Exception:
            return False

    def _run_analytics_if_missing(self):
        if not self._table_exists("analytics.payment_performance"):
            self._run_analytics()

    def _compute_rfm_if_missing(self):
        if not self._table_exists("analytics.rfm"):
            self._compute_rfm()

    def _compute_merchant_analytics_if_missing(self):
        if not self._table_exists("analytics.merchant_scorecard"):
            self._compute_merchant_analytics()

    def _compute_cohorts_if_missing(self):
        if not self._table_exists("analytics.cohort_retention"):
            self._compute_cohorts()


    def _bridge_dbt_marts(self, dbt_e):
        """
        Create analytics.* views that point at the real dbt mart tables.

        The analytics accessors (SegmentAccessor, CohortAccessor, etc.) query
        analytics.rfm, analytics.cohort_retention, etc.

        When using the dbt engine, those tables live in:
          main_marts_customers.fct_rfm_segmentation
          main_marts_customers.fct_cohort_retention
          main_marts_payments.fct_monthly_payment_performance
          main_marts_risk.dim_merchant_risk_scorecard

        This method creates analytics.* views aliasing those real dbt tables,
        so all accessors work identically regardless of engine choice.
        """
        self._con.execute("CREATE SCHEMA IF NOT EXISTS analytics")

        # RFM — map dbt column names to what SegmentAccessor expects
        try:
            self._con.execute("""
                CREATE OR REPLACE VIEW analytics.rfm AS
                SELECT
                    customer_id,
                    recency_days,
                    frequency,
                    monetary_gbp          as monetary,
                    last_transaction_date as last_txn_date,
                    r_score,
                    f_score,
                    m_score,
                    rfm_total,
                    rfm_segment           as segment,
                    recommended_action
                FROM main_marts_customers.fct_rfm_segmentation
            """)
        except Exception:
            self._compute_rfm()  # fallback to native

        # Cohort retention
        try:
            self._con.execute("""
                CREATE OR REPLACE VIEW analytics.cohort_retention AS
                SELECT
                    cohort_month,
                    months_since_signup,
                    active_customers,
                    cohort_size,
                    retention_pct
                FROM main_marts_customers.fct_cohort_retention
            """)
        except Exception:
            self._compute_cohorts()

        # Payment performance
        try:
            self._con.execute("""
                CREATE OR REPLACE VIEW analytics.payment_performance AS
                SELECT
                    month,
                    currency,
                    total_transactions,
                    completed_transactions  as completed,
                    declined_transactions   as declined,
                    total_volume_gbp        as total_volume,
                    avg_transaction_gbp     as avg_transaction,
                    fraud_flagged_count     as fraud_count,
                    round(completion_rate * 100, 2) as completion_pct,
                    round(fraud_rate * 100, 3)      as fraud_rate_pct
                FROM main_marts_payments.fct_monthly_payment_performance
            """)
        except Exception:
            self._run_analytics()

        # Merchant scorecard
        try:
            self._con.execute("""
                CREATE OR REPLACE VIEW analytics.merchant_scorecard AS
                SELECT
                    merchant_id,
                    merchant_name,
                    merchant_category,
                    total_transactions,
                    completed_transactions         as completed,
                    total_volume_gbp               as total_volume,
                    fraud_flagged_count            as fraud_count,
                    round(fraud_rate * 100, 2)     as fraud_rate_pct,
                    round(completion_rate * 100, 2) as completion_rate_pct,
                    unique_customers,
                    risk_band
                FROM main_marts_risk.dim_merchant_risk_scorecard
            """)
        except Exception:
            self._compute_merchant_analytics()

    def _load_to_duckdb(self):
        """Load normalised DataFrame into DuckDB."""
        df = self._normalised.copy()
        # Normalise fraud flag column name — dbt models expect is_flagged_fraud
        if "is_flagged_fraud" in df.columns and "is_flagged_fraud" not in df.columns:
            df = df.rename(columns={"is_flagged_fraud": "is_flagged_fraud"})
        if "is_flagged_fraud" not in df.columns:
            df["is_flagged_fraud"] = False
        self._con.execute("CREATE SCHEMA IF NOT EXISTS raw")
        self._con.execute("DROP TABLE IF EXISTS raw.transactions")
        self._con.execute("CREATE TABLE raw.transactions AS SELECT * FROM df")

    def _run_analytics(self):
        """Run core SQL analytics directly in DuckDB."""
        self._con.execute("CREATE SCHEMA IF NOT EXISTS analytics")
        self._con.execute("""
            CREATE OR REPLACE VIEW analytics.payment_performance AS
            SELECT
                date_trunc('month', cast(transaction_at as date)) as month,
                currency,
                count(*)                                           as total_transactions,
                count(case when status='completed' then 1 end)    as completed,
                count(case when status='declined'  then 1 end)    as declined,
                sum(case when status='completed' then amount end)  as total_volume,
                avg(case when status='completed' then amount end)  as avg_transaction,
                count(case when is_flagged_fraud then 1 end)               as fraud_count,
                round(
                    count(case when status='completed' then 1 end)*100.0
                    / nullif(count(*),0), 2)                       as completion_pct,
                round(
                    count(case when is_flagged_fraud then 1 end)*100.0
                    / nullif(count(*),0), 3)                       as fraud_rate_pct
            FROM raw.transactions
            GROUP BY 1, 2
        """)

    def _compute_rfm(self):
        """Compute RFM scores in DuckDB."""
        self._con.execute("""
            CREATE OR REPLACE TABLE analytics.rfm AS
            WITH ref AS (SELECT max(cast(transaction_at as date)) as today FROM raw.transactions),
            raw_rfm AS (
                SELECT
                    customer_id,
                    datediff('day', max(cast(transaction_at as date)), r.today) as recency_days,
                    count(*) as frequency,
                    sum(amount) as monetary,
                    max(cast(transaction_at as date)) as last_txn_date
                FROM raw.transactions t CROSS JOIN ref r
                WHERE status = 'completed'
                GROUP BY customer_id, r.today
            ),
            scored AS (
                SELECT *,
                    6 - ntile(5) OVER (ORDER BY recency_days DESC)  as r_score,
                    ntile(5)     OVER (ORDER BY frequency ASC)       as f_score,
                    ntile(5)     OVER (ORDER BY monetary ASC)        as m_score
                FROM raw_rfm
            )
            SELECT *,
                r_score + f_score + m_score as rfm_total,
                CASE
                    WHEN r_score=5 AND f_score>=4 AND m_score>=4 THEN 'Champions'
                    WHEN f_score>=4 AND m_score>=3               THEN 'Loyal Customers'
                    WHEN r_score>=4 AND f_score>=2 AND f_score<4 THEN 'Potential Loyalists'
                    WHEN r_score>=4 AND f_score<2                THEN 'Recent Customers'
                    WHEN r_score=3  AND f_score>=2               THEN 'Promising'
                    WHEN r_score=3  AND f_score<2 AND m_score>=3 THEN 'Need Attention'
                    WHEN r_score=2  AND f_score<=2               THEN 'About to Sleep'
                    WHEN r_score<=2 AND f_score>=3 AND m_score>=3 THEN 'At Risk'
                    WHEN r_score=1  AND f_score>=4               THEN 'Cannot Lose Them'
                    WHEN r_score<=2 AND f_score<=2 AND m_score<=2 THEN 'Hibernating'
                    WHEN r_score=1  AND f_score=1                THEN 'Lost'
                    ELSE 'Others'
                END as segment,
                CASE
                    WHEN r_score=5 AND f_score>=4 AND m_score>=4 THEN 'Reward them. Ask for reviews. Upsell premium.'
                    WHEN f_score>=4 AND m_score>=3               THEN 'Offer loyalty programme. Upsell higher tier.'
                    WHEN r_score>=4 AND f_score>=2 AND f_score<4 THEN 'Offer membership. Personalised recommendations.'
                    WHEN r_score>=4 AND f_score<2                THEN 'Onboarding support. Build the habit.'
                    WHEN r_score<=2 AND f_score>=3 AND m_score>=3 THEN 'Send personalised email. Win-back campaign.'
                    WHEN r_score=1  AND f_score>=4               THEN 'Win back with renewals. Talk to them.'
                    ELSE 'Monitor and re-engage.'
                END as recommended_action
            FROM scored
        """)

    def _compute_merchant_analytics(self):
        """Compute merchant risk scorecard."""
        self._con.execute("""
            CREATE OR REPLACE TABLE analytics.merchant_scorecard AS
            SELECT
                merchant_id,
                merchant_name,
                merchant_category,
                count(*)                                            as total_transactions,
                count(case when status='completed' then 1 end)     as completed,
                sum(case when status='completed' then amount end)   as total_volume,
                count(case when is_flagged_fraud then 1 end)                as fraud_count,
                round(count(case when is_flagged_fraud then 1 end)*100.0
                      / nullif(count(*), 0), 2)                     as fraud_rate_pct,
                round(count(case when status='completed' then 1 end)*100.0
                      / nullif(count(*), 0), 2)                     as completion_rate_pct,
                count(distinct customer_id)                         as unique_customers,
                CASE
                    WHEN count(case when is_flagged_fraud then 1 end)*1.0
                         / nullif(count(*),0) > 0.05               THEN 'critical'
                    WHEN count(case when is_flagged_fraud then 1 end)*1.0
                         / nullif(count(*),0) > 0.02               THEN 'elevated'
                    ELSE 'standard'
                END as risk_band
            FROM raw.transactions
            GROUP BY 1, 2, 3
            ORDER BY fraud_rate_pct DESC
        """)

    def _compute_cohorts(self):
        """Compute cohort retention matrix."""
        self._con.execute("""
            CREATE OR REPLACE TABLE analytics.cohort_retention AS
            WITH first_txn AS (
                SELECT customer_id,
                    date_trunc('month', min(cast(transaction_at as date))) as cohort_month
                FROM raw.transactions WHERE status='completed'
                GROUP BY customer_id
            ),
            activity AS (
                SELECT t.customer_id, f.cohort_month,
                    date_trunc('month', cast(t.transaction_at as date)) as activity_month,
                    datediff('month', f.cohort_month,
                             date_trunc('month', cast(t.transaction_at as date))) as months_since_signup
                FROM raw.transactions t
                JOIN first_txn f USING (customer_id)
                WHERE t.status = 'completed'
            ),
            cohort_sizes AS (
                SELECT cohort_month, count(distinct customer_id) as cohort_size
                FROM first_txn GROUP BY 1
            ),
            retention AS (
                SELECT cohort_month, months_since_signup,
                    count(distinct customer_id) as active_customers
                FROM activity WHERE months_since_signup >= 0
                GROUP BY 1, 2
            )
            SELECT r.cohort_month, r.months_since_signup,
                r.active_customers, cs.cohort_size,
                round(r.active_customers * 100.0 / nullif(cs.cohort_size, 0), 1) as retention_pct
            FROM retention r JOIN cohort_sizes cs USING (cohort_month)
            ORDER BY cohort_month, months_since_signup
        """)

    def _build_metrics(self) -> dict:
        """Build top-level KPI summary."""
        r = self._con.execute("""
            SELECT
                count(*)                                            as total_transactions,
                count(distinct customer_id)                        as unique_customers,
                count(distinct merchant_id)                        as unique_merchants,
                round(sum(case when status='completed' then amount end), 2) as total_volume,
                round(avg(case when status='completed' then amount end), 2) as avg_transaction,
                round(count(case when status='completed' then 1 end)*100.0
                      / nullif(count(*),0), 1)                     as completion_pct,
                round(count(case when
                    coalesce(
                        try_cast(is_flagged_fraud as boolean),
                        false
                    ) then 1 end)*100.0
                    / nullif(count(*),0), 2)                       as fraud_rate_pct,
                min(cast(transaction_at as date))                  as date_from,
                max(cast(transaction_at as date))                  as date_to
            FROM raw.transactions
        """).fetchone()

        return {
            "total_transactions": int(r[0]) if r[0] else 0,
            "unique_customers":   int(r[1]) if r[1] else 0,
            "unique_merchants":   int(r[2]) if r[2] else 0,
            "total_volume":       float(r[3]) if r[3] else 0.0,
            "avg_transaction":    float(r[4]) if r[4] else 0.0,
            "completion_pct":     float(r[5]) if r[5] else 0.0,
            "fraud_rate_pct":     float(r[6]) if r[6] else 0.0,
            "date_from":          str(r[7]) if r[7] else None,
            "date_to":            str(r[8]) if r[8] else None,
        }

    def _print_summary(self):
        """Print a rich summary after pipeline run."""
        from rich.table import Table
        m = self._metrics
        engine_label = "dbt (bundled SQL models)" if self._dbt_result else "built-in Python engine"
        console.print(f"\n[bold cyan]Pipeline complete[/bold cyan] · {engine_label}\n")
        t = Table(show_header=False, box=None, padding=(0,2))
        t.add_column(style="dim")
        t.add_column(style="bold white")
        t.add_row("Transactions",  f"{m['total_transactions']:,}")
        t.add_row("Customers",     f"{m['unique_customers']:,}")
        t.add_row("Merchants",     f"{m['unique_merchants']:,}")
        t.add_row("Total Volume",  f"${m['total_volume']:,.2f}")
        t.add_row("Completion",    f"{m['completion_pct']}%")
        t.add_row("Fraud Rate",    f"{m['fraud_rate_pct']}%")
        t.add_row("Date Range",    f"{m['date_from']} → {m['date_to']}")
        console.print(t)

    # ── ANALYTICS ACCESSORS ───────────────────────────────────────────────────

    def _check_run(self):
        if not self._is_run:
            raise RuntimeError("Call pipeline.run() before accessing analytics.")

    @property
    def metrics(self) -> dict:
        """Top-level KPI summary dict."""
        self._check_run()
        return self._metrics

    @property
    def segment(self) -> "SegmentAccessor":
        """Customer segmentation accessor."""
        self._check_run()
        if self._segment is None:
            from fintech_analytics.analytics.segmentation import SegmentAccessor
            self._segment = SegmentAccessor(self._con)
        return self._segment

    @property
    def fraud(self) -> "FraudAccessor":
        """Fraud detection accessor."""
        self._check_run()
        if self._fraud is None:
            from fintech_analytics.ml.fraud import FraudAccessor
            self._fraud = FraudAccessor(self._con, self._normalised)
        return self._fraud

    @property
    def cohorts(self) -> "CohortAccessor":
        """Cohort analytics accessor."""
        self._check_run()
        if self._cohorts is None:
            from fintech_analytics.analytics.cohorts import CohortAccessor
            self._cohorts = CohortAccessor(self._con)
        return self._cohorts

    @property
    def merchants(self) -> "MerchantAccessor":
        """Merchant intelligence accessor."""
        self._check_run()
        if self._merchants is None:
            from fintech_analytics.analytics.merchants import MerchantAccessor
            self._merchants = MerchantAccessor(self._con)
        return self._merchants

    @property
    def compliance(self) -> "ComplianceAccessor":
        """Compliance checks accessor."""
        self._check_run()
        if self._compliance is None:
            from fintech_analytics.compliance.checks import ComplianceAccessor
            self._compliance = ComplianceAccessor(self._con, self._normalised)
        return self._compliance

    # ── DASHBOARD ─────────────────────────────────────────────────────────────

    def dashboard(self, port: int = 8888, open_browser: bool = True):
        """
        Serve an interactive analytics dashboard.

        Args:
            port:         Local port to serve on
            open_browser: Automatically open in browser
        """
        self._check_run()
        from fintech_analytics.pipeline.dashboard import serve_dashboard
        serve_dashboard(self._con, self._metrics, port=port, open_browser=open_browser)

    # ── EXPORT ────────────────────────────────────────────────────────────────

    def export(self, output_dir: Union[str, Path] = "fintech_analytics_output"):
        """
        Export all analytics results to CSV files.

        Args:
            output_dir: Directory to write files to
        """
        self._check_run()
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        exports = {
            "metrics.csv":            None,
            "rfm_segments.csv":       "SELECT * FROM analytics.rfm",
            "cohort_retention.csv":   "SELECT * FROM analytics.cohort_retention",
            "merchant_scorecard.csv": "SELECT * FROM analytics.merchant_scorecard",
            "payment_performance.csv":"SELECT * FROM analytics.payment_performance",
        }

        import json
        with open(output_dir / "metrics.json", "w") as f:
            json.dump(self._metrics, f, indent=2, default=str)

        for filename, sql in exports.items():
            if sql:
                df = self._con.execute(sql).df()
                df.to_csv(output_dir / filename, index=False)
                if self._verbose:
                    console.print(f"[green]✓[/green] {filename} ({len(df):,} rows)")

        if self._verbose:
            console.print(f"\n[bold]Exported to:[/bold] {output_dir.absolute()}")

    # ── QUERY ─────────────────────────────────────────────────────────────────

    def query(self, sql: str) -> pd.DataFrame:
        """
        Run a custom SQL query against the pipeline database.

        Tables available:
            raw.transactions           — normalised input data
            analytics.rfm              — RFM segments
            analytics.cohort_retention — retention matrix
            analytics.merchant_scorecard — merchant risk
            analytics.payment_performance — monthly KPIs
        """
        self._check_run()
        return self._con.execute(sql).df()

    @property
    def dbt_result(self) -> Optional[dict]:
        """dbt build results (None if python engine was used)."""
        return self._dbt_result

    def __repr__(self) -> str:
        status = "ready" if self._is_run else "not run — call .run()"
        rows   = len(self._normalised) if self._normalised is not None else 0
        engine = "dbt" if self._dbt_result else "python"
        return f"Pipeline(rows={rows:,}, engine={engine}, status={status})"
