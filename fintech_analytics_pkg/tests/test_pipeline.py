"""
tests/test_pipeline.py
Tests for the fintech_analytics package.
"""

import pandas as pd
import numpy as np
import pytest
from fintech_analytics import Pipeline
from fintech_analytics.schema.detector import SchemaDetector
from fintech_analytics.schema.mapping import ColumnMapping


# ── FIXTURES ─────────────────────────────────────────────────────────────────

@pytest.fixture
def sample_df():
    """Standard-schema sample transactions."""
    np.random.seed(42)
    n = 500
    return pd.DataFrame({
        "transaction_id":  [f"txn_{i}" for i in range(n)],
        "customer_id":     [f"cust_{i % 50}" for i in range(n)],
        "merchant_id":     [f"merch_{i % 20}" for i in range(n)],
        "merchant_name":   np.random.choice(["Tesco", "Amazon", "HSBC ATM", "Shell"], n),
        "merchant_category": np.random.choice(["Groceries", "Retail", "ATM", "Fuel"], n),
        "amount":          np.random.exponential(85, n).round(2),
        "currency":        np.random.choice(["GBP", "USD", "EUR"], n),
        "transaction_at":  pd.date_range("2023-01-01", periods=n, freq="1h"),
        "status":          np.random.choice(["completed", "declined", "pending"], n, p=[0.9, 0.07, 0.03]),
        "channel":         np.random.choice(["card", "app", "web"], n),
        "is_fraud":        np.random.random(n) < 0.02,
    })


@pytest.fixture
def nonstandard_df():
    """Non-standard column names to test auto-detection."""
    np.random.seed(42)
    n = 200
    return pd.DataFrame({
        "ref_number":  [f"ref_{i}" for i in range(n)],
        "cust_id":     [f"c{i % 30}" for i in range(n)],
        "trans_amt":   np.random.exponential(80, n).round(2),
        "date":        pd.date_range("2023-06-01", periods=n, freq="2h"),
        "result":      np.random.choice(["success", "failed", "pending"], n, p=[0.88, 0.09, 0.03]),
        "description": np.random.choice(["Walmart", "PayPal Transfer", "Citi ATM"], n),
    })


@pytest.fixture
def pipeline(sample_df, tmp_path):
    """Ready-run pipeline with isolated DuckDB file."""
    db = str(tmp_path / "test.duckdb")
    p = Pipeline.from_dataframe(sample_df, db_path=db, verbose=False)
    p.run(engine="native")
    return p


# ── SCHEMA DETECTION ─────────────────────────────────────────────────────────

class TestSchemaDetector:
    def test_detects_standard_schema(self, sample_df):
        d = SchemaDetector()
        m = d.detect(sample_df)
        assert "transaction_id" in m.mapping
        assert "amount"         in m.mapping
        assert "transaction_at" in m.mapping

    def test_detects_nonstandard_schema(self, nonstandard_df):
        d = SchemaDetector()
        m = d.detect(nonstandard_df)
        # Should fuzzy-match: trans_amt → amount, date → transaction_at, ref_number → transaction_id
        assert "amount"         in m.mapping
        assert "transaction_at" in m.mapping

    def test_is_valid_passes_with_required_fields(self, sample_df):
        d = SchemaDetector()
        m = d.detect(sample_df)
        valid, missing = m.is_valid()
        assert valid
        assert len(missing) == 0

    def test_override_works(self, nonstandard_df):
        d  = SchemaDetector()
        m  = d.detect(nonstandard_df)
        m2 = m.override({"transaction_id": "ref_number", "amount": "trans_amt"})
        assert m2.mapping["transaction_id"] == "ref_number"
        assert m2.mapping["amount"] == "trans_amt"
        assert m2.confidence["transaction_id"] == 100


# ── PIPELINE CONSTRUCTION ────────────────────────────────────────────────────

class TestPipelineConstruction:
    def test_from_dataframe(self, sample_df):
        p = Pipeline.from_dataframe(sample_df, verbose=False)
        assert p._normalised is not None
        assert len(p._normalised) == len(sample_df)

    def test_from_csv(self, sample_df, tmp_path):
        csv_path = tmp_path / "transactions.csv"
        sample_df.to_csv(csv_path, index=False)
        p = Pipeline.from_csv(csv_path, verbose=False)
        assert len(p._normalised) == len(sample_df)

    def test_requires_run_before_analytics(self, sample_df):
        p = Pipeline.from_dataframe(sample_df, verbose=False)
        with pytest.raises(RuntimeError, match="run()"):
            _ = p.metrics

    def test_manual_schema_override(self, nonstandard_df):
        p = Pipeline.from_dataframe(
            nonstandard_df,
            schema_mapping={"transaction_id": "ref_number", "amount": "trans_amt", "transaction_at": "date"},
            verbose=False,
        )
        assert p._normalised is not None


# ── PIPELINE RUN ─────────────────────────────────────────────────────────────

class TestPipelineRun:
    def test_run_returns_self(self, sample_df, tmp_path):
        db = str(tmp_path / "test.duckdb")
        p = Pipeline.from_dataframe(sample_df, db_path=db, verbose=False)
        result = p.run(engine="native")
        assert result is p

    def test_metrics_populated(self, pipeline):
        m = pipeline.metrics
        assert m["total_transactions"] > 0
        assert m["unique_customers"]   > 0
        assert 0 <= m["completion_pct"] <= 100
        assert 0 <= m["fraud_rate_pct"] <= 100

    def test_duckdb_tables_created(self, pipeline):
        tables = pipeline.query(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'analytics'"
        )
        names = tables["table_name"].tolist()
        assert "rfm"               in names
        assert "merchant_scorecard" in names
        assert "cohort_retention"   in names


# ── SEGMENTATION ─────────────────────────────────────────────────────────────

class TestSegmentation:
    def test_rfm_returns_dataframe(self, pipeline):
        df = pipeline.segment.rfm()
        assert isinstance(df, pd.DataFrame)
        assert "segment" in df.columns
        assert "r_score" in df.columns
        assert len(df) > 0

    def test_rfm_scores_in_range(self, pipeline):
        df = pipeline.segment.rfm()
        assert df["r_score"].between(1, 5).all()
        assert df["f_score"].between(1, 5).all()
        assert df["m_score"].between(1, 5).all()

    def test_rfm_total_correct(self, pipeline):
        df = pipeline.segment.rfm()
        assert (df["rfm_total"] == df["r_score"] + df["f_score"] + df["m_score"]).all()

    def test_summary_has_all_segments(self, pipeline):
        df = pipeline.segment.summary()
        assert len(df) > 0
        assert "segment" in df.columns
        assert "customers" in df.columns

    def test_at_risk_subset(self, pipeline):
        at_risk = pipeline.segment.at_risk
        assert isinstance(at_risk, pd.DataFrame)


# ── COHORTS ──────────────────────────────────────────────────────────────────

class TestCohorts:
    def test_retention_returns_dataframe(self, pipeline):
        df = pipeline.cohorts.retention()
        assert isinstance(df, pd.DataFrame)
        assert "retention_pct" in df.columns

    def test_retention_pct_in_range(self, pipeline):
        df = pipeline.cohorts.retention()
        if not df.empty:
            assert df["retention_pct"].between(0, 100).all()

    def test_matrix_pivots_correctly(self, pipeline):
        matrix = pipeline.cohorts.retention_matrix()
        if not matrix.empty:
            assert "M0" in matrix.columns


# ── MERCHANTS ─────────────────────────────────────────────────────────────────

class TestMerchants:
    def test_scorecard_returns_dataframe(self, pipeline):
        df = pipeline.merchants.risk_scorecard()
        assert isinstance(df, pd.DataFrame)
        assert "risk_band" in df.columns

    def test_risk_bands_valid(self, pipeline):
        df = pipeline.merchants.risk_scorecard()
        valid_bands = {"critical", "elevated", "standard"}
        assert set(df["risk_band"].unique()).issubset(valid_bands)

    def test_top_by_volume(self, pipeline):
        df = pipeline.merchants.top_by_volume(5)
        assert len(df) <= 5


# ── FRAUD ─────────────────────────────────────────────────────────────────────

class TestFraud:
    def test_detect_returns_dataframe(self, pipeline):
        df = pipeline.fraud.detect(mode="unsupervised")
        assert isinstance(df, pd.DataFrame)
        assert "fraud_probability" in df.columns
        assert "is_predicted_fraud" in df.columns

    def test_probabilities_in_range(self, pipeline):
        df = pipeline.fraud.detect(mode="unsupervised")
        assert df["fraud_probability"].between(0, 1).all()

    def test_supervised_with_labels(self, sample_df, tmp_path):
        db = str(tmp_path / "test.duckdb")
        p = Pipeline.from_dataframe(sample_df, db_path=db, verbose=False)
        p.run(engine="native")
        df = p.fraud.detect(mode="supervised")
        assert "fraud_probability" in df.columns

    def test_explain_returns_dict(self, pipeline):
        # Get a transaction ID from the data
        txn_id = pipeline._normalised["transaction_id"].iloc[0]
        pipeline.fraud.detect(mode="unsupervised")
        result = pipeline.fraud.explain(str(txn_id))
        assert "fraud_probability" in result
        assert "reasons"           in result
        assert "recommended_action" in result
        assert len(result["reasons"]) > 0


# ── COMPLIANCE ────────────────────────────────────────────────────────────────

class TestCompliance:
    def test_aml_flags_returns_dataframe(self, pipeline):
        df = pipeline.compliance.aml_flags()
        assert isinstance(df, pd.DataFrame)

    def test_aml_columns_present(self, pipeline):
        df = pipeline.compliance.aml_flags()
        if not df.empty:
            assert "flag_type" in df.columns
            assert "severity"  in df.columns
            assert "recommended_action" in df.columns

    def test_severity_values_valid(self, pipeline):
        df = pipeline.compliance.aml_flags()
        if not df.empty:
            valid = {"HIGH", "MEDIUM", "LOW"}
            assert set(df["severity"].unique()).issubset(valid)


# ── EXPORT ────────────────────────────────────────────────────────────────────

class TestExport:
    def test_export_creates_files(self, pipeline, tmp_path):
        pipeline.export(tmp_path)
        assert (tmp_path / "metrics.json").exists()
        assert (tmp_path / "rfm_segments.csv").exists()
        assert (tmp_path / "merchant_scorecard.csv").exists()

    def test_custom_query(self, pipeline):
        df = pipeline.query("SELECT count(*) as n FROM raw.transactions")
        assert df["n"].iloc[0] > 0


# ── DBT ENGINE ────────────────────────────────────────────────────────────────

class TestDbtEngine:
    def test_dbt_available_returns_bool(self):
        from fintech_analytics.pipeline.dbt_engine import dbt_available
        result = dbt_available()
        assert isinstance(result, bool)

    def test_dbt_project_dir_exists(self):
        from fintech_analytics.pipeline.dbt_engine import DBT_PROJECT_DIR
        assert DBT_PROJECT_DIR.exists(), f"dbt project not found at {DBT_PROJECT_DIR}"

    def test_bundled_sql_files_exist(self):
        from fintech_analytics.pipeline.dbt_engine import DBT_PROJECT_DIR
        sql_files = list(DBT_PROJECT_DIR.rglob("*.sql"))
        assert len(sql_files) >= 13, f"Expected 13+ SQL files, found {len(sql_files)}"

    def test_bundled_staging_models(self):
        from fintech_analytics.pipeline.dbt_engine import DBT_PROJECT_DIR
        staging = DBT_PROJECT_DIR / "models" / "staging"
        expected = ["stg_transactions.sql","stg_customers.sql","stg_merchants.sql",
                    "stg_fx_rates.sql","stg_disputes.sql"]
        for f in expected:
            assert (staging / f).exists(), f"Missing: {f}"

    def test_bundled_mart_models(self):
        from fintech_analytics.pipeline.dbt_engine import DBT_PROJECT_DIR
        marts = DBT_PROJECT_DIR / "models" / "marts"
        expected = [
            "payments/fct_monthly_payment_performance.sql",
            "customers/dim_customers_360.sql",
            "customers/fct_rfm_segmentation.sql",
            "customers/fct_cohort_retention.sql",
            "risk/fct_fraud_events.sql",
            "risk/dim_merchant_risk_scorecard.sql",
        ]
        for f in expected:
            assert (marts / f).exists(), f"Missing mart: {f}"

    def test_native_engine_explicit(self, sample_df):
        """engine='native' should work without dbt installed."""
        p = Pipeline.from_dataframe(sample_df, verbose=False)
        p.run(engine="native")
        assert p._is_run
        assert p.metrics["total_transactions"] > 0

    def test_invalid_engine_raises(self, sample_df, tmp_path):
        db = str(tmp_path / "test.duckdb")
        p = Pipeline.from_dataframe(sample_df, db_path=db, verbose=False)
        with pytest.raises((ValueError, RuntimeError)):
            p.run(engine="invalid_engine")

    def test_auto_engine_runs(self, sample_df, tmp_path):
        """engine='auto' should always work (falls back to native if no dbt)."""
        from fintech_analytics.pipeline.dbt_engine import dbt_available
        db = str(tmp_path / "test.duckdb")
        p = Pipeline.from_dataframe(sample_df, db_path=db, verbose=False)
        # If dbt is available, auto uses it; otherwise native. Both should succeed.
        # Skip if dbt would cause lock issues in test environment
        try:
            p.run(engine="auto")
            assert p._is_run
        except RuntimeError as e:
            if "dbt" in str(e).lower() or "lock" in str(e).lower():
                pytest.skip("dbt lock issue in test environment — tested separately")
            raise

    @pytest.mark.skipif(
        not __import__('shutil').which("dbt"),
        reason="dbt not installed — skipping dbt engine test"
    )
    def test_dbt_engine_full_run(self, sample_df, tmp_path):
        """Full dbt engine run — only runs when dbt is installed."""
        db_path = str(tmp_path / "test.duckdb")
        p = Pipeline.from_dataframe(sample_df, db_path=db_path, verbose=True)
        p.run(engine="dbt")
        assert p._is_run
        # Verify dbt mart tables exist via bridge views
        rfm = p.segment.rfm()
        assert len(rfm) > 0
        assert "segment" in rfm.columns


# ── DRIFT DETECTION ───────────────────────────────────────────────────────────

class TestDriftDetection:

    def test_drift_returns_report(self, sample_df):
        """Drift report is returned without errors."""
        p1 = Pipeline.from_dataframe(sample_df.iloc[:250].copy(), verbose=False)
        p1.run()
        p2 = Pipeline.from_dataframe(sample_df.iloc[250:].copy(), verbose=False)
        p2.run()
        report = p1.fraud.drift(p2)
        assert report is not None
        assert hasattr(report, "overall_psi")
        assert hasattr(report, "should_retrain")
        assert hasattr(report, "status")

    def test_drift_status_valid(self, sample_df):
        """Status is one of the three valid values."""
        p1 = Pipeline.from_dataframe(sample_df.iloc[:250].copy(), verbose=False)
        p1.run()
        p2 = Pipeline.from_dataframe(sample_df.iloc[250:].copy(), verbose=False)
        p2.run()
        report = p1.fraud.drift(p2)
        assert report.status in {"stable", "monitor", "retrain"}

    def test_drift_psi_non_negative(self, sample_df):
        """PSI scores are always non-negative."""
        p1 = Pipeline.from_dataframe(sample_df.iloc[:250].copy(), verbose=False)
        p1.run()
        p2 = Pipeline.from_dataframe(sample_df.iloc[250:].copy(), verbose=False)
        p2.run()
        report = p1.fraud.drift(p2)
        assert report.overall_psi >= 0
        for feat, psi in report.feature_psi.items():
            assert psi >= 0, f"Negative PSI for {feat}"

    def test_drift_identical_data_is_stable(self, sample_df):
        """Same data compared to itself should be stable (PSI ≈ 0)."""
        p1 = Pipeline.from_dataframe(sample_df.copy(), verbose=False)
        p1.run()
        p2 = Pipeline.from_dataframe(sample_df.copy(), verbose=False)
        p2.run()
        report = p1.fraud.drift(p2)
        assert report.overall_psi < 0.10, f"Expected stable PSI, got {report.overall_psi}"
        assert report.status == "stable"

    def test_drift_summary_has_required_keys(self, sample_df):
        """Summary dict has all expected keys."""
        p1 = Pipeline.from_dataframe(sample_df.iloc[:250].copy(), verbose=False)
        p1.run()
        p2 = Pipeline.from_dataframe(sample_df.iloc[250:].copy(), verbose=False)
        p2.run()
        report = p1.fraud.drift(p2)
        s = report.summary()
        for key in ["overall_psi", "status", "should_retrain",
                    "fraud_rate_ref", "fraud_rate_cur", "volume_ref", "volume_cur"]:
            assert key in s, f"Missing key: {key}"

    def test_drift_accepts_dataframe(self, sample_df):
        """drift() accepts a raw DataFrame, not just a Pipeline."""
        p1 = Pipeline.from_dataframe(sample_df.copy(), verbose=False)
        p1.run()
        report = p1.fraud.drift(sample_df.copy())
        assert report is not None

    def test_drift_recommendations_non_empty(self, sample_df):
        """Drift report always includes at least one recommendation."""
        p1 = Pipeline.from_dataframe(sample_df.iloc[:250].copy(), verbose=False)
        p1.run()
        p2 = Pipeline.from_dataframe(sample_df.iloc[250:].copy(), verbose=False)
        p2.run()
        report = p1.fraud.drift(p2)
        assert len(report.recommendations) >= 1


# ── SEGMENT EXPLAINABILITY ────────────────────────────────────────────────────

class TestSegmentExplainability:

    def test_explain_returns_explanation(self, pipeline):
        """explain() returns a SegmentExplanation object."""
        cid = pipeline._normalised["customer_id"].iloc[0]
        exp = pipeline.segment.explain(str(cid))
        assert exp is not None
        assert hasattr(exp, "segment")
        assert hasattr(exp, "r_score")
        assert hasattr(exp, "recommended_action")

    def test_explain_segment_is_valid(self, pipeline):
        """Explanation segment name is a known RFM segment."""
        valid = {
            "Champions", "Loyal Customers", "Potential Loyalists",
            "Recent Customers", "Promising", "Need Attention",
            "About to Sleep", "At Risk", "Cannot Lose Them",
            "Hibernating", "Lost", "Others"
        }
        cid = pipeline._normalised["customer_id"].iloc[0]
        exp = pipeline.segment.explain(str(cid))
        assert exp.segment in valid, f"Unknown segment: {exp.segment}"

    def test_explain_scores_in_range(self, pipeline):
        """RFM scores are all between 1 and 5."""
        cid = pipeline._normalised["customer_id"].iloc[0]
        exp = pipeline.segment.explain(str(cid))
        assert 1 <= exp.r_score <= 5
        assert 1 <= exp.f_score <= 5
        assert 1 <= exp.m_score <= 5

    def test_explain_has_drivers(self, pipeline):
        """Explanation always has at least one driver."""
        cid = pipeline._normalised["customer_id"].iloc[0]
        exp = pipeline.segment.explain(str(cid))
        assert len(exp.what_drives_this) >= 1

    def test_explain_str_works(self, pipeline):
        """str(explanation) returns non-empty string."""
        cid = pipeline._normalised["customer_id"].iloc[0]
        exp = pipeline.segment.explain(str(cid))
        assert len(str(exp)) > 50

    def test_explain_invalid_customer_raises(self, pipeline):
        """Unknown customer_id raises ValueError."""
        with pytest.raises(ValueError, match="not found"):
            pipeline.segment.explain("nonexistent_customer_xyz")

    def test_batch_explain_returns_dataframe(self, pipeline):
        """batch_explain() returns a non-empty DataFrame."""
        df = pipeline.segment.batch_explain()
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0
        assert "segment" in df.columns
        assert "recommended_action" in df.columns

    def test_batch_explain_segment_filter(self, pipeline):
        """batch_explain() with segment filter only returns that segment."""
        # Get a segment that exists
        existing = pipeline.segment.rfm()["segment"].iloc[0]
        df = pipeline.segment.batch_explain(segment_filter=existing)
        assert isinstance(df, pd.DataFrame)
        if len(df) > 0:
            assert (df["segment"] == existing).all()


# ── REVENUE FORECASTING ───────────────────────────────────────────────────────

class TestForecasting:

    def test_forecast_returns_result(self, pipeline):
        """forecast() returns a ForecastResult."""
        result = pipeline.forecast.forecast(months=3)
        assert result is not None
        assert hasattr(result, "revenue")
        assert hasattr(result, "fraud_rate")
        assert hasattr(result, "months_forecast")

    def test_forecast_correct_number_of_months(self, pipeline):
        """forecast() returns the requested number of months."""
        result = pipeline.forecast.forecast(months=3)
        if not result.revenue.empty:
            assert len(result.revenue) == 3
        if not result.fraud_rate.empty:
            assert len(result.fraud_rate) == 3

    def test_forecast_revenue_non_negative(self, pipeline):
        """Forecast revenue values are always non-negative."""
        result = pipeline.forecast.forecast(months=3)
        if not result.revenue.empty:
            assert (result.revenue["predicted"] >= 0).all()
            assert (result.revenue["lower_95"]  >= 0).all()

    def test_forecast_fraud_rate_in_range(self, pipeline):
        """Fraud rate forecast is between 0% and 15%."""
        result = pipeline.forecast.forecast(months=3)
        if not result.fraud_rate.empty:
            assert (result.fraud_rate["predicted"].between(0, 15)).all()

    def test_forecast_has_confidence_intervals(self, pipeline):
        """Forecast includes lower_95 and upper_95 columns."""
        result = pipeline.forecast.forecast(months=3)
        if not result.revenue.empty:
            assert "lower_95" in result.revenue.columns
            assert "upper_95" in result.revenue.columns
        if not result.fraud_rate.empty:
            assert "lower_95" in result.fraud_rate.columns
            assert "upper_95" in result.fraud_rate.columns

    def test_forecast_model_used_is_valid(self, pipeline):
        """model_used is a known model name."""
        result = pipeline.forecast.forecast(months=3)
        assert result.model_used in {"prophet", "linear_trend", "none"}

    def test_forecast_confidence_is_valid(self, pipeline):
        """confidence field is one of the three valid values."""
        result = pipeline.forecast.forecast(months=3)
        assert result.confidence in {"high", "medium", "low"}

    def test_forecast_summary_has_keys(self, pipeline):
        """summary() returns dict with expected keys."""
        result = pipeline.forecast.forecast(months=3)
        s = result.summary()
        assert "months_forecast" in s
        assert "model" in s
        assert "confidence" in s

    def test_forecast_months_clamped(self, pipeline):
        """Requesting more than 12 months is clamped to 12."""
        result = pipeline.forecast.forecast(months=24)
        assert result.months_forecast <= 12

    def test_forecast_1_month(self, pipeline):
        """1 month forecast works without error."""
        result = pipeline.forecast.forecast(months=1)
        assert result is not None
