"""
fintech_analytics.analytics.merchants
=======================================
Merchant risk scoring and intelligence.
"""
from __future__ import annotations
import pandas as pd
import duckdb


class MerchantAccessor:
    """Access merchant intelligence analytics."""

    def __init__(self, con: duckdb.DuckDBPyConnection):
        self._con = con

    def risk_scorecard(self) -> pd.DataFrame:
        """Full merchant risk scorecard, ordered by fraud rate."""
        return self._con.execute(
            "SELECT * FROM analytics.merchant_scorecard ORDER BY fraud_rate_pct DESC"
        ).df()

    def critical(self) -> pd.DataFrame:
        """Merchants in the critical risk band (fraud rate > 5%)."""
        return self._con.execute(
            "SELECT * FROM analytics.merchant_scorecard WHERE risk_band = 'critical'"
        ).df()

    def elevated(self) -> pd.DataFrame:
        """Merchants in the elevated risk band (2–5% fraud rate)."""
        return self._con.execute(
            "SELECT * FROM analytics.merchant_scorecard WHERE risk_band = 'elevated'"
        ).df()

    def top_by_volume(self, n: int = 10) -> pd.DataFrame:
        """Top N merchants by total transaction volume."""
        return self._con.execute(f"""
            SELECT * FROM analytics.merchant_scorecard
            ORDER BY total_volume DESC NULLS LAST
            LIMIT {n}
        """).df()

    def category_summary(self) -> pd.DataFrame:
        """Aggregate metrics per merchant category."""
        return self._con.execute("""
            SELECT
                merchant_category,
                count(*)                        as merchants,
                sum(total_transactions)         as total_transactions,
                round(sum(total_volume), 2)     as total_volume,
                round(avg(fraud_rate_pct), 2)   as avg_fraud_rate_pct,
                round(avg(completion_rate_pct), 2) as avg_completion_pct
            FROM analytics.merchant_scorecard
            GROUP BY 1
            ORDER BY total_volume DESC NULLS LAST
        """).df()

    def velocity_alerts(self, z_threshold: float = 2.5) -> pd.DataFrame:
        """
        Flag merchants with unusually high transaction velocity.
        Uses z-score: merchants more than z_threshold std devs above mean.
        """
        return self._con.execute(f"""
            WITH stats AS (
                SELECT
                    avg(total_transactions) as mean_txns,
                    stddev(total_transactions) as std_txns
                FROM analytics.merchant_scorecard
            )
            SELECT m.*,
                round((m.total_transactions - s.mean_txns) / nullif(s.std_txns, 0), 2) as z_score
            FROM analytics.merchant_scorecard m CROSS JOIN stats s
            WHERE (m.total_transactions - s.mean_txns) / nullif(s.std_txns, 0) > {z_threshold}
            ORDER BY z_score DESC
        """).df()
