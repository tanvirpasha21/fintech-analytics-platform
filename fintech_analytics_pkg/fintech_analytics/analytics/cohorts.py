"""
fintech_analytics.analytics.cohorts
=====================================
Cohort retention and revenue analytics.
"""
from __future__ import annotations
import pandas as pd
import duckdb


class CohortAccessor:
    """Access cohort analytics."""

    def __init__(self, con: duckdb.DuckDBPyConnection):
        self._con = con

    def retention(self, max_months: int = 12) -> pd.DataFrame:
        """
        Return cohort retention matrix.

        Returns:
            DataFrame: cohort_month × months_since_signup → retention_pct
        """
        return self._con.execute(f"""
            SELECT * FROM analytics.cohort_retention
            WHERE months_since_signup <= {max_months}
            ORDER BY cohort_month, months_since_signup
        """).df()

    def retention_matrix(self, max_months: int = 6) -> pd.DataFrame:
        """
        Return pivot table of cohort retention (cohorts as rows, months as columns).
        Classic heatmap-ready format.
        """
        df = self.retention(max_months)
        if df.empty:
            return df

        pivot = df.pivot_table(
            index="cohort_month",
            columns="months_since_signup",
            values="retention_pct",
        )
        pivot.columns = [f"M{int(c)}" for c in pivot.columns]
        pivot.index   = pivot.index.astype(str)
        return pivot

    def best_cohort(self) -> dict:
        """Return the cohort with the highest M3 retention."""
        r = self._con.execute("""
            SELECT cohort_month, retention_pct
            FROM analytics.cohort_retention
            WHERE months_since_signup = 3
            ORDER BY retention_pct DESC
            LIMIT 1
        """).fetchone()
        return {"cohort": str(r[0]), "m3_retention_pct": r[1]} if r else {}

    def average_retention(self) -> pd.DataFrame:
        """Average retention rate across all cohorts per month."""
        return self._con.execute("""
            SELECT
                months_since_signup,
                round(avg(retention_pct), 1) as avg_retention_pct,
                count(distinct cohort_month)  as cohorts
            FROM analytics.cohort_retention
            GROUP BY 1
            ORDER BY 1
        """).df()
