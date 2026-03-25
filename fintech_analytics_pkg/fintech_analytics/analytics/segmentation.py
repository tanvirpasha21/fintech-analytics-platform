"""
fintech_analytics.analytics.segmentation
==========================================
RFM segmentation and customer analytics accessor.
"""

from __future__ import annotations
from typing import Optional
import pandas as pd
import duckdb
from rich.console import Console
from rich.table import Table

console = Console()

SEGMENT_COLORS = {
    "Champions":          "#00d4ff",
    "Loyal Customers":    "#7c3aed",
    "Potential Loyalists":"#10b981",
    "Recent Customers":   "#3b82f6",
    "Promising":          "#06b6d4",
    "Need Attention":     "#f59e0b",
    "About to Sleep":     "#f97316",
    "At Risk":            "#ef4444",
    "Cannot Lose Them":   "#dc2626",
    "Hibernating":        "#6b7280",
    "Lost":               "#374151",
    "Others":             "#9ca3af",
}


class SegmentAccessor:
    """Access customer segmentation analytics."""

    def __init__(self, con: duckdb.DuckDBPyConnection):
        self._con = con

    def rfm(self) -> pd.DataFrame:
        """
        Return the full RFM segmentation table.

        Returns:
            DataFrame with columns:
                customer_id, recency_days, frequency, monetary,
                r_score, f_score, m_score, rfm_total,
                segment, recommended_action
        """
        return self._con.execute("SELECT * FROM analytics.rfm ORDER BY monetary DESC").df()

    def summary(self) -> pd.DataFrame:
        """
        Return segment-level summary.

        Returns:
            DataFrame with one row per segment:
                segment, customers, avg_monetary, avg_frequency,
                avg_recency_days, total_revenue, pct_of_customers
        """
        return self._con.execute("""
            SELECT
                segment,
                count(*)                        as customers,
                round(avg(monetary), 2)         as avg_monetary,
                round(avg(frequency), 1)        as avg_frequency,
                round(avg(recency_days), 0)     as avg_recency_days,
                round(sum(monetary), 2)         as total_revenue,
                round(100.0 * count(*) / sum(count(*)) over(), 1) as pct_of_customers
            FROM analytics.rfm
            GROUP BY 1
            ORDER BY total_revenue DESC
        """).df()

    @property
    def champions(self) -> pd.DataFrame:
        """Customers in the Champions segment."""
        return self._con.execute(
            "SELECT * FROM analytics.rfm WHERE segment = 'Champions' ORDER BY monetary DESC"
        ).df()

    @property
    def at_risk(self) -> pd.DataFrame:
        """Customers in At Risk or Cannot Lose Them segments."""
        return self._con.execute("""
            SELECT * FROM analytics.rfm
            WHERE segment IN ('At Risk', 'Cannot Lose Them')
            ORDER BY monetary DESC
        """).df()

    @property
    def lost(self) -> pd.DataFrame:
        """Customers in Hibernating or Lost segments."""
        return self._con.execute("""
            SELECT * FROM analytics.rfm
            WHERE segment IN ('Hibernating', 'Lost')
            ORDER BY monetary DESC
        """).df()

    def get_segment(self, segment_name: str) -> pd.DataFrame:
        """Get customers for a specific segment by name."""
        return self._con.execute(
            "SELECT * FROM analytics.rfm WHERE segment = ? ORDER BY monetary DESC",
            [segment_name]
        ).df()

    def explain(self, customer_id: str) -> "SegmentExplanation":
        """
        Explain why a specific customer is in their current segment,
        what behaviours drove it, and exactly what they need to do to move up.

        Args:
            customer_id: The customer to explain

        Returns:
            SegmentExplanation with full breakdown, drivers, and action steps.

        Example:
            exp = p.segment.explain("cust_123")
            print(exp)                    # plain text
            p.segment.print_explain("cust_123")  # rich formatted
        """
        from fintech_analytics.analytics.explain import SegmentExplainer
        return SegmentExplainer(self._con).explain(customer_id)

    def print_explain(self, customer_id: str):
        """
        Print a rich formatted segment explanation for a customer.

        Shows: current segment, RFM scores, why they're there,
        what's holding them back, and exact steps to move up.

        Example:
            p.segment.print_explain("cust_123")
        """
        from fintech_analytics.analytics.explain import SegmentExplainer
        SegmentExplainer(self._con).print_explain(customer_id)

    def batch_explain(
        self,
        segment_filter: "Optional[str]" = None,
    ) -> "pd.DataFrame":
        """
        Explain all customers as a DataFrame — great for CRM export.

        Args:
            segment_filter: Only explain this segment (e.g. "At Risk")
                            None = all customers

        Returns:
            DataFrame with customer_id, segment, key_driver,
            recommended_action, next_segment, next_step

        Example:
            at_risk = p.segment.batch_explain("At Risk")
            at_risk.to_csv("at_risk_actions.csv", index=False)
        """
        from fintech_analytics.analytics.explain import SegmentExplainer
        return SegmentExplainer(self._con).batch_explain(segment_filter)

    def print_summary(self):
        """Print a rich table of segment summary."""
        df = self.summary()
        t  = Table(title="RFM Segment Summary")
        t.add_column("Segment",        style="cyan")
        t.add_column("Customers",      justify="right")
        t.add_column("% of Base",      justify="right")
        t.add_column("Avg Spend",      justify="right")
        t.add_column("Avg Frequency",  justify="right")
        t.add_column("Total Revenue",  justify="right")

        for _, row in df.iterrows():
            t.add_row(
                row["segment"],
                f"{row['customers']:,}",
                f"{row['pct_of_customers']}%",
                f"${row['avg_monetary']:,.2f}",
                f"{row['avg_frequency']:.1f}",
                f"${row['total_revenue']:,.2f}",
            )
        console.print(t)
