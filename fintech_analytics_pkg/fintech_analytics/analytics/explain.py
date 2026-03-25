"""
fintech_analytics.analytics.explain
=====================================
Segment explainability — explains WHY a customer is in their current segment,
what changed to put them there, and exactly what they need to do to move up.

This turns RFM analytics into actionable CRM intelligence.

Usage:
    p = Pipeline.from_csv("transactions.csv")
    p.run()

    explanation = p.segment.explain("cust_123")
    print(explanation)

    p.segment.print_explain("cust_123")
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

import duckdb
import pandas as pd
from rich.console import Console
from rich.table import Table

console = Console()

# Segment hierarchy — higher index = better segment
SEGMENT_RANK = {
    "Lost":               1,
    "Hibernating":        2,
    "Cannot Lose Them":   3,
    "At Risk":            4,
    "About to Sleep":     5,
    "Need Attention":     6,
    "Others":             7,
    "Promising":          8,
    "Recent Customers":   9,
    "Potential Loyalists":10,
    "Loyal Customers":    11,
    "Champions":          12,
}

SEGMENT_COLOR = {
    "Champions":          "cyan",
    "Loyal Customers":    "blue",
    "Potential Loyalists":"green",
    "Recent Customers":   "blue",
    "Promising":          "cyan",
    "Need Attention":     "yellow",
    "About to Sleep":     "yellow",
    "At Risk":            "red",
    "Cannot Lose Them":   "red",
    "Hibernating":        "dim",
    "Lost":               "dim",
    "Others":             "white",
}


@dataclass
class SegmentExplanation:
    """
    Full explanation of a customer's current segment and how to improve it.

    Attributes:
        customer_id:       The customer being explained
        segment:           Current segment name
        r_score:           Recency score (1–5)
        f_score:           Frequency score (1–5)
        m_score:           Monetary score (1–5)
        rfm_total:         Total RFM score (3–15)
        recency_days:      Days since last transaction
        frequency:         Total completed transactions
        monetary:          Total spend
        what_drives_this:  Why they're in this segment
        what_changed:      What deteriorated vs their best potential
        to_move_up:        Exact actions to reach the next segment
        recommended_action: The one key action to take now
    """
    customer_id:       str
    segment:           str
    r_score:           int
    f_score:           int
    m_score:           int
    rfm_total:         int
    recency_days:      int
    frequency:         int
    monetary:          float
    what_drives_this:  list[str]    = field(default_factory=list)
    what_changed:      list[str]    = field(default_factory=list)
    to_move_up:        dict         = field(default_factory=dict)
    recommended_action: str        = ""

    def __str__(self) -> str:
        lines = [
            f"Customer: {self.customer_id}",
            f"Segment:  {self.segment}  (R={self.r_score} F={self.f_score} M={self.m_score}  total={self.rfm_total})",
            f"Recency:  {self.recency_days} days since last transaction",
            f"Frequency:{self.frequency} transactions",
            f"Monetary: ${self.monetary:,.2f} lifetime spend",
            "",
            "Why this segment:",
        ]
        for d in self.what_drives_this:
            lines.append(f"  • {d}")
        if self.what_changed:
            lines += ["", "What's changed:"]
            for c in self.what_changed:
                lines.append(f"  • {c}")
        if self.to_move_up:
            lines += ["", "To move up:"]
            for seg, action in self.to_move_up.items():
                lines.append(f"  → {seg}: {action}")
        if self.recommended_action:
            lines += ["", f"Action now: {self.recommended_action}"]
        return "\n".join(lines)


class SegmentExplainer:
    """
    Generates plain-English explanations for individual customer segments.
    """

    def __init__(self, con: duckdb.DuckDBPyConnection):
        self._con = con

    def explain(self, customer_id: str) -> SegmentExplanation:
        """
        Explain why a specific customer is in their current segment.

        Args:
            customer_id: The customer_id to explain

        Returns:
            SegmentExplanation with full breakdown and action steps
        """
        # ── Fetch customer RFM data ───────────────────────────────────────────
        row = self._con.execute("""
            SELECT
                customer_id,
                segment,
                r_score, f_score, m_score, rfm_total,
                recency_days, frequency, monetary,
                recommended_action
            FROM analytics.rfm
            WHERE customer_id = ?
        """, [str(customer_id)]).fetchone()

        if row is None:
            raise ValueError(
                f"Customer '{customer_id}' not found in RFM table. "
                "Make sure pipeline.run() has been called."
            )

        (cid, segment, r, f, m, total,
         recency_days, frequency, monetary,
         recommended_action) = row

        # ── Fetch population averages for context ─────────────────────────────
        avgs = self._con.execute("""
            SELECT
                avg(recency_days)  as avg_recency,
                avg(frequency)     as avg_freq,
                avg(monetary)      as avg_monetary,
                percentile_cont(0.75) within group (order by monetary)     as p75_monetary,
                percentile_cont(0.75) within group (order by frequency)    as p75_freq,
                percentile_cont(0.25) within group (order by recency_days) as p25_recency
            FROM analytics.rfm
        """).fetchone()

        avg_recency, avg_freq, avg_monetary, p75_monetary, p75_freq, p25_recency = avgs

        # ── Build explanation ─────────────────────────────────────────────────
        what_drives_this = self._explain_scores(
            r, f, m, recency_days, frequency, monetary,
            avg_recency, avg_freq, avg_monetary
        )

        what_changed = self._explain_changes(
            r, f, m, recency_days, frequency, monetary,
            avg_recency, avg_freq, avg_monetary,
            p75_monetary, p75_freq, p25_recency
        )

        to_move_up = self._explain_to_move_up(
            segment, r, f, m, recency_days, frequency, monetary,
            avg_recency, avg_freq, avg_monetary
        )

        return SegmentExplanation(
            customer_id       = str(cid),
            segment           = segment,
            r_score           = int(r),
            f_score           = int(f),
            m_score           = int(m),
            rfm_total         = int(total),
            recency_days      = int(recency_days) if recency_days else 0,
            frequency         = int(frequency) if frequency else 0,
            monetary          = float(monetary) if monetary else 0.0,
            what_drives_this  = what_drives_this,
            what_changed      = what_changed,
            to_move_up        = to_move_up,
            recommended_action= recommended_action or "",
        )

    def _explain_scores(
        self, r, f, m,
        recency_days, frequency, monetary,
        avg_recency, avg_freq, avg_monetary
    ) -> list[str]:
        """Explain what each score means for this customer."""
        reasons = []

        # Recency
        if r >= 4:
            reasons.append(
                f"Recency score {r}/5 — transacted {recency_days} days ago "
                f"(population avg: {avg_recency:.0f} days) — recently active"
            )
        elif r == 3:
            reasons.append(
                f"Recency score {r}/5 — transacted {recency_days} days ago "
                f"(avg: {avg_recency:.0f}) — starting to go quiet"
            )
        else:
            reasons.append(
                f"Recency score {r}/5 — last transaction {recency_days} days ago "
                f"(avg: {avg_recency:.0f}) — significantly lapsed"
            )

        # Frequency
        if f >= 4:
            reasons.append(
                f"Frequency score {f}/5 — {frequency} transactions "
                f"(avg: {avg_freq:.0f}) — highly engaged"
            )
        elif f == 3:
            reasons.append(
                f"Frequency score {f}/5 — {frequency} transactions "
                f"(avg: {avg_freq:.0f}) — average engagement"
            )
        else:
            reasons.append(
                f"Frequency score {f}/5 — only {frequency} transactions "
                f"(avg: {avg_freq:.0f}) — low engagement"
            )

        # Monetary
        if m >= 4:
            reasons.append(
                f"Monetary score {m}/5 — ${monetary:,.0f} lifetime spend "
                f"(avg: ${avg_monetary:,.0f}) — high value customer"
            )
        elif m == 3:
            reasons.append(
                f"Monetary score {m}/5 — ${monetary:,.0f} lifetime spend "
                f"(avg: ${avg_monetary:,.0f}) — mid-value customer"
            )
        else:
            reasons.append(
                f"Monetary score {m}/5 — ${monetary:,.0f} lifetime spend "
                f"(avg: ${avg_monetary:,.0f}) — low value so far"
            )

        return reasons

    def _explain_changes(
        self, r, f, m,
        recency_days, frequency, monetary,
        avg_recency, avg_freq, avg_monetary,
        p75_monetary, p75_freq, p25_recency
    ) -> list[str]:
        """Explain what's holding this customer back from a better segment."""
        changes = []

        # What would make them a Champion (R=5, F≥4, M≥4)
        if r < 4 and recency_days and avg_recency:
            needed = max(0, recency_days - (p25_recency or avg_recency * 0.5))
            if needed > 0:
                changes.append(
                    f"Recency is the main drag — needs to transact within "
                    f"{int(p25_recency or avg_recency * 0.5):.0f} days to reach top quartile"
                )

        if f < 4 and avg_freq:
            gap = max(0, (p75_freq or avg_freq * 1.5) - frequency)
            if gap > 1:
                changes.append(
                    f"Frequency is below top quartile — "
                    f"{gap:.0f} more transactions needed to reach top 25%"
                )

        if m < 4 and avg_monetary:
            gap = max(0, (p75_monetary or avg_monetary * 1.5) - monetary)
            if gap > 0:
                changes.append(
                    f"Spend is below top quartile — "
                    f"${gap:,.0f} more needed to reach top 25%"
                )

        return changes

    def _explain_to_move_up(
        self, segment, r, f, m,
        recency_days, frequency, monetary,
        avg_recency, avg_freq, avg_monetary
    ) -> dict[str, str]:
        """Return exact steps to reach the next 1–2 segments."""
        rank  = SEGMENT_RANK.get(segment, 5)
        steps = {}

        # Get the next 2 segments above current
        higher = {
            s: r for s, r in SEGMENT_RANK.items()
            if r > rank and s != "Others"
        }
        next_segs = sorted(higher.items(), key=lambda x: x[1])[:2]

        for next_seg, _ in next_segs:
            action = self._action_to_reach(
                next_seg, r, f, m,
                recency_days, frequency, monetary
            )
            if action:
                steps[next_seg] = action

        return steps

    def _action_to_reach(
        self, target_segment, r, f, m,
        recency_days, frequency, monetary
    ) -> str:
        """Return a concrete action to reach a target segment."""
        actions = {
            "Champions": (
                "Reach R=5, F≥4, M≥4 — transact within 30 days AND "
                "make 3+ more transactions totalling significant spend"
            ),
            "Loyal Customers": (
                "Reach F≥4, M≥3 — make frequent repeat transactions "
                "to build habit and increase lifetime spend"
            ),
            "Potential Loyalists": (
                "Reach R≥4, F≥2 — return within 30 days and "
                "make at least 2 transactions"
            ),
            "Recent Customers": (
                "Reach R≥4 — make one transaction in the next 30 days"
            ),
            "Promising": (
                "Reach R=3, F≥2 — return within 60 days and "
                "make at least 2 more purchases"
            ),
            "Need Attention": (
                "Reach R=3, F<2, M≥3 — make one high-value transaction "
                "within the next 60 days"
            ),
        }

        # Generate dynamic action if no preset
        if target_segment not in actions:
            if r < 3:
                return f"Make a transaction within {max(30, recency_days // 2)} days"
            elif f < 3:
                return f"Complete {max(1, 3 - f)} more transactions"
            elif m < 3:
                return f"Increase spend to top 40% of customer base"
            return "Increase engagement and recency"

        return actions[target_segment]

    def batch_explain(self, segment_filter: Optional[str] = None) -> pd.DataFrame:
        """
        Explain all customers, optionally filtered to a specific segment.

        Args:
            segment_filter: Only explain this segment (e.g. "At Risk")
                            None = all customers

        Returns:
            DataFrame with one row per customer and explanation fields
        """
        if segment_filter:
            customers = self._con.execute("""
                SELECT customer_id FROM analytics.rfm WHERE segment = ?
            """, [segment_filter]).df()["customer_id"].tolist()
        else:
            customers = self._con.execute(
                "SELECT customer_id FROM analytics.rfm"
            ).df()["customer_id"].tolist()

        rows = []
        for cid in customers:
            try:
                exp = self.explain(str(cid))
                rows.append({
                    "customer_id":        exp.customer_id,
                    "segment":            exp.segment,
                    "r_score":            exp.r_score,
                    "f_score":            exp.f_score,
                    "m_score":            exp.m_score,
                    "recency_days":       exp.recency_days,
                    "frequency":          exp.frequency,
                    "monetary":           exp.monetary,
                    "key_driver":         exp.what_drives_this[0] if exp.what_drives_this else "",
                    "recommended_action": exp.recommended_action,
                    "next_segment":       list(exp.to_move_up.keys())[0] if exp.to_move_up else "",
                    "next_step":          list(exp.to_move_up.values())[0] if exp.to_move_up else "",
                })
            except Exception:
                continue

        return pd.DataFrame(rows)

    def print_explain(self, customer_id: str):
        """Print a rich formatted explanation for a customer."""
        exp = self.explain(customer_id)
        colour = SEGMENT_COLOR.get(exp.segment, "white")

        console.print(f"\n[bold]Segment Explanation — {exp.customer_id}[/bold]")

        t = Table(show_header=False, box=None, padding=(0, 2))
        t.add_column(style="dim",   width=20)
        t.add_column(style="white", width=40)
        t.add_row("Segment",    f"[bold {colour}]{exp.segment}[/bold {colour}]")
        t.add_row("RFM Scores", f"R={exp.r_score}  F={exp.f_score}  M={exp.m_score}  Total={exp.rfm_total}/15")
        t.add_row("Last txn",   f"{exp.recency_days} days ago")
        t.add_row("Frequency",  f"{exp.frequency} transactions")
        t.add_row("Lifetime $", f"${exp.monetary:,.2f}")
        console.print(t)

        console.print("\n[bold dim]Why this segment:[/bold dim]")
        for d in exp.what_drives_this:
            console.print(f"  • {d}")

        if exp.what_changed:
            console.print("\n[bold dim]What's holding them back:[/bold dim]")
            for c in exp.what_changed:
                console.print(f"  • {c}")

        if exp.to_move_up:
            console.print("\n[bold dim]To move up:[/bold dim]")
            for seg, action in exp.to_move_up.items():
                console.print(f"  [cyan]→ {seg}:[/cyan] {action}")

        if exp.recommended_action:
            console.print(f"\n[bold]Action now:[/bold] {exp.recommended_action}\n")
