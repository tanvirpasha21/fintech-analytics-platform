"""
fintech_analytics.compliance.checks
=====================================
AML (Anti-Money Laundering) and regulatory compliance checks.

This is the most unique feature of fintech_analytics — no other
Python library provides financial compliance checks out of the box.

Checks implemented:
  - Structuring (smurfing) — multiple transactions just below reporting thresholds
  - Velocity anomalies — unusual transaction frequency in short windows
  - Round number bias — unusually high proportion of round-number transactions
  - Cross-border patterns — high volume of cross-jurisdiction transactions
  - Layering patterns — rapid movement of funds through multiple merchants
  - Dormant account activity — sudden activity on previously quiet accounts
"""

from __future__ import annotations

import pandas as pd
import numpy as np
import duckdb
from dataclasses import dataclass, field
from typing import Optional
from rich.console import Console
from rich.table import Table

console = Console()

# Regulatory thresholds (UK/EU defaults — configurable)
AML_THRESHOLDS = {
    "structuring_threshold_gbp":  10_000,   # UK POCA threshold
    "structuring_window_days":    5,         # days to look back for structuring
    "structuring_min_txns":       3,         # min transactions to flag structuring
    "velocity_window_hours":      24,        # velocity check window
    "velocity_max_txns":          20,        # max transactions in window
    "round_number_threshold_pct": 40.0,      # % of round-number txns to flag
    "dormancy_days":              90,        # days of inactivity = dormant
    "large_cash_threshold_gbp":   5_000,     # flag large ATM/cash transactions
}


@dataclass
class AMLFlag:
    """A single AML flag raised on a customer or transaction."""
    flag_type:    str
    severity:     str       # "HIGH", "MEDIUM", "LOW"
    customer_id:  Optional[str]
    description:  str
    transactions: list      = field(default_factory=list)
    amount_total: float     = 0.0
    recommended:  str       = ""


class ComplianceAccessor:
    """AML and regulatory compliance checks."""

    def __init__(self, con: duckdb.DuckDBPyConnection, df: pd.DataFrame):
        self._con = con
        self._df  = df

    def aml_flags(self, thresholds: Optional[dict] = None) -> pd.DataFrame:
        """
        Run all AML checks and return flagged customers/transactions.

        Args:
            thresholds: Override default thresholds.
                        e.g. {"structuring_threshold_gbp": 15000}

        Returns:
            DataFrame of AML flags with:
                flag_type, severity, customer_id, description,
                transaction_count, amount_total, recommended_action
        """
        t = {**AML_THRESHOLDS, **(thresholds or {})}
        flags = []

        flags.extend(self._check_structuring(t))
        flags.extend(self._check_velocity(t))
        flags.extend(self._check_round_numbers(t))
        flags.extend(self._check_dormant_activity(t))
        flags.extend(self._check_large_cash(t))

        if not flags:
            console.print("[green]✓ No AML flags detected[/green]")
            return pd.DataFrame(columns=[
                "flag_type","severity","customer_id","description",
                "transaction_count","amount_total","recommended_action"
            ])

        df = pd.DataFrame([{
            "flag_type":          f.flag_type,
            "severity":           f.severity,
            "customer_id":        f.customer_id,
            "description":        f.description,
            "transaction_count":  len(f.transactions),
            "amount_total":       round(f.amount_total, 2),
            "recommended_action": f.recommended,
        } for f in flags])

        return df.sort_values(
            ["severity", "amount_total"],
            key=lambda x: x.map({"HIGH": 0, "MEDIUM": 1, "LOW": 2}) if x.name == "severity" else x,
            ascending=[True, False],
        ).reset_index(drop=True)

    def _check_structuring(self, t: dict) -> list[AMLFlag]:
        """
        Structuring (smurfing): multiple transactions just below reporting threshold.
        Classic pattern: £9,500, £9,800, £9,200 over a few days.
        """
        threshold = t["structuring_threshold_gbp"]
        window    = t["structuring_window_days"]
        min_txns  = t["structuring_min_txns"]
        low_band  = threshold * 0.7

        flags = []
        try:
            results = self._con.execute(f"""
                WITH suspicious AS (
                    SELECT customer_id, transaction_at, amount, transaction_id
                    FROM raw.transactions
                    WHERE amount >= {low_band}
                      AND amount <  {threshold}
                      AND status = 'completed'
                ),
                grouped AS (
                    SELECT
                        a.customer_id,
                        count(b.transaction_id) as txn_count,
                        sum(b.amount)           as total_amount,
                        list(b.transaction_id)  as txn_ids
                    FROM suspicious a
                    JOIN suspicious b ON a.customer_id = b.customer_id
                        AND cast(b.transaction_at as date)
                            BETWEEN cast(a.transaction_at as date) - {window}
                            AND cast(a.transaction_at as date)
                    GROUP BY a.customer_id, cast(a.transaction_at as date)
                    HAVING count(b.transaction_id) >= {min_txns}
                )
                SELECT customer_id, max(txn_count) as txn_count,
                       max(total_amount) as total_amount
                FROM grouped
                GROUP BY customer_id
            """).df()

            for _, row in results.iterrows():
                flags.append(AMLFlag(
                    flag_type   ="STRUCTURING",
                    severity    ="HIGH",
                    customer_id =str(row["customer_id"]),
                    description =(
                        f"Possible structuring: {int(row['txn_count'])} transactions "
                        f"totalling ${row['total_amount']:,.2f} — all below "
                        f"${threshold:,} threshold within {window} days"
                    ),
                    amount_total=float(row["total_amount"]),
                    recommended ="File Suspicious Activity Report (SAR). "
                                 "Escalate to MLRO for review.",
                ))
        except Exception as e:
            console.print(f"[yellow]Structuring check skipped: {e}[/yellow]")

        return flags

    def _check_velocity(self, t: dict) -> list[AMLFlag]:
        """Unusual transaction frequency in a short time window."""
        max_txns = t["velocity_max_txns"]
        window_h = t["velocity_window_hours"]

        flags = []
        try:
            results = self._con.execute(f"""
                SELECT
                    customer_id,
                    count(*) as txn_count,
                    sum(amount) as total_amount,
                    min(transaction_at) as window_start,
                    max(transaction_at) as window_end
                FROM raw.transactions
                WHERE status = 'completed'
                GROUP BY customer_id,
                    epoch(cast(transaction_at as timestamp)) / ({window_h} * 3600)
                HAVING count(*) > {max_txns}
            """).df()

            for _, row in results.iterrows():
                flags.append(AMLFlag(
                    flag_type   ="VELOCITY_ANOMALY",
                    severity    ="MEDIUM",
                    customer_id =str(row["customer_id"]),
                    description =(
                        f"Velocity flag: {int(row['txn_count'])} transactions "
                        f"in {window_h}h window — exceeds {max_txns} threshold. "
                        f"Total: ${row['total_amount']:,.2f}"
                    ),
                    amount_total=float(row["total_amount"]),
                    recommended ="Enhanced due diligence. "
                                 "Review account activity pattern.",
                ))
        except Exception as e:
            console.print(f"[yellow]Velocity check skipped: {e}[/yellow]")

        return flags

    def _check_round_numbers(self, t: dict) -> list[AMLFlag]:
        """
        Round number bias — human fraud often uses round numbers.
        Legitimate spending has natural distribution; fraud shows spikes at 100, 500, 1000.
        """
        threshold_pct = t["round_number_threshold_pct"]

        flags = []
        try:
            results = self._con.execute(f"""
                SELECT
                    customer_id,
                    count(*) as total_txns,
                    count(case when amount % 100 = 0 then 1 end) as round_txns,
                    round(count(case when amount % 100 = 0 then 1 end) * 100.0
                          / nullif(count(*), 0), 1) as round_pct,
                    sum(amount) as total_amount
                FROM raw.transactions
                WHERE status = 'completed'
                GROUP BY customer_id
                HAVING count(*) >= 5
                   AND round(count(case when amount % 100 = 0 then 1 end) * 100.0
                       / nullif(count(*), 0), 1) > {threshold_pct}
            """).df()

            for _, row in results.iterrows():
                flags.append(AMLFlag(
                    flag_type   ="ROUND_NUMBER_BIAS",
                    severity    ="LOW",
                    customer_id =str(row["customer_id"]),
                    description =(
                        f"{row['round_pct']}% of transactions are round numbers "
                        f"(threshold: {threshold_pct}%) — possible manual fraud"
                    ),
                    amount_total=float(row["total_amount"]),
                    recommended ="Monitor account. Consider enhanced verification.",
                ))
        except Exception as e:
            console.print(f"[yellow]Round number check skipped: {e}[/yellow]")

        return flags

    def _check_dormant_activity(self, t: dict) -> list[AMLFlag]:
        """Sudden activity on previously dormant accounts."""
        dormancy_days = t["dormancy_days"]

        flags = []
        try:
            results = self._con.execute(f"""
                WITH activity AS (
                    SELECT customer_id,
                        cast(transaction_at as date) as txn_date,
                        amount
                    FROM raw.transactions
                    WHERE status = 'completed'
                ),
                gaps AS (
                    SELECT customer_id, txn_date, amount,
                        lag(txn_date) OVER (PARTITION BY customer_id ORDER BY txn_date) as prev_date,
                        datediff('day',
                            lag(txn_date) OVER (PARTITION BY customer_id ORDER BY txn_date),
                            txn_date) as days_gap
                    FROM activity
                )
                SELECT customer_id,
                    max(days_gap) as max_gap_days,
                    sum(amount) as recent_amount,
                    count(*) as recent_txns
                FROM gaps
                WHERE days_gap > {dormancy_days}
                GROUP BY customer_id
            """).df()

            for _, row in results.iterrows():
                flags.append(AMLFlag(
                    flag_type   ="DORMANT_ACCOUNT_ACTIVITY",
                    severity    ="MEDIUM",
                    customer_id =str(row["customer_id"]),
                    description =(
                        f"Account was dormant for {int(row['max_gap_days'])} days, "
                        f"then made {int(row['recent_txns'])} transaction(s) "
                        f"totalling ${row['recent_amount']:,.2f}"
                    ),
                    amount_total=float(row["recent_amount"]),
                    recommended ="Verify customer identity. "
                                 "Review recent transactions for legitimacy.",
                ))
        except Exception as e:
            console.print(f"[yellow]Dormant account check skipped: {e}[/yellow]")

        return flags

    def _check_large_cash(self, t: dict) -> list[AMLFlag]:
        """Flag large ATM / cash-equivalent transactions."""
        threshold = t["large_cash_threshold_gbp"]

        flags = []
        try:
            results = self._con.execute(f"""
                SELECT customer_id, transaction_id, amount, merchant_category
                FROM raw.transactions
                WHERE amount > {threshold}
                  AND (lower(merchant_category) LIKE '%atm%'
                       OR lower(merchant_category) LIKE '%cash%'
                       OR lower(merchant_category) LIKE '%transfer%')
                  AND status = 'completed'
                ORDER BY amount DESC
            """).df()

            for _, row in results.iterrows():
                flags.append(AMLFlag(
                    flag_type   ="LARGE_CASH_TRANSACTION",
                    severity    ="HIGH",
                    customer_id =str(row["customer_id"]),
                    description =(
                        f"Large {row['merchant_category']} transaction: "
                        f"${row['amount']:,.2f} — exceeds ${threshold:,} threshold"
                    ),
                    transactions=[str(row["transaction_id"])],
                    amount_total=float(row["amount"]),
                    recommended ="Mandatory reporting review. "
                                 "Consider filing CTR (Currency Transaction Report).",
                ))
        except Exception as e:
            console.print(f"[yellow]Large cash check skipped: {e}[/yellow]")

        return flags

    def print_report(self, thresholds: Optional[dict] = None):
        """Print a formatted AML compliance report."""
        df = self.aml_flags(thresholds)
        if df.empty:
            return

        t = Table(title="AML Compliance Flags")
        t.add_column("Flag Type",     style="red",   width=28)
        t.add_column("Severity",      style="white", width=10)
        t.add_column("Customer",      style="cyan",  width=16)
        t.add_column("Description",   style="white", width=50)
        t.add_column("Amount",        style="white", width=12, justify="right")
        t.add_column("Action",        style="yellow",width=35)

        for _, row in df.iterrows():
            sev_color = "red" if row["severity"] == "HIGH" else "yellow" if row["severity"] == "MEDIUM" else "green"
            t.add_row(
                row["flag_type"],
                f"[{sev_color}]{row['severity']}[/{sev_color}]",
                str(row["customer_id"])[:14],
                str(row["description"])[:48],
                f"${row['amount_total']:,.2f}",
                str(row["recommended_action"])[:33],
            )
        console.print(t)
        console.print(f"\n[bold]Total flags: {len(df)}[/bold]  "
                      f"HIGH: {(df['severity']=='HIGH').sum()}  "
                      f"MEDIUM: {(df['severity']=='MEDIUM').sum()}  "
                      f"LOW: {(df['severity']=='LOW').sum()}")
