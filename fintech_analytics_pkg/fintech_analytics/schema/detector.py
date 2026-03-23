"""
fintech_analytics.schema.detector
===================================
Automatically detects financial column names from any CSV.

Uses fuzzy string matching + pattern recognition to map arbitrary
column names to the internal fintech_analytics schema.

Example:
    detector = SchemaDetector()
    mapping = detector.detect(df)
    # {"transaction_id": "ref_number", "amount": "trans_amt", ...}
"""

from __future__ import annotations

import re
from typing import Optional
import pandas as pd
from fuzzywuzzy import fuzz
from rich.console import Console
from rich.table import Table

console = Console()

# ── CANONICAL SCHEMA ─────────────────────────────────────────────────────────
# These are the internal column names the pipeline expects.
# Each entry has aliases that fuzzy-match against user column names.

SCHEMA_ALIASES: dict[str, list[str]] = {
    # Required fields
    "transaction_id": [
        "transaction_id", "txn_id", "trans_id", "id", "ref",
        "reference", "ref_number", "transaction_ref", "payment_id",
        "order_id", "record_id", "uuid", "tid",
    ],
    "amount": [
        "amount", "value", "amt", "transaction_amount", "trans_amount",
        "trans_amt", "payment_amount", "sum", "total", "price",
        "charge", "debit", "credit", "gbp_amount", "usd_amount",
    ],
    "transaction_at": [
        "transaction_at", "timestamp", "date", "datetime", "trans_date",
        "transaction_date", "created_at", "time", "trans_time",
        "payment_date", "occurred_at", "event_time", "dt",
    ],
    # Optional but important
    "customer_id": [
        "customer_id", "user_id", "client_id", "account_id",
        "cardholder_id", "holder_id", "member_id", "cust_id",
        "account_number", "card_id",
    ],
    "merchant_id": [
        "merchant_id", "vendor_id", "store_id", "shop_id",
        "retailer_id", "payee_id", "recipient_id",
    ],
    "merchant_name": [
        "merchant_name", "merchant", "vendor", "store", "shop",
        "retailer", "payee", "recipient", "description",
        "transaction_description", "narration",
    ],
    "merchant_category": [
        "merchant_category", "category", "mcc", "merchant_type",
        "type", "sector", "industry", "merchant_category_code",
    ],
    "currency": [
        "currency", "ccy", "currency_code", "iso_currency",
        "transaction_currency", "orig_currency",
    ],
    "status": [
        "status", "transaction_status", "state", "result",
        "outcome", "response", "trans_status", "payment_status",
    ],
    "channel": [
        "channel", "payment_channel", "medium", "method",
        "payment_method", "source", "transaction_type",
    ],
    "country": [
        "country", "country_code", "ip_country", "location",
        "region", "geo", "origin_country",
    ],
    "is_fraud": [
        "is_fraud", "fraud", "is_fraudulent", "fraud_flag",
        "fraud_label", "class", "label", "target",
        "is_flagged_fraud", "fraud_indicator",
    ],
    "card_type": [
        "card_type", "card", "payment_type", "instrument",
    ],
}

# Required columns — pipeline cannot run without these
REQUIRED_COLUMNS = {"transaction_id", "amount", "transaction_at"}

# Columns with known value mappings
STATUS_VALUES = {
    "completed": ["completed", "success", "approved", "settled", "successful", "ok", "1", "true"],
    "declined":  ["declined", "failed", "rejected", "denied", "refused", "0", "false"],
    "pending":   ["pending", "processing", "in_progress", "submitted"],
}


class SchemaDetector:
    """
    Automatically detects and maps financial column names.

    Uses fuzzy string matching (Levenshtein distance) to map
    arbitrary column names to the internal fintech_analytics schema.
    """

    def __init__(self, threshold: int = 70):
        """
        Args:
            threshold: Minimum fuzzy match score (0-100). Default 70.
                       Higher = stricter matching, fewer false positives.
        """
        self.threshold = threshold
        self._mapping: dict[str, str] = {}
        self._confidence: dict[str, int] = {}
        self._unmatched: list[str] = []

    def detect(self, df: pd.DataFrame) -> "ColumnMapping":
        """
        Detect column mappings from a DataFrame.

        Returns a ColumnMapping object with detected mappings
        and confidence scores.
        """
        from fintech_analytics.schema.mapping import ColumnMapping

        user_columns = list(df.columns)
        mapping = {}
        confidence = {}
        unmatched_canonical = []

        for canonical, aliases in SCHEMA_ALIASES.items():
            best_col   = None
            best_score = 0

            for user_col in user_columns:
                # Try exact match first
                if user_col.lower().strip() in [a.lower() for a in aliases]:
                    best_col   = user_col
                    best_score = 100
                    break

                # Fuzzy match
                for alias in aliases:
                    score = fuzz.ratio(user_col.lower().strip(), alias.lower())
                    if score > best_score and score >= self.threshold:
                        best_score = score
                        best_col   = user_col

            if best_col:
                mapping[canonical]    = best_col
                confidence[canonical] = best_score
            else:
                unmatched_canonical.append(canonical)

        self._mapping    = mapping
        self._confidence = confidence
        self._unmatched  = unmatched_canonical

        return ColumnMapping(
            mapping=mapping,
            confidence=confidence,
            unmatched_canonical=unmatched_canonical,
            original_columns=user_columns,
        )

    def print_report(self, mapping: "ColumnMapping"):
        """Print a rich table showing the detected schema mapping."""
        table = Table(title="Schema Detection Report", show_header=True)
        table.add_column("Internal Field",    style="cyan",  width=25)
        table.add_column("Your Column",       style="green", width=25)
        table.add_column("Confidence",        style="white", width=12)
        table.add_column("Required",          style="white", width=10)

        for canonical in SCHEMA_ALIASES:
            is_required = canonical in REQUIRED_COLUMNS
            if canonical in mapping.mapping:
                user_col   = mapping.mapping[canonical]
                conf       = mapping.confidence.get(canonical, 0)
                conf_str   = f"[green]{conf}%[/green]" if conf >= 90 else f"[yellow]{conf}%[/yellow]"
                req_str    = "[red]✓ required[/red]" if is_required else "optional"
                table.add_row(canonical, user_col, conf_str, req_str)
            elif is_required:
                table.add_row(
                    canonical,
                    "[red]NOT FOUND[/red]",
                    "[red]0%[/red]",
                    "[red]✓ required[/red]",
                )

        console.print(table)

        if mapping.unmatched_canonical:
            optional_missing = [
                c for c in mapping.unmatched_canonical
                if c not in REQUIRED_COLUMNS
            ]
            if optional_missing:
                console.print(
                    f"\n[yellow]Optional columns not detected:[/yellow] "
                    f"{', '.join(optional_missing)}"
                )
                console.print(
                    "[dim]These fields will be set to null. "
                    "Some analytics may be limited.[/dim]"
                )
