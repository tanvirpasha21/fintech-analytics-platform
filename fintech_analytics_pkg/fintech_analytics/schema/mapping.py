"""
fintech_analytics.schema.mapping
==================================
ColumnMapping dataclass and DataFrame normalisation.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Optional
import pandas as pd
import numpy as np
from rich.console import Console

console = Console()

REQUIRED_COLUMNS = {"transaction_id", "amount", "transaction_at"}

@dataclass
class ColumnMapping:
    """
    Holds the detected mapping between user columns and internal schema.

    Attributes:
        mapping:              canonical_name → user_column_name
        confidence:           canonical_name → match score (0-100)
        unmatched_canonical:  canonical fields with no detected match
        original_columns:     list of all user columns
    """
    mapping:              dict[str, str]
    confidence:           dict[str, int]
    unmatched_canonical:  list[str]
    original_columns:     list[str]

    def is_valid(self) -> tuple[bool, list[str]]:
        """
        Check if the mapping has all required fields.

        Returns:
            (is_valid, list_of_missing_required_fields)
        """
        missing = [col for col in REQUIRED_COLUMNS if col not in self.mapping]
        return len(missing) == 0, missing

    def override(self, overrides: dict[str, str]) -> "ColumnMapping":
        """
        Manually override detected mappings.

        Args:
            overrides: {canonical_name: user_column_name}

        Example:
            mapping.override({"transaction_id": "ref_no", "amount": "value"})
        """
        new_mapping    = {**self.mapping, **overrides}
        new_confidence = {**self.confidence, **{k: 100 for k in overrides}}
        return ColumnMapping(
            mapping=new_mapping,
            confidence=new_confidence,
            unmatched_canonical=[
                c for c in self.unmatched_canonical
                if c not in overrides
            ],
            original_columns=self.original_columns,
        )

    def normalise(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply the mapping to a DataFrame, returning a normalised DataFrame
        with internal column names.

        Missing optional columns are added as null.
        transaction_id nulls are replaced with generated UUIDs.
        amount is cast to float.
        transaction_at is cast to datetime.
        """
        valid, missing = self.is_valid()
        if not valid:
            raise ValueError(
                f"Cannot normalise: missing required columns: {missing}\n"
                f"Use mapping.override() to specify these columns manually."
            )

        # Rename user columns → canonical names
        rename_map = {v: k for k, v in self.mapping.items()}
        out = df.rename(columns=rename_map)

        # Keep only known canonical columns
        known = list(self.mapping.keys())
        out = out[[c for c in known if c in out.columns]].copy()

        # ── TRANSACTION ID ────────────────────────────────────────────────────
        if "transaction_id" in out.columns:
            null_mask = out["transaction_id"].isna()
            if null_mask.any():
                console.print(
                    f"[yellow]⚠  {null_mask.sum()} null transaction_ids "
                    f"— generating UUIDs[/yellow]"
                )
                out.loc[null_mask, "transaction_id"] = [
                    str(uuid.uuid4()) for _ in range(null_mask.sum())
                ]
            out["transaction_id"] = out["transaction_id"].astype(str)

        # ── AMOUNT ────────────────────────────────────────────────────────────
        if "amount" in out.columns:
            out["amount"] = pd.to_numeric(out["amount"], errors="coerce")
            neg = (out["amount"] < 0).sum()
            if neg > 0:
                console.print(
                    f"[yellow]⚠  {neg} negative amounts — taking absolute value[/yellow]"
                )
                out["amount"] = out["amount"].abs()

        # ── TIMESTAMP ─────────────────────────────────────────────────────────
        if "transaction_at" in out.columns:
            out["transaction_at"] = pd.to_datetime(out["transaction_at"], errors="coerce")
            nat = out["transaction_at"].isna().sum()
            if nat > 0:
                console.print(
                    f"[yellow]⚠  {nat} unparseable timestamps — these rows will be dropped[/yellow]"
                )
                out = out.dropna(subset=["transaction_at"])

        # ── STATUS NORMALISATION ──────────────────────────────────────────────
        if "status" in out.columns:
            from fintech_analytics.schema.detector import STATUS_VALUES
            status_map = {}
            for canonical_status, variants in STATUS_VALUES.items():
                for v in variants:
                    status_map[v.lower()] = canonical_status
            out["status"] = (
                out["status"]
                .astype(str)
                .str.lower()
                .str.strip()
                .map(status_map)
                .fillna("unknown")
            )

        # ── FRAUD FLAG ────────────────────────────────────────────────────────
        if "is_fraud" in out.columns:
            out["is_fraud"] = out["is_fraud"].map(
                lambda x: True if str(x).lower() in ["1", "true", "yes", "fraud"] else False
            )
        else:
            out["is_fraud"] = False

        # ── CURRENCY ─────────────────────────────────────────────────────────
        if "currency" not in out.columns:
            out["currency"] = "USD"   # default
        else:
            out["currency"] = out["currency"].str.upper().str.strip()

        # ── CUSTOMER ID ───────────────────────────────────────────────────────
        if "customer_id" not in out.columns:
            out["customer_id"] = "unknown"
        else:
            out["customer_id"] = out["customer_id"].astype(str).fillna("unknown")

        # ── MERCHANT FIELDS ───────────────────────────────────────────────────
        if "merchant_name" not in out.columns:
            out["merchant_name"] = "Unknown Merchant"
        if "merchant_category" not in out.columns:
            out["merchant_category"] = "Other"
        if "merchant_id" not in out.columns:
            out["merchant_id"] = (
                out.get("merchant_name", pd.Series(["unknown"] * len(out)))
                .astype(str)
                .apply(lambda x: str(uuid.uuid5(uuid.NAMESPACE_DNS, x)))
            )

        return out.reset_index(drop=True)
