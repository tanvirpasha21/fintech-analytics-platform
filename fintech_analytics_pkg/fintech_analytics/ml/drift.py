"""
fintech_analytics.ml.drift
============================
Fraud model drift detection using Population Stability Index (PSI)
and feature distribution monitoring.

PSI is the industry-standard metric used by banks and fintechs to determine
when a fraud model needs retraining. A PSI above 0.2 means the population
has changed significantly enough that model predictions can no longer be trusted.

Usage:
    p_jan = Pipeline.from_csv("jan.csv"); p_jan.run()
    p_feb = Pipeline.from_csv("feb.csv"); p_feb.run()

    report = p_jan.fraud.drift(p_feb)
    print(report.summary())
    report.print_report()
    report.should_retrain   # True / False
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd
from rich.console import Console
from rich.table import Table

console = Console()

# PSI thresholds — industry standard (Basel II / model risk management)
PSI_STABLE    = 0.10   # < 0.10  → stable, no action needed
PSI_MONITOR   = 0.20   # 0.10–0.20 → monitor, consider retraining soon
PSI_RETRAIN   = 0.20   # > 0.20  → significant drift, retrain now


def _psi_score(expected: np.ndarray, actual: np.ndarray, bins: int = 10) -> float:
    """
    Compute the Population Stability Index between two distributions.

    PSI = Σ (Actual% - Expected%) × ln(Actual% / Expected%)

    Args:
        expected: Distribution from the reference (training) period
        actual:   Distribution from the current (monitoring) period
        bins:     Number of buckets to use (default 10)

    Returns:
        PSI score (float). Interpretation:
            < 0.10  → stable
            0.10–0.20 → monitor
            > 0.20  → retrain
    """
    # Create quantile-based bins from the expected distribution
    breakpoints = np.nanpercentile(expected, np.linspace(0, 100, bins + 1))
    breakpoints = np.unique(breakpoints)  # remove duplicates from constant features

    if len(breakpoints) < 2:
        return 0.0  # constant feature — no drift possible

    expected_counts = np.histogram(expected, bins=breakpoints)[0]
    actual_counts   = np.histogram(actual,   bins=breakpoints)[0]

    # Add small epsilon to avoid log(0) / division by zero
    eps = 1e-6
    expected_pct = (expected_counts + eps) / (len(expected) + eps * bins)
    actual_pct   = (actual_counts   + eps) / (len(actual)   + eps * bins)

    psi = np.sum((actual_pct - expected_pct) * np.log(actual_pct / expected_pct))
    return float(round(psi, 4))


@dataclass
class DriftReport:
    """
    Full drift analysis report comparing a reference period to a current period.

    Attributes:
        overall_psi:      Weighted average PSI across all features
        feature_psi:      Per-feature PSI scores
        fraud_rate_ref:   Fraud rate in the reference period
        fraud_rate_cur:   Fraud rate in the current period
        volume_ref:       Transaction count in reference period
        volume_cur:       Transaction count in current period
        status:           "stable" | "monitor" | "retrain"
        should_retrain:   True if model needs immediate retraining
        recommendations:  List of specific action recommendations
    """
    overall_psi:    float
    feature_psi:    dict[str, float]
    fraud_rate_ref: float
    fraud_rate_cur: float
    volume_ref:     int
    volume_cur:     int
    status:         str
    should_retrain: bool
    recommendations: list[str] = field(default_factory=list)

    def summary(self) -> dict:
        """Return a plain dict summary."""
        return {
            "overall_psi":    self.overall_psi,
            "status":         self.status,
            "should_retrain": self.should_retrain,
            "fraud_rate_ref": self.fraud_rate_ref,
            "fraud_rate_cur": self.fraud_rate_cur,
            "volume_ref":     self.volume_ref,
            "volume_cur":     self.volume_cur,
            "top_drifted_features": dict(
                sorted(self.feature_psi.items(), key=lambda x: x[1], reverse=True)[:5]
            ),
        }

    def print_report(self):
        """Print a rich formatted drift report."""
        # ── Overall status banner ─────────────────────────────────────────────
        colour = {"stable": "green", "monitor": "yellow", "retrain": "red"}[self.status]
        console.print(f"\n[bold {colour}]Fraud Model Drift Report — {self.status.upper()}[/bold {colour}]")
        console.print(f"[dim]Overall PSI: {self.overall_psi:.4f} (threshold: stable<0.10, monitor<0.20, retrain≥0.20)[/dim]\n")

        # ── Key metrics comparison ────────────────────────────────────────────
        t = Table(show_header=True, box=None, padding=(0, 2))
        t.add_column("Metric",         style="dim",   width=28)
        t.add_column("Reference",      style="cyan",  width=15, justify="right")
        t.add_column("Current",        style="white", width=15, justify="right")
        t.add_column("Change",         style="white", width=12, justify="right")

        fraud_delta = self.fraud_rate_cur - self.fraud_rate_ref
        vol_delta   = ((self.volume_cur - self.volume_ref) / max(self.volume_ref, 1)) * 100
        fd_colour   = "red" if abs(fraud_delta) > 0.5 else "green"
        vd_colour   = "red" if abs(vol_delta) > 20 else "green"

        t.add_row(
            "Transaction Volume",
            f"{self.volume_ref:,}",
            f"{self.volume_cur:,}",
            f"[{vd_colour}]{vol_delta:+.1f}%[/{vd_colour}]",
        )
        t.add_row(
            "Fraud Rate",
            f"{self.fraud_rate_ref:.3f}%",
            f"{self.fraud_rate_cur:.3f}%",
            f"[{fd_colour}]{fraud_delta:+.3f}%[/{fd_colour}]",
        )
        t.add_row(
            "Overall PSI",
            "—",
            f"{self.overall_psi:.4f}",
            f"[{colour}]{self.status}[/{colour}]",
        )
        console.print(t)

        # ── Per-feature PSI table ─────────────────────────────────────────────
        console.print("\n[dim]Per-Feature PSI Scores:[/dim]")
        ft = Table(show_header=True, box=None, padding=(0, 2))
        ft.add_column("Feature",   style="cyan",  width=30)
        ft.add_column("PSI",       style="white", width=10, justify="right")
        ft.add_column("Status",    style="white", width=12)

        for feat, psi in sorted(self.feature_psi.items(), key=lambda x: x[1], reverse=True):
            if psi >= PSI_RETRAIN:
                s = "[red]retrain[/red]"
            elif psi >= PSI_STABLE:
                s = "[yellow]monitor[/yellow]"
            else:
                s = "[green]stable[/green]"
            ft.add_row(feat, f"{psi:.4f}", s)
        console.print(ft)

        # ── Recommendations ───────────────────────────────────────────────────
        if self.recommendations:
            console.print("\n[bold]Recommendations:[/bold]")
            for r in self.recommendations:
                console.print(f"  • {r}")
        console.print()

    def to_json(self, path: Optional[str] = None) -> str:
        """Export report as JSON."""
        data = {
            **self.summary(),
            "feature_psi":    self.feature_psi,
            "recommendations": self.recommendations,
        }
        out = json.dumps(data, indent=2)
        if path:
            with open(path, "w") as f:
                f.write(out)
        return out


class DriftDetector:
    """
    Compares two FraudAccessor instances to measure model drift.

    Computes PSI for every feature used in fraud detection,
    monitors fraud rate changes, and gives actionable recommendations.
    """

    def __init__(
        self,
        reference_df: pd.DataFrame,
        current_df:   pd.DataFrame,
    ):
        """
        Args:
            reference_df: Normalised transactions from the reference period
                          (e.g. last month's data that the model was trained on)
            current_df:   Normalised transactions from the current period
                          (e.g. this month's data to check for drift)
        """
        self._ref = reference_df
        self._cur = current_df

    def detect(self) -> DriftReport:
        """Run full drift detection and return a DriftReport."""
        ref_feats = self._build_features(self._ref)
        cur_feats = self._build_features(self._cur)

        # ── PSI per feature ───────────────────────────────────────────────────
        common_cols = [c for c in ref_feats.columns if c in cur_feats.columns]
        feature_psi = {}
        for col in common_cols:
            try:
                psi = _psi_score(
                    ref_feats[col].dropna().values,
                    cur_feats[col].dropna().values,
                )
                feature_psi[col] = psi
            except Exception:
                feature_psi[col] = 0.0

        # ── Overall PSI — weighted average ────────────────────────────────────
        overall_psi = float(np.mean(list(feature_psi.values()))) if feature_psi else 0.0

        # ── Fraud rate comparison ─────────────────────────────────────────────
        fraud_col_ref = "is_flagged_fraud" if "is_flagged_fraud" in self._ref.columns else "is_fraud"
        fraud_col_cur = "is_flagged_fraud" if "is_flagged_fraud" in self._cur.columns else "is_fraud"

        fraud_rate_ref = float(
            self._ref[fraud_col_ref].astype(float).mean() * 100
            if fraud_col_ref in self._ref.columns else 0.0
        )
        fraud_rate_cur = float(
            self._cur[fraud_col_cur].astype(float).mean() * 100
            if fraud_col_cur in self._cur.columns else 0.0
        )

        # ── Status classification ─────────────────────────────────────────────
        if overall_psi >= PSI_RETRAIN:
            status = "retrain"
        elif overall_psi >= PSI_STABLE:
            status = "monitor"
        else:
            status = "stable"

        should_retrain = (
            overall_psi >= PSI_RETRAIN
            or abs(fraud_rate_cur - fraud_rate_ref) > 1.0
        )

        # ── Generate recommendations ──────────────────────────────────────────
        recommendations = []

        if overall_psi >= PSI_RETRAIN:
            recommendations.append(
                f"PSI {overall_psi:.3f} exceeds 0.20 threshold — retrain model immediately."
            )
        elif overall_psi >= PSI_STABLE:
            recommendations.append(
                f"PSI {overall_psi:.3f} in warning zone (0.10–0.20) — schedule retraining soon."
            )
        else:
            recommendations.append(
                f"PSI {overall_psi:.3f} is stable — no retraining needed."
            )

        # Highlight most drifted features
        top_drifted = sorted(feature_psi.items(), key=lambda x: x[1], reverse=True)[:3]
        for feat, psi in top_drifted:
            if psi >= PSI_RETRAIN:
                recommendations.append(
                    f"Feature '{feat}' has high drift (PSI={psi:.3f}) — "
                    "investigate distribution change."
                )

        # Fraud rate spike
        fraud_delta = fraud_rate_cur - fraud_rate_ref
        if fraud_delta > 1.0:
            recommendations.append(
                f"Fraud rate increased {fraud_delta:+.2f}% — potential new attack pattern. "
                "Review recent flagged transactions immediately."
            )
        elif fraud_delta < -1.0:
            recommendations.append(
                f"Fraud rate dropped {fraud_delta:+.2f}% — possible model under-detecting. "
                "Verify false negative rate hasn't increased."
            )

        # Volume change
        vol_change = (len(self._cur) - len(self._ref)) / max(len(self._ref), 1) * 100
        if abs(vol_change) > 30:
            recommendations.append(
                f"Transaction volume changed {vol_change:+.0f}% — "
                "significant volume shift can distort PSI scores."
            )

        return DriftReport(
            overall_psi    = round(overall_psi, 4),
            feature_psi    = {k: round(v, 4) for k, v in feature_psi.items()},
            fraud_rate_ref = round(fraud_rate_ref, 3),
            fraud_rate_cur = round(fraud_rate_cur, 3),
            volume_ref     = len(self._ref),
            volume_cur     = len(self._cur),
            status         = status,
            should_retrain = should_retrain,
            recommendations= recommendations,
        )

    def _build_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Build the same feature set used in fraud detection."""
        out = pd.DataFrame(index=df.index)

        out["amount"] = pd.to_numeric(df.get("amount", pd.Series([0.0] * len(df))), errors="coerce").fillna(0)
        out["amount_log"] = np.log1p(out["amount"])

        if "transaction_at" in df.columns:
            ts = pd.to_datetime(df["transaction_at"], errors="coerce")
            out["hour"]        = ts.dt.hour.fillna(12)
            out["day_of_week"] = ts.dt.dayofweek.fillna(0)
            out["is_weekend"]  = out["day_of_week"].isin([5, 6]).astype(float)
            out["is_night"]    = out["hour"].apply(lambda h: 1.0 if h < 6 or h > 22 else 0.0)
        else:
            out["hour"] = out["day_of_week"] = out["is_weekend"] = out["is_night"] = 0.0

        # Customer velocity
        if "customer_id" in df.columns:
            out["customer_txn_count"] = df.groupby("customer_id").cumcount() + 1
        else:
            out["customer_txn_count"] = 1.0

        # Merchant category encoded
        from sklearn.preprocessing import LabelEncoder
        le = LabelEncoder()
        if "merchant_category" in df.columns:
            out["merchant_category_enc"] = le.fit_transform(
                df["merchant_category"].fillna("Other").astype(str)
            ).astype(float)
        else:
            out["merchant_category_enc"] = 0.0

        return out.fillna(0)
