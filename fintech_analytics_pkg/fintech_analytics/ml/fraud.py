"""
fintech_analytics.ml.fraud
============================
Fraud detection with XGBoost + SHAP explainability.

What makes this different from other fraud libraries:
- Explains WHY each transaction is flagged in plain English
- No training labels required (uses Isolation Forest for unsupervised detection)
- Supervised mode available when fraud labels exist in the data
- Per-transaction explanation with human-readable reasons
"""

from __future__ import annotations

from typing import Optional
import pandas as pd
import numpy as np
import duckdb
from rich.console import Console
from rich.table import Table

console = Console()


class FraudAccessor:
    """Fraud detection and explainability."""

    def __init__(self, con: duckdb.DuckDBPyConnection, df: pd.DataFrame):
        self._con    = con
        self._df     = df
        self._model  = None
        self._fitted = False
        self._mode   = None   # "supervised" or "unsupervised"

    def detect(
        self,
        mode: str = "auto",
        contamination: float = 0.02,
        threshold: float = 0.5,
    ) -> pd.DataFrame:
        """
        Detect fraudulent transactions.

        Args:
            mode:          "auto" = use supervised if labels exist, else unsupervised
                           "supervised"   = XGBoost (requires is_fraud labels)
                           "unsupervised" = Isolation Forest (no labels needed)
            contamination: Expected fraud rate for Isolation Forest (default 2%)
            threshold:     Classification threshold for supervised mode (default 0.5)

        Returns:
            DataFrame with columns:
                transaction_id, fraud_probability, risk_level,
                is_predicted_fraud, [top_reason if explain was called]
        """
        features = self._build_features()

        has_labels = (
            "is_fraud" in self._df.columns
            and self._df["is_fraud"].sum() > 0
        )

        if mode == "auto":
            mode = "supervised" if has_labels else "unsupervised"

        self._mode = mode

        if mode == "supervised":
            return self._supervised(features, threshold)
        else:
            return self._unsupervised(features, contamination)

    def _build_features(self) -> pd.DataFrame:
        """Build feature matrix from normalised transactions."""
        df = self._df.copy()

        # Time features
        df["transaction_at"] = pd.to_datetime(df["transaction_at"], errors="coerce")
        df["hour"]        = df["transaction_at"].dt.hour
        df["day_of_week"] = df["transaction_at"].dt.dayofweek
        df["is_weekend"]  = df["day_of_week"].isin([5, 6]).astype(int)
        df["is_night"]    = df["hour"].apply(lambda h: 1 if h < 6 or h > 22 else 0)

        # Amount features
        df["amount_log"]  = np.log1p(df["amount"].fillna(0))
        df["amount"]      = df["amount"].fillna(0)

        # Customer velocity — transactions per customer in rolling window
        df = df.sort_values("transaction_at")
        df["customer_txn_count"] = df.groupby("customer_id").cumcount() + 1

        # Merchant risk — encode category
        from sklearn.preprocessing import LabelEncoder
        le = LabelEncoder()
        df["merchant_category_enc"] = le.fit_transform(
            df["merchant_category"].fillna("Unknown").astype(str)
        )
        df["channel_enc"] = le.fit_transform(
            df.get("channel", pd.Series(["unknown"] * len(df))).fillna("unknown").astype(str)
        )

        feature_cols = [
            "amount", "amount_log", "hour", "day_of_week",
            "is_weekend", "is_night", "customer_txn_count",
            "merchant_category_enc", "channel_enc",
        ]

        # Fill missing
        features = df[feature_cols].fillna(0)
        features.index = df.index
        return features

    def _supervised(self, features: pd.DataFrame, threshold: float) -> pd.DataFrame:
        """XGBoost supervised fraud detection."""
        try:
            from xgboost import XGBClassifier
        except ImportError:
            console.print("[yellow]xgboost not installed — falling back to unsupervised[/yellow]")
            return self._unsupervised(features, contamination=0.02)

        from sklearn.model_selection import train_test_split

        y = self._df["is_fraud"].astype(int).fillna(0)
        X = features

        # Simple train/predict on full dataset for scoring purposes
        # In production you'd use a pre-trained model
        scale_pos_weight = max((y == 0).sum() / max((y == 1).sum(), 1), 1)
        model = XGBClassifier(
            n_estimators=100,
            max_depth=4,
            learning_rate=0.1,
            scale_pos_weight=scale_pos_weight,
            eval_metric="logloss",
            verbosity=0,
        )
        model.fit(X, y)
        self._model  = model
        self._fitted = True

        proba = model.predict_proba(X)[:, 1]

        return pd.DataFrame({
            "transaction_id":     self._df.get("transaction_id", pd.Series(range(len(self._df)))).values,
            "amount":             self._df["amount"].values,
            "fraud_probability":  np.round(proba, 4),
            "risk_level":         pd.cut(
                proba,
                bins=[0, 0.3, 0.6, 0.8, 1.0],
                labels=["LOW", "MEDIUM", "HIGH", "CRITICAL"],
                include_lowest=True,
            ),
            "is_predicted_fraud": proba >= threshold,
            "actual_label":       y.values,
        })

    def _unsupervised(self, features: pd.DataFrame, contamination: float) -> pd.DataFrame:
        """Isolation Forest unsupervised anomaly detection."""
        from sklearn.ensemble import IsolationForest

        model = IsolationForest(
            contamination=contamination,
            random_state=42,
            n_estimators=100,
        )
        scores = model.fit_predict(features)
        # Convert to probability-like score (higher = more anomalous)
        raw_scores    = model.decision_function(features)
        normalised    = 1 - (raw_scores - raw_scores.min()) / (raw_scores.max() - raw_scores.min() + 1e-9)

        self._model  = model
        self._fitted = True

        return pd.DataFrame({
            "transaction_id":    self._df.get("transaction_id", pd.Series(range(len(self._df)))).values,
            "amount":            self._df["amount"].values,
            "fraud_probability": np.round(normalised, 4),
            "risk_level":        pd.cut(
                normalised,
                bins=[0, 0.3, 0.6, 0.8, 1.0],
                labels=["LOW", "MEDIUM", "HIGH", "CRITICAL"],
                include_lowest=True,
            ),
            "is_predicted_fraud": scores == -1,
        })

    def explain(self, transaction_id: str) -> dict:
        """
        Explain why a specific transaction was flagged.

        Returns human-readable reasons in plain English.
        This is the key differentiator — not just a score, but WHY.

        Args:
            transaction_id: The transaction to explain

        Returns:
            dict with fraud_probability, risk_level, and plain-English reasons
        """
        if not self._fitted:
            raise RuntimeError("Call fraud.detect() before calling fraud.explain()")

        # Find the transaction
        mask = self._df.get("transaction_id", pd.Series()).astype(str) == str(transaction_id)
        if not mask.any():
            raise ValueError(f"Transaction '{transaction_id}' not found")

        txn = self._df[mask].iloc[0]

        # Build explanation using rule-based signals
        reasons = []
        risk_signals = []

        # Amount analysis
        avg_amount = self._df["amount"].median()
        if txn["amount"] > avg_amount * 3:
            pct_above = int((txn["amount"] / avg_amount - 1) * 100)
            reasons.append(f"Amount ${txn['amount']:.2f} is {pct_above}% above median transaction")
            risk_signals.append("HIGH_AMOUNT")

        # Time analysis
        txn_time = pd.to_datetime(txn["transaction_at"], errors="coerce")
        if txn_time is not pd.NaT:
            hour = txn_time.hour
            if hour < 4 or hour > 23:
                reasons.append(f"Transaction at {hour:02d}:xx — unusual hour (high fraud window)")
                risk_signals.append("ODD_HOURS")

        # Merchant category risk
        high_risk_cats = ["Transfer", "ATM", "Crypto", "Wire Transfer"]
        cat = str(txn.get("merchant_category", ""))
        if any(h.lower() in cat.lower() for h in high_risk_cats):
            reasons.append(f"Merchant category '{cat}' has elevated fraud rate")
            risk_signals.append("HIGH_RISK_MERCHANT_CATEGORY")

        # Country mismatch
        ip_country = str(txn.get("country", ""))
        currency   = str(txn.get("currency", ""))
        if ip_country and currency:
            currency_country_map = {"GBP": "GB", "USD": "US", "EUR": "EU"}
            expected = currency_country_map.get(currency, "")
            if expected and ip_country not in [expected, ""] and len(ip_country) == 2:
                reasons.append(
                    f"Transaction origin '{ip_country}' doesn't match "
                    f"account currency '{currency}' (expected {expected})"
                )
                risk_signals.append("CROSS_BORDER_MISMATCH")

        # Customer velocity — how many transactions today
        if "transaction_at" in self._df.columns:
            txn_date = pd.to_datetime(txn["transaction_at"], errors="coerce")
            if txn_date is not pd.NaT and "customer_id" in self._df.columns:
                same_customer = self._df[
                    self._df["customer_id"].astype(str) == str(txn.get("customer_id", ""))
                ]
                same_day = same_customer[
                    pd.to_datetime(same_customer["transaction_at"], errors="coerce").dt.date
                    == txn_date.date()
                ]
                if len(same_day) > 5:
                    reasons.append(
                        f"Customer had {len(same_day)} transactions on this date — velocity flag"
                    )
                    risk_signals.append("HIGH_VELOCITY")

        # SHAP explanation if supervised model available
        if self._mode == "supervised" and self._model is not None:
            try:
                import shap
                features = self._build_features()
                txn_idx  = self._df[mask].index[0]
                explainer = shap.TreeExplainer(self._model)
                shap_values = explainer.shap_values(features.loc[[txn_idx]])
                feature_names = features.columns.tolist()
                shap_pairs = sorted(
                    zip(feature_names, shap_values[0]),
                    key=lambda x: abs(x[1]),
                    reverse=True,
                )[:3]
                for feat, val in shap_pairs:
                    direction = "increases" if val > 0 else "decreases"
                    reasons.append(f"Feature '{feat}' {direction} fraud probability by {abs(val):.3f}")
            except Exception:
                pass  # SHAP optional — degrade gracefully

        if not reasons:
            reasons = ["No specific risk signals detected — flagged by anomaly model"]

        # Compute fraud probability for this transaction
        features      = self._build_features()
        txn_idx       = self._df[mask].index[0]
        if self._mode == "supervised":
            proba = float(self._model.predict_proba(features.loc[[txn_idx]])[:, 1][0])
        else:
            raw   = self._model.decision_function(features.loc[[txn_idx]])[0]
            all_  = self._model.decision_function(features)
            proba = float(1 - (raw - all_.min()) / (all_.max() - all_.min() + 1e-9))

        risk_level = (
            "CRITICAL" if proba > 0.8
            else "HIGH" if proba > 0.6
            else "MEDIUM" if proba > 0.3
            else "LOW"
        )

        recommended = {
            "CRITICAL": "Block immediately and request manual review",
            "HIGH":     "Block and send SCA re-authentication challenge",
            "MEDIUM":   "Flag for review — allow with monitoring",
            "LOW":      "Allow — no action required",
        }

        return {
            "transaction_id":    transaction_id,
            "amount":            float(txn["amount"]),
            "merchant":          str(txn.get("merchant_name", "Unknown")),
            "fraud_probability": round(proba, 4),
            "risk_level":        risk_level,
            "risk_signals":      risk_signals,
            "reasons":           reasons,
            "recommended_action":recommended[risk_level],
            "detection_mode":    self._mode,
        }

    def print_explain(self, transaction_id: str):
        """Print a rich fraud explanation for a transaction."""
        result = self.explain(transaction_id)
        console.print(f"\n[bold]Fraud Explanation — {transaction_id}[/bold]")
        console.print(f"  Amount:      ${result['amount']:,.2f}")
        console.print(f"  Merchant:    {result['merchant']}")
        console.print(f"  Probability: [red]{result['fraud_probability']:.1%}[/red]")
        console.print(f"  Risk Level:  [red]{result['risk_level']}[/red]")
        console.print(f"\n  [bold]Reasons:[/bold]")
        for r in result["reasons"]:
            console.print(f"    • {r}")
        console.print(f"\n  [bold]Recommended Action:[/bold]")
        console.print(f"    {result['recommended_action']}\n")

    def summary(self) -> dict:
        """Quick fraud summary statistics."""
        results = self.detect()
        return {
            "total_transactions":     len(results),
            "predicted_fraud":        int(results["is_predicted_fraud"].sum()),
            "fraud_rate_pct":         round(results["is_predicted_fraud"].mean() * 100, 2),
            "critical_count":         int((results["risk_level"] == "CRITICAL").sum()),
            "high_count":             int((results["risk_level"] == "HIGH").sum()),
            "avg_fraud_probability":  round(results["fraud_probability"].mean(), 4),
        }
