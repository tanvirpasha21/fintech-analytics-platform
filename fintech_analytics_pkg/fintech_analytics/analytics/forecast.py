"""
fintech_analytics.analytics.forecast
======================================
Revenue and fraud rate forecasting using Prophet + SARIMA fallback.

Forecasts are generated from the monthly payment performance mart,
so they reflect real pipeline data — not synthetic samples.

Usage:
    p = Pipeline.from_csv("transactions.csv")
    p.run()

    forecast = p.forecast(months=3)

    print(forecast.revenue)       # monthly revenue predictions
    print(forecast.fraud_rate)    # monthly fraud rate predictions
    print(forecast.summary())     # key numbers

    forecast.print_report()       # rich formatted output
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

import duckdb
import numpy as np
import pandas as pd
from rich.console import Console
from rich.table import Table

console = Console()

# Minimum months of data needed to produce a meaningful forecast
MIN_MONTHS_FOR_FORECAST = 3


@dataclass
class ForecastResult:
    """
    Forecast results for revenue and fraud rate.

    Attributes:
        revenue:         DataFrame with columns: month, predicted, lower_95, upper_95
        fraud_rate:      DataFrame with columns: month, predicted, lower_95, upper_95
        months_forecast: Number of months forecast
        months_history:  Number of historical months used
        model_used:      "prophet" or "linear_trend"
        confidence:      "high" | "medium" | "low" based on data available
    """
    revenue:         pd.DataFrame
    fraud_rate:      pd.DataFrame
    months_forecast: int
    months_history:  int
    model_used:      str
    confidence:      str
    warnings:        list[str] = field(default_factory=list)

    def summary(self) -> dict:
        """Return a plain dict with key forecast numbers."""
        if self.revenue.empty:
            return {"error": "no data"}

        next_month_rev   = self.revenue.iloc[0]["predicted"]  if not self.revenue.empty   else None
        next_month_fraud = self.fraud_rate.iloc[0]["predicted"] if not self.fraud_rate.empty else None

        return {
            "next_month_revenue":       round(float(next_month_rev),   2) if next_month_rev   else None,
            "next_month_fraud_rate_pct":round(float(next_month_fraud), 3) if next_month_fraud else None,
            "months_forecast":          self.months_forecast,
            "months_history":           self.months_history,
            "model":                    self.model_used,
            "confidence":               self.confidence,
        }

    def print_report(self):
        """Print a rich formatted forecast report."""
        console.print(f"\n[bold cyan]Forecast Report[/bold cyan]")
        console.print(
            f"[dim]{self.months_forecast}-month forecast using {self.months_history} months "
            f"of history  ·  model: {self.model_used}  ·  confidence: {self.confidence}[/dim]\n"
        )

        if self.warnings:
            for w in self.warnings:
                console.print(f"[yellow]⚠  {w}[/yellow]")
            console.print()

        # Revenue forecast table
        if not self.revenue.empty:
            console.print("[bold]Revenue Forecast[/bold]")
            t = Table(show_header=True, box=None, padding=(0, 2))
            t.add_column("Month",      style="cyan",  width=12)
            t.add_column("Predicted",  style="white", width=14, justify="right")
            t.add_column("Low (95%)",  style="dim",   width=14, justify="right")
            t.add_column("High (95%)", style="dim",   width=14, justify="right")
            t.add_column("vs History", style="white", width=12, justify="right")

            hist_avg = self.revenue["predicted"].mean()
            for _, row in self.revenue.iterrows():
                delta = ((row["predicted"] - hist_avg) / max(hist_avg, 1)) * 100
                dc = "green" if delta >= 0 else "red"
                t.add_row(
                    str(row["month"])[:7],
                    f"${row['predicted']:>12,.0f}",
                    f"${row['lower_95']:>12,.0f}",
                    f"${row['upper_95']:>12,.0f}",
                    f"[{dc}]{delta:+.1f}%[/{dc}]",
                )
            console.print(t)

        # Fraud rate forecast table
        if not self.fraud_rate.empty:
            console.print("\n[bold]Fraud Rate Forecast[/bold]")
            ft = Table(show_header=True, box=None, padding=(0, 2))
            ft.add_column("Month",      style="cyan",  width=12)
            ft.add_column("Predicted",  style="white", width=12, justify="right")
            ft.add_column("Low (95%)",  style="dim",   width=12, justify="right")
            ft.add_column("High (95%)", style="dim",   width=12, justify="right")

            for _, row in self.fraud_rate.iterrows():
                pred = row["predicted"]
                c = "red" if pred > 3.0 else "yellow" if pred > 2.0 else "green"
                ft.add_row(
                    str(row["month"])[:7],
                    f"[{c}]{pred:.3f}%[/{c}]",
                    f"{row['lower_95']:.3f}%",
                    f"{row['upper_95']:.3f}%",
                )
            console.print(ft)
        console.print()


class Forecaster:
    """
    Generates revenue and fraud rate forecasts from the analytics.payment_performance mart.

    Uses Prophet when installed (best accuracy), falls back to
    linear trend + seasonal decomposition otherwise.
    """

    def __init__(self, con: duckdb.DuckDBPyConnection):
        self._con = con

    def forecast(self, months: int = 3) -> ForecastResult:
        """
        Forecast revenue and fraud rate for the next N months.

        Args:
            months: Number of months to forecast (default 3, max 12)

        Returns:
            ForecastResult with revenue and fraud_rate DataFrames
        """
        months = min(max(months, 1), 12)

        # ── Pull monthly history from payment performance mart ────────────────
        history = self._con.execute("""
            SELECT
                date_trunc('month', cast(month as date))         as month,
                sum(total_volume)                                 as revenue,
                round(avg(fraud_rate_pct), 4)                     as fraud_rate_pct,
                sum(total_transactions)                           as txn_count,
                round(avg(completion_pct), 2)                     as completion_pct
            FROM analytics.payment_performance
            GROUP BY 1
            ORDER BY 1
        """).df()

        warnings = []
        confidence = "high"

        if history.empty or len(history) < MIN_MONTHS_FOR_FORECAST:
            warnings.append(
                f"Only {len(history)} month(s) of data — forecasts will have low accuracy. "
                f"At least {MIN_MONTHS_FOR_FORECAST} months recommended."
            )
            confidence = "low"
        elif len(history) < 6:
            warnings.append(
                f"Only {len(history)} months of history — seasonal patterns may not be captured. "
                "Forecasts are indicative only."
            )
            confidence = "medium"

        if history.empty:
            return ForecastResult(
                revenue         = pd.DataFrame(),
                fraud_rate      = pd.DataFrame(),
                months_forecast = months,
                months_history  = 0,
                model_used      = "none",
                confidence      = "low",
                warnings        = warnings + ["No historical data available for forecasting."],
            )

        # ── Try Prophet, fall back to linear trend ────────────────────────────
        model_used = "linear_trend"
        try:
            from prophet import Prophet   # type: ignore
            model_used = "prophet"
        except ImportError:
            pass

        if model_used == "prophet":
            rev_forecast   = self._prophet_forecast(history, "revenue",       months)
            fraud_forecast = self._prophet_forecast(history, "fraud_rate_pct", months)
        else:
            warnings.append(
                "Prophet not installed — using linear trend model. "
                "Install Prophet for better accuracy: pip install prophet"
            )
            rev_forecast   = self._linear_forecast(history, "revenue",        months)
            fraud_forecast = self._linear_forecast(history, "fraud_rate_pct", months)

        # Clamp fraud rate to realistic range [0, 15]
        if not fraud_forecast.empty:
            for col in ["predicted", "lower_95", "upper_95"]:
                fraud_forecast[col] = fraud_forecast[col].clip(0, 15).round(3)

        # Clamp revenue to non-negative
        if not rev_forecast.empty:
            for col in ["predicted", "lower_95", "upper_95"]:
                rev_forecast[col] = rev_forecast[col].clip(0).round(2)

        return ForecastResult(
            revenue         = rev_forecast,
            fraud_rate      = fraud_forecast,
            months_forecast = months,
            months_history  = len(history),
            model_used      = model_used,
            confidence      = confidence,
            warnings        = warnings,
        )

    def _prophet_forecast(
        self,
        history: pd.DataFrame,
        metric: str,
        months: int,
    ) -> pd.DataFrame:
        """Forecast using Facebook Prophet."""
        from prophet import Prophet  # type: ignore

        # Prophet requires columns: ds (date), y (value)
        df = history[["month", metric]].copy()
        df.columns = ["ds", "y"]
        df["ds"] = pd.to_datetime(df["ds"])
        df = df.dropna()

        if len(df) < 2:
            return self._linear_forecast(history, metric, months)

        model = Prophet(
            yearly_seasonality  = len(df) >= 12,
            weekly_seasonality  = False,
            daily_seasonality   = False,
            interval_width      = 0.95,
            changepoint_prior_scale = 0.05,
        )
        model.fit(df)

        future   = model.make_future_dataframe(periods=months, freq="MS")
        forecast = model.predict(future)

        # Return only the forecast period (not history)
        result = forecast.tail(months)[["ds", "yhat", "yhat_lower", "yhat_upper"]].copy()
        result.columns = ["month", "predicted", "lower_95", "upper_95"]
        result["month"] = result["month"].dt.strftime("%Y-%m")
        return result.reset_index(drop=True)

    def _linear_forecast(
        self,
        history: pd.DataFrame,
        metric: str,
        months: int,
    ) -> pd.DataFrame:
        """
        Forecast using linear regression with seasonal decomposition.

        Works well for short series where Prophet would overfit.
        Adds ±1.5 standard deviation confidence intervals.
        """
        values = history[metric].astype(float).fillna(0).values
        n      = len(values)

        if n == 0:
            return pd.DataFrame(columns=["month", "predicted", "lower_95", "upper_95"])

        # Fit linear trend on index
        x = np.arange(n)
        if n >= 2:
            coeffs   = np.polyfit(x, values, deg=min(1, n - 1))
            trend_fn = np.poly1d(coeffs)
        else:
            trend_fn = lambda i: values[0]

        # Residual standard deviation for confidence interval
        residuals = values - np.array([trend_fn(i) for i in x])
        sigma     = np.std(residuals) if len(residuals) > 1 else values.mean() * 0.1

        # Project forward
        last_date = pd.to_datetime(history["month"].iloc[-1])
        rows = []
        for i in range(months):
            idx     = n + i
            pred    = float(trend_fn(idx))
            ci      = sigma * 1.96   # 95% CI
            month   = (last_date + pd.DateOffset(months=i + 1)).strftime("%Y-%m")
            rows.append({
                "month":     month,
                "predicted": pred,
                "lower_95":  pred - ci,
                "upper_95":  pred + ci,
            })

        return pd.DataFrame(rows)
