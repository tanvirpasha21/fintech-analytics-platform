# fintech-analytics

> The dbt-powered Python library for financial analytics — schema-aware, compliance-ready, and ML-native.

[![PyPI version](https://img.shields.io/pypi/v/fintech-analytics)](https://pypi.org/project/fintech-analytics/)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue)](https://python.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-green)](LICENSE)
[![Tests](https://github.com/tanvirpasha21/fintech-analytics-platform/actions/workflows/dbt_ci.yml/badge.svg)](https://github.com/tanvirpasha21/fintech-analytics-platform/actions)

From raw transactions to fraud scores, RFM segments, cohort retention, and regulatory compliance reports — in 5 lines.

---

## Install

```bash
pip install fintech-analytics
```

---

## Quick Start

```python
from fintech_analytics import Pipeline

# Load from CSV — schema auto-detected
p = Pipeline.from_csv("transactions.csv")
p.run()

# KPI summary
print(p.metrics)
# {
#   "total_transactions": 150000,
#   "unique_customers":   2000,
#   "total_volume":       10230000.0,
#   "completion_pct":     92.3,
#   "fraud_rate_pct":     1.96,
#   ...
# }

# RFM customer segmentation
segments = p.segment.rfm()
print(p.segment.summary())

# Fraud detection with explanations
p.fraud.detect()
p.fraud.print_explain("txn_12345")
# Fraud Explanation — txn_12345
#   Amount:      $4,520.00
#   Probability: 87.3%
#   Risk Level:  HIGH
#   Reasons:
#     • Amount is 340% above median transaction
#     • Merchant category 'Transfer' has elevated fraud rate
#     • IP country 'NG' doesn't match account currency 'GBP'
#   Recommended Action: Block and send SCA re-authentication challenge

# AML compliance checks
p.compliance.print_report()

# Cohort retention matrix
print(p.cohorts.retention_matrix())

# Merchant risk scorecard
print(p.merchants.risk_scorecard())

# Interactive dashboard (opens in browser)
p.dashboard()

# Export all results to CSV
p.export("results/")
```

---

## Schema Auto-Detection

Works with any column naming convention — no renaming required:

```python
# Your CSV might have columns named anything
# trans_amt, ref_number, date, result, description...
p = Pipeline.from_csv("transactions.csv")
# fintech-analytics detects and maps them automatically

# Or override specific columns
p = Pipeline.from_csv(
    "transactions.csv",
    schema_mapping={
        "transaction_id": "ref_number",
        "amount":         "trans_amt",
        "transaction_at": "date",
    }
)
```

---

## Kaggle Integration

```python
# Download and analyse a Kaggle dataset directly
p = Pipeline.from_kaggle("mlg-ulb/creditcardfraud")
p.run()
print(p.fraud.detect(mode="supervised").head())
```

Requires: `pip install fintech-analytics[kaggle]` and `~/.kaggle/kaggle.json`

---

## Command Line Interface

```bash
# Full pipeline
fintech-analytics run --input transactions.csv --output results/

# Fraud detection
fintech-analytics fraud --input transactions.csv
fintech-analytics fraud --input transactions.csv --explain txn_123

# RFM segmentation
fintech-analytics rfm --input transactions.csv
fintech-analytics rfm --input transactions.csv --export crm.csv --segment "At Risk"

# AML compliance
fintech-analytics compliance --input transactions.csv
fintech-analytics compliance --input transactions.csv --threshold 15000

# Dashboard
fintech-analytics dashboard --input transactions.csv --port 8080

# Benchmark against industry
fintech-analytics benchmark --input transactions.csv --industry payments_uk
```

---

## Analytics Reference

### `p.metrics` — KPI Summary
```python
{
    "total_transactions": int,
    "unique_customers":   int,
    "unique_merchants":   int,
    "total_volume":       float,
    "avg_transaction":    float,
    "completion_pct":     float,
    "fraud_rate_pct":     float,
    "date_from":          str,
    "date_to":            str,
}
```

### `p.segment` — Customer Segmentation
```python
p.segment.rfm()                    # Full RFM table
p.segment.summary()                # Per-segment summary
p.segment.champions                # Champions DataFrame
p.segment.at_risk                  # At Risk + Cannot Lose Them
p.segment.lost                     # Hibernating + Lost
p.segment.get_segment("Promising") # Any segment by name
p.segment.print_summary()          # Rich table output
```

### `p.fraud` — Fraud Detection
```python
p.fraud.detect()                          # All transactions scored
p.fraud.detect(mode="supervised")         # XGBoost (needs labels)
p.fraud.detect(mode="unsupervised")       # Isolation Forest
p.fraud.explain("txn_123")               # Plain-English explanation
p.fraud.print_explain("txn_123")         # Rich formatted output
p.fraud.summary()                         # Aggregate stats
```

### `p.cohorts` — Cohort Analytics
```python
p.cohorts.retention()              # Full retention table
p.cohorts.retention_matrix()       # Pivot table (heatmap-ready)
p.cohorts.best_cohort()            # Highest M3 retention cohort
p.cohorts.average_retention()      # Average across all cohorts
```

### `p.merchants` — Merchant Intelligence
```python
p.merchants.risk_scorecard()       # All merchants by risk
p.merchants.critical()             # Critical risk merchants
p.merchants.elevated()             # Elevated risk merchants
p.merchants.top_by_volume(10)      # Top 10 by volume
p.merchants.category_summary()     # Aggregated by category
p.merchants.velocity_alerts()      # Unusual transaction spikes
```

### `p.compliance` — AML Checks
```python
p.compliance.aml_flags()           # All AML flags as DataFrame
p.compliance.print_report()        # Rich formatted report

# AML checks performed:
# - Structuring (smurfing)         — transactions below reporting threshold
# - Velocity anomalies             — unusual frequency in time window
# - Round number bias              — manual fraud signal
# - Dormant account activity       — sudden activity on quiet accounts
# - Large cash transactions        — ATM/transfer above threshold
```

### `p.query()` — Custom SQL
```python
# Run any SQL against the pipeline database
p.query("SELECT * FROM analytics.rfm WHERE segment = 'Champions'")
p.query("SELECT * FROM raw.transactions WHERE amount > 5000")
```

---

## Supported Data Formats

| Format | Method |
|---|---|
| CSV file | `Pipeline.from_csv("file.csv")` |
| pandas DataFrame | `Pipeline.from_dataframe(df)` |
| Kaggle dataset | `Pipeline.from_kaggle("user/dataset")` |

---

## Optional Dependencies

```bash
# Kaggle integration
pip install fintech-analytics[kaggle]

# dbt model integration
pip install fintech-analytics[dbt]

# Kafka streaming
pip install fintech-analytics[streaming]

# Everything
pip install fintech-analytics[all]
```

---

## Architecture

```
fintech_analytics/
├── schema/
│   ├── detector.py     # fuzzy column name detection
│   └── mapping.py      # schema normalisation
├── pipeline/
│   ├── core.py         # Pipeline class — main entry point
│   └── dashboard.py    # interactive HTML dashboard server
├── analytics/
│   ├── segmentation.py # RFM segments
│   ├── cohorts.py      # cohort retention
│   └── merchants.py    # merchant risk scoring
├── ml/
│   └── fraud.py        # XGBoost + Isolation Forest + SHAP explainability
├── compliance/
│   └── checks.py       # AML / regulatory checks
└── cli/
    └── main.py         # typer CLI
```

---

## Contributing

```bash
git clone https://github.com/tanvirpasha21/fintech-analytics-platform
cd fintech-analytics-platform/fintech_analytics_pkg

pip install -e ".[dev]"
pytest tests/ -v
```

---

## License

MIT — free to use, modify, and distribute.

---

## Author

**MD Tanvir Anjum** — Founder, Void Studio
[linkedin.com/in/mdtanviranjum21](https://linkedin.com/in/mdtanviranjum21) · [voidstudio.tech](https://voidstudio.tech)
