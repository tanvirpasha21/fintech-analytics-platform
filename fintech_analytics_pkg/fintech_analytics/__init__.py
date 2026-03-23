"""
fintech_analytics
=================
The dbt-powered Python library for financial analytics.

Schema-aware · Compliance-ready · ML-native

Quick start:
    from fintech_analytics import Pipeline

    p = Pipeline.from_csv("transactions.csv")
    p.run()

    print(p.metrics)
    print(p.segment.rfm())
    print(p.fraud.detect())
    p.dashboard()
"""

from fintech_analytics.pipeline.core import Pipeline
from fintech_analytics.schema.detector import SchemaDetector
from fintech_analytics.schema.mapping import ColumnMapping

__version__ = "0.1.0"
__author__  = "MD Tanvir Anjum"
__email__   = "contact@voidstudio.tech"

__all__ = [
    "Pipeline",
    "SchemaDetector",
    "ColumnMapping",
]
