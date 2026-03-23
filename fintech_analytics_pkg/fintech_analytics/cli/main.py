"""
fintech_analytics.cli.main
============================
Command-line interface for fintech_analytics.

Usage:
    fintech-analytics run --input transactions.csv
    fintech-analytics fraud --input transactions.csv --explain
    fintech-analytics rfm --input transactions.csv --export crm.csv
    fintech-analytics compliance --input transactions.csv
    fintech-analytics dashboard --input transactions.csv --port 8080
    fintech-analytics benchmark --input transactions.csv
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional
import typer
from rich.console import Console

app     = typer.Typer(
    name="fintech-analytics",
    help="The dbt-powered Python library for financial analytics.",
    add_completion=False,
)
console = Console()


def _load(input_path: Path, schema: Optional[str] = None) -> "Pipeline":
    from fintech_analytics import Pipeline
    overrides = {}
    if schema:
        for pair in schema.split(","):
            k, v = pair.strip().split("=")
            overrides[k.strip()] = v.strip()
    return Pipeline.from_csv(input_path, schema_mapping=overrides if overrides else None)


@app.command()
def run(
    input:   Path = typer.Option(..., "--input",  "-i", help="Path to CSV file"),
    output:  Path = typer.Option("fintech_output", "--output", "-o", help="Output directory"),
    engine:  str  = typer.Option("auto", "--engine", "-e",
             help="Pipeline engine: auto, dbt, native"),
    schema:  Optional[str] = typer.Option(None, "--schema", "-s",
             help="Column overrides: 'transaction_id=ref,amount=value'"),
    no_dashboard: bool = typer.Option(False, "--no-dashboard", help="Skip dashboard"),
):
    """Run the full analytics pipeline on a CSV file."""
    console.print(f"\n[bold cyan]fintech-analytics[/bold cyan] v0.1.0\n")
    if engine == "dbt":
        console.print("[cyan]Engine: dbt (real .sql models)[/cyan]")
    elif engine == "native":
        console.print("[dim]Engine: native (built-in SQL)[/dim]")
    else:
        console.print("[dim]Engine: auto (dbt if available, else native)[/dim]")
    p = _load(input, schema)
    p.run(engine=engine)
    p.export(output)
    if not no_dashboard:
        p.dashboard()


@app.command()
def fraud(
    input:    Path = typer.Option(..., "--input",  "-i", help="Path to CSV file"),
    explain:  Optional[str] = typer.Option(None, "--explain", "-e",
              help="Explain a specific transaction_id"),
    mode:     str = typer.Option("auto", "--mode", "-m",
              help="Detection mode: auto, supervised, unsupervised"),
    export:   Optional[Path] = typer.Option(None, "--export", help="Export results to CSV"),
    schema:   Optional[str]  = typer.Option(None, "--schema", "-s",
              help="Column overrides: 'transaction_id=ref,amount=value'"),
):
    """Detect fraud and explain flagged transactions."""
    p = _load(input, schema)
    p.run()

    if explain:
        p.fraud.print_explain(explain)
    else:
        results = p.fraud.detect(mode=mode)
        summary = p.fraud.summary()

        console.print(f"\n[bold]Fraud Detection Results[/bold]")
        console.print(f"  Total transactions:  {summary['total_transactions']:,}")
        console.print(f"  Predicted fraud:     {summary['predicted_fraud']:,}")
        console.print(f"  Fraud rate:          {summary['fraud_rate_pct']}%")
        console.print(f"  Critical risk:       {summary['critical_count']:,}")
        console.print(f"  High risk:           {summary['high_count']:,}")

        if export:
            results.to_csv(export, index=False)
            console.print(f"\n[green]✓[/green] Exported to {export}")


@app.command()
def rfm(
    input:   Path = typer.Option(..., "--input",  "-i", help="Path to CSV file"),
    export:  Optional[Path] = typer.Option(None, "--export", help="Export segments to CSV"),
    segment: Optional[str]  = typer.Option(None, "--segment",
             help="Filter to specific segment (e.g. 'At Risk')"),
    schema:  Optional[str]  = typer.Option(None, "--schema", "-s"),
):
    """Run RFM customer segmentation."""
    p = _load(input, schema)
    p.run()
    p.segment.print_summary()

    if segment:
        df = p.segment.get_segment(segment)
        console.print(f"\n[bold]{segment}[/bold] — {len(df):,} customers")
        console.print(df.head(20).to_string())

    if export:
        df_export = p.segment.rfm() if not segment else p.segment.get_segment(segment)
        df_export.to_csv(export, index=False)
        console.print(f"\n[green]✓[/green] Exported to {export}")


@app.command()
def compliance(
    input:      Path = typer.Option(..., "--input", "-i", help="Path to CSV file"),
    export:     Optional[Path] = typer.Option(None, "--export", help="Export flags to CSV"),
    threshold:  Optional[float] = typer.Option(None, "--threshold",
                help="AML structuring threshold (default 10000)"),
    schema:     Optional[str] = typer.Option(None, "--schema", "-s"),
):
    """Run AML compliance checks."""
    p = _load(input, schema)
    p.run()

    overrides = {}
    if threshold:
        overrides["structuring_threshold_gbp"] = threshold

    p.compliance.print_report(overrides if overrides else None)

    if export:
        flags = p.compliance.aml_flags(overrides if overrides else None)
        flags.to_csv(export, index=False)
        console.print(f"\n[green]✓[/green] Exported to {export}")


@app.command()
def dashboard(
    input:  Path = typer.Option(..., "--input", "-i", help="Path to CSV file"),
    port:   int  = typer.Option(8888, "--port",  "-p", help="Port to serve on"),
    schema: Optional[str] = typer.Option(None, "--schema", "-s"),
):
    """Serve an interactive analytics dashboard."""
    p = _load(input, schema)
    p.run()
    p.dashboard(port=port, open_browser=True)


@app.command()
def benchmark(
    input:    Path = typer.Option(..., "--input", "-i", help="Path to CSV file"),
    industry: str  = typer.Option("payments_global", "--industry",
              help="Industry benchmark: payments_global, payments_uk, ecommerce"),
    schema:   Optional[str] = typer.Option(None, "--schema", "-s"),
):
    """Compare your metrics against industry benchmarks."""
    p = _load(input, schema)
    p.run()

    # Industry benchmarks from published FCA / UK Finance reports
    BENCHMARKS = {
        "payments_global": {
            "completion_pct":  {"avg": 94.1, "top_quartile": 97.2, "label": "Completion Rate"},
            "fraud_rate_pct":  {"avg": 2.10,  "top_quartile": 0.80,  "label": "Fraud Rate"},
        },
        "payments_uk": {
            "completion_pct":  {"avg": 95.2, "top_quartile": 98.1, "label": "Completion Rate"},
            "fraud_rate_pct":  {"avg": 1.80,  "top_quartile": 0.60,  "label": "Fraud Rate"},
        },
        "ecommerce": {
            "completion_pct":  {"avg": 92.5, "top_quartile": 96.0, "label": "Completion Rate"},
            "fraud_rate_pct":  {"avg": 2.80,  "top_quartile": 1.20,  "label": "Fraud Rate"},
        },
    }

    bmarks = BENCHMARKS.get(industry, BENCHMARKS["payments_global"])
    m      = p.metrics

    from rich.table import Table
    t = Table(title=f"Benchmark vs {industry.replace('_',' ').title()}")
    t.add_column("Metric",        style="cyan")
    t.add_column("Your Value",    justify="right")
    t.add_column("Industry Avg",  justify="right")
    t.add_column("Top Quartile",  justify="right")
    t.add_column("Assessment",    style="white")

    for key, b in bmarks.items():
        your_val = m.get(key, 0)
        avg      = b["avg"]
        top_q    = b["top_quartile"]

        # Higher is better for completion, lower is better for fraud
        is_rate  = "fraud" in key
        if is_rate:
            if your_val <= top_q:   assessment = "[green]Top quartile ✓[/green]"
            elif your_val <= avg:   assessment = "[cyan]Above average[/cyan]"
            else:                   assessment = "[red]Below average ↑ improve[/red]"
        else:
            if your_val >= top_q:   assessment = "[green]Top quartile ✓[/green]"
            elif your_val >= avg:   assessment = "[cyan]Above average[/cyan]"
            else:                   assessment = "[red]Below average ↓ investigate[/red]"

        t.add_row(
            b["label"],
            f"{your_val:.1f}%",
            f"{avg:.1f}%",
            f"{top_q:.1f}%",
            assessment,
        )

    console.print("\n")
    console.print(t)


if __name__ == "__main__":
    app()
