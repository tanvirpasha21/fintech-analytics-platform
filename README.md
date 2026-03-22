# Fintech Analytics Platform

End-to-end data engineering portfolio — dbt · DuckDB · Apache Kafka · Apache Airflow · Python · SQL

![dbt CI](https://github.com/tanvirpasha21/fintech-analytics-platform/actions/workflows/dbt_ci.yml/badge.svg)

---

## What This Is

A production-grade data engineering platform modelled after a real UK fintech company processing payments across multiple currencies, channels, and merchant categories.

It demonstrates the full modern data stack — real-time streaming, batch transformation, data quality testing, orchestration, and an interactive analytics dashboard — built entirely with free, open-source tools.

---

## Architecture

```
                        STREAMING LAYER
 ┌──────────────────────────────────────────────────────────┐
 │                                                          │
 │   transaction_producer.py                                │
 │   (simulates live payment API)                           │
 │          │                                               │
 │          ▼                                               │
 │   Apache Kafka  (3 topics)                               │
 │     fintech.transactions   ← all payment events          │
 │     fintech.fraud_alerts   ← high risk events            │
 │     fintech.dlq            ← dead letter queue           │
 │          │                                               │
 │          ▼                                               │
 │   transaction_consumer.py                                │
 │   (micro-batch, manual offset commit)                    │
 │          │                                               │
 │          ▼                                               │
 │   fintech_stream.duckdb                                  │
 │   stream.raw_transactions                                │
 └──────────────────────────────────────────────────────────┘
                        │
                        │ merge_stream_data (hourly, Airflow)
                        ▼
                  BATCH LAYER
 ┌──────────────────────────────────────────────────────────┐
 │                                                          │
 │   fintech.duckdb  (main warehouse)                       │
 │   raw.transactions  ← batch + merged stream events       │
 │          │                                               │
 │          ▼                                               │
 │   Airflow DAG: fintech_pipeline  (every hour)            │
 │                                                          │
 │   check_kafka_health                                     │
 │       │                                                  │
 │   health_check  →  validate_raw_data                     │
 │       │                                                  │
 │   merge_stream_data  ← pulls Kafka events into warehouse │
 │       │                                                  │
 │   dbt staging  →  dbt intermediate                       │
 │       │                                                  │
 │   ┌───┴──────────┬──────────────┐                        │
 │   payments   customers       risk   (parallel)           │
 │   └───┬──────────┴──────────────┘                        │
 │       │                                                  │
 │   dbt tests (30)  →  export dashboard                    │
 │                                                          │
 │   Airflow DAG: streaming_monitor  (every 5 min)          │
 │   check_kafka_lag  →  stream_freshness  →  anomalies     │
 │                                                          │
 └──────────────────────────────────────────────────────────┘
```

---

## Stack

| Layer | Tool | Purpose |
|---|---|---|
| Warehouse | DuckDB | Columnar, embedded, zero config |
| Transformation | dbt Core | 13 models, 3-layer architecture |
| Streaming | Apache Kafka | Real-time transaction events |
| Orchestration | Apache Airflow | Scheduled pipeline + monitoring |
| Metadata DB | PostgreSQL | Airflow backend (enables parallel tasks) |
| Data Generation | Python + Faker | 150k synthetic transactions |
| Dashboard | Vanilla JS + Canvas API | Zero-dependency interactive charts |
| CI/CD | GitHub Actions | dbt build + test + syntax check on every push |

---

## Project Structure

```
fintech-analytics-platform/
│
├── scripts/
│   ├── generate_data.py              # seeds DuckDB with 150k transactions
│   └── export_dashboard.py           # regenerates dashboard.html from DuckDB
│
├── fintech_dbt/                      # dbt project
│   ├── models/
│   │   ├── staging/                  # 5 views — clean + type-cast source tables
│   │   │   ├── stg_customers.sql
│   │   │   ├── stg_merchants.sql
│   │   │   ├── stg_transactions.sql
│   │   │   ├── stg_fx_rates.sql
│   │   │   ├── stg_disputes.sql
│   │   │   └── sources.yml
│   │   ├── intermediate/             # 2 views — joins, FX conversion, enrichment
│   │   │   ├── int_transactions_enriched.sql
│   │   │   └── int_customer_transaction_summary.sql
│   │   └── marts/                    # 6 tables — business-facing
│   │       ├── payments/
│   │       │   └── fct_monthly_payment_performance.sql
│   │       ├── customers/
│   │       │   ├── dim_customers_360.sql
│   │       │   ├── fct_cohort_retention.sql
│   │       │   └── fct_rfm_segmentation.sql
│   │       └── risk/
│   │           ├── fct_fraud_events.sql          ← INCREMENTAL model
│   │           └── dim_merchant_risk_scorecard.sql
│   └── macros/
│       └── finance_utils.sql         # safe_divide, pct_of_total, convert_currency
│
├── streaming/
│   ├── producer/
│   │   └── transaction_producer.py   # Kafka producer — simulates live payment API
│   └── consumer/
│       └── transaction_consumer.py   # Kafka → DuckDB micro-batch consumer
│
├── airflow/
│   └── dags/
│       ├── fintech_pipeline.py       # hourly pipeline DAG (13 tasks)
│       └── streaming_monitor.py      # 5-min stream health + Kafka lag DAG
│
├── docker-compose.yml                # full stack: Kafka + Airflow + PostgreSQL
├── dashboard.html                    # interactive dashboard (zero dependencies)
└── requirements.txt
```

---

## dbt Models

| Model | Layer | Type | Description |
|---|---|---|---|
| stg_customers | Staging | View | Cleaned customer records, KYC flags |
| stg_merchants | Staging | View | Merchant + MCC codes |
| stg_transactions | Staging | View | Typed transactions with date parts and boolean flags |
| stg_fx_rates | Staging | View | Daily FX rates with inverse rate |
| stg_disputes | Staging | View | Customer dispute records |
| int_transactions_enriched | Intermediate | View | FX-normalised, joined with merchant + customer context, risk tier |
| int_customer_transaction_summary | Intermediate | View | Pre-aggregated per-customer stats (reused by multiple marts) |
| fct_monthly_payment_performance | Mart | Table | Monthly KPIs: TPV, completion rate, fraud rate by currency + channel |
| dim_customers_360 | Mart | Table | LTV tier, churn risk segment, engagement score, dispute summary |
| fct_cohort_retention | Mart | Table | M0–M12 monthly cohort retention matrix |
| fct_rfm_segmentation | Mart | Table | RFM scores + 12 named segments + recommended actions |
| fct_fraud_events | Mart | **Incremental** | Fraud events with dispute match + 30-day velocity window |
| dim_merchant_risk_scorecard | Mart | Table | Composite risk score, fraud rate, dispute rate per merchant |

---

## Kafka Topics

| Topic | Partitions | Retention | Content |
|---|---|---|---|
| `fintech.transactions` | 3 | 7 days | All payment events, partitioned by customer_id |
| `fintech.fraud_alerts` | 3 | 30 days | High risk transaction alerts |
| `fintech.dlq` | 1 | 30 days | Dead letter queue — failed / malformed messages |

---

## Airflow DAGs

### `fintech_pipeline` — runs every hour

```
check_kafka_health          verify broker reachable, topics exist, consumer active
      │
health_check                verify DuckDB has data
      │
validate_raw_data           check for nulls, bad statuses, negative amounts
      │
merge_stream_data           pull new Kafka consumer events into main warehouse
      │
dbt_staging                 rebuild staging views
      │
dbt_intermediate            rebuild intermediate views
      │
┌─────┴──────────┬──────────────┐
dbt_marts_payments  dbt_marts_customers  dbt_marts_risk   (parallel)
└─────┬──────────┴──────────────┘
      │
dbt_tests                   run all 30 data quality tests
      │
branch_on_tests
  ├── export_dashboard       regenerate dashboard.html
  └── skip_export            (if any test failed)
          │
    pipeline_summary         log volume, completion %, fraud %, Kafka lag (always runs)
```

### `streaming_monitor` — runs every 5 minutes

```
check_kafka_lag         AdminClient: committed vs end offset per partition → total lag
      │
check_stream_freshness  DuckDB: minutes since last consumer write
      │
anomaly_detection       fraud rate spike, decline rate spike, avg amount anomaly
      │
stream_summary          rolling totals + full health report
```

---

## Quick Start — No Docker Needed

```bash
# 1. Clone
git clone https://github.com/tanvirpasha21/fintech-analytics-platform.git
cd fintech-analytics-platform

# 2. Virtual environment
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# 3. Generate data + run dbt pipeline
python scripts/generate_data.py
cd fintech_dbt && dbt build --profiles-dir . && cd ..
# Expected: 42 of 42 PASS (12 models + 30 tests)

# 4. Open dashboard
open dashboard.html

# 5. Stream in simulation mode (no Kafka required)
python streaming/consumer/transaction_consumer.py --simulate

# 6. Run Airflow locally
pip install apache-airflow==2.9.1

export AIRFLOW_HOME=$(pwd)/airflow
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/airflow/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:///$(pwd)/airflow/airflow.db
export FINTECH_DB_PATH=$(pwd)/data/fintech.duckdb
export FINTECH_STREAM_DB_PATH=$(pwd)/data/fintech_stream.duckdb
export FINTECH_DBT_PATH=$(pwd)/fintech_dbt
export FINTECH_SCRIPTS_PATH=$(pwd)/scripts
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export KAFKA_CONSUMER_GROUP=fintech-duckdb-sink

airflow db migrate
airflow users create --username admin --password admin \
    --firstname Tanvir --lastname Anjum \
    --role Admin --email contact@voidstudio.tech

airflow webserver --port 8081 &   # http://localhost:8081
airflow scheduler
```

---

## Full Stack — Docker (Kafka + Airflow + PostgreSQL)

```bash
# Start all 8 services
docker compose up -d

# Check everything is running
docker compose ps

# Wait ~60 seconds then open:
# Kafka UI  →  http://localhost:8080
# Airflow   →  http://localhost:8081  (admin / admin)
```

**Port conflict on 5432?** Change the postgres port mapping in docker-compose.yml:
```yaml
ports:
  - "5433:5432"   # use 5433 if local PostgreSQL is already running
```

**Run the streaming pipeline:**
```bash
# Terminal 1 — producer (live transactions into Kafka)
source venv/bin/activate
python streaming/producer/transaction_producer.py --tps 3

# Terminal 2 — consumer (Kafka → DuckDB)
source venv/bin/activate
python streaming/consumer/transaction_consumer.py
```

**Trigger the Airflow pipeline:**

Go to http://localhost:8081 → click `fintech_pipeline` → click ▶ Trigger DAG

---

## Key Concepts Demonstrated

### dbt
- 3-layer architecture: staging → intermediate → marts
- Incremental materialisation with `is_incremental()` guard (`fct_fraud_events`)
- Reusable macros: `safe_divide`, `pct_of_total`, `convert_currency`, `generate_date_spine`
- 30 data quality tests: uniqueness, not null, accepted values across all layers
- Pre-aggregated intermediate layer to avoid redundant computation across marts

### Apache Kafka
- Partitioning by `customer_id` — ordering guaranteed per customer across all 3 partitions
- Idempotent producer (`enable_idempotence=True`, `acks=all`) — no duplicates on retry
- Manual offset commit — offsets committed only after successful DuckDB write (at-least-once)
- Dead Letter Queue — malformed messages go to `fintech.dlq`, never block the pipeline
- Simulation mode — full pipeline runs without a Kafka broker for local dev and CI

### Apache Airflow
- `merge_stream_data` task — connects the Kafka consumer database to the main warehouse before dbt runs, making the streaming and batch layers genuinely end-to-end
- `check_kafka_health` — first task in every pipeline run, verifies broker, topics, and consumer group using the Kafka AdminClient
- `check_kafka_lag` — real partition-level lag measurement (committed offset vs end offset) in the streaming monitor
- PostgreSQL metadata database — replaces SQLite, required for parallel task execution
- BranchPythonOperator — skips dashboard export if any dbt test fails
- XCom — passes row counts, test results, merged event counts, and Kafka health between tasks
- `TriggerRule.ALL_DONE` — pipeline summary always runs regardless of upstream failures
- Exponential backoff retries on all tasks

---

## Results (150k transactions, 2023–2024)

| Metric | Value |
|---|---|
| Total Payment Volume | £10.23M |
| Avg Completion Rate | 92.3% |
| Avg Fraud Rate | 1.96% |
| Fraud Events Detected | 2,881 |
| Total Disputed Amount | £217,134 |
| RFM Segments | 12 |
| dbt Models | 13 |
| dbt Tests | 30 / 30 passing |
| Airflow Tasks (pipeline DAG) | 13 |
| Kafka Topics | 3 |

---

## Author

**MD Tanvir Anjum** — Co-Founder, Void Studio
[linkedin.com/in/mdtanviranjum21](https://linkedin.com/in/mdtanviranjum21) · [voidstudiotech.co.uk](https://voidstudiotech.co.uk)
