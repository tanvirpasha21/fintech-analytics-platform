# Setup Guide — Kafka + Airflow

This guide wires Apache Kafka and Apache Airflow into the existing
dbt + DuckDB fintech analytics platform.

---

## Option A — Docker (recommended, easiest)

One command starts Kafka, Kafka UI, Airflow webserver, and Airflow scheduler.

### Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running

### Steps

```bash
# 1. Clone the repo
git clone https://github.com/tanvirpasha21/fintech-analytics-platform.git
cd fintech-analytics-platform

# 2. Seed the database (one-time)
pip install -r requirements.txt
python scripts/generate_data.py

# 3. Start the full stack
docker compose up -d

# Wait ~60 seconds for all services to initialise, then open:
#   http://localhost:8080   Kafka UI    (browse topics and messages)
#   http://localhost:8081   Airflow     (login: admin / admin)
```

### Stream live transactions

Open a new terminal (outside Docker — your local machine):

```bash
source venv/bin/activate   # or however you activate your venv

# Stream at 5 transactions per second
python streaming/producer/transaction_producer.py --tps 5

# Watch messages appear in Kafka UI at http://localhost:8080
# Topic: fintech.transactions
```

### Trigger the Airflow pipeline

1. Open http://localhost:8081
2. Login: **admin** / **admin**
3. Find `fintech_pipeline` → click the ▶ play button → **Trigger DAG**
4. Watch each task turn green: health_check → staging → intermediate → marts → tests → dashboard

### Stop everything

```bash
docker compose down        # stop containers, keep volumes
docker compose down -v     # stop + delete all data (full reset)
```

---

## Option B — Local (no Docker)

Run everything directly on your Mac without Docker.

### Prerequisites

```bash
# Python venv
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
pip install apache-airflow==2.9.1   # install separately
```

### Kafka (local)

Kafka requires Java. Install with Homebrew:

```bash
brew install kafka

# Start Zookeeper (Terminal 1)
zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties

# Start Kafka (Terminal 2)
kafka-server-start /usr/local/etc/kafka/server.properties

# Create topics (Terminal 3, one-time)
kafka-topics --bootstrap-server localhost:9092 --create --topic fintech.transactions --partitions 3 --replication-factor 1
kafka-topics --bootstrap-server localhost:9092 --create --topic fintech.fraud_alerts  --partitions 3 --replication-factor 1
kafka-topics --bootstrap-server localhost:9092 --create --topic fintech.dlq           --partitions 1 --replication-factor 1
```

### Airflow (local)

```bash
# Set up Airflow (one-time)
export AIRFLOW_HOME=$(pwd)/airflow
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/airflow/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:///$(pwd)/airflow/airflow.db
export FINTECH_DB_PATH=$(pwd)/data/fintech.duckdb

airflow db migrate
airflow users create \
  --username admin --password admin \
  --firstname Tanvir --lastname Anjum \
  --role Admin --email contact@voidstudio.tech

# Start webserver (Terminal 4)
airflow webserver --port 8081

# Start scheduler (Terminal 5)
airflow scheduler
```

### Option B — No Kafka at all (simulation mode)

If you just want to test the consumer without installing Kafka:

```bash
# Terminal 1: consumer reads simulated events
python streaming/consumer/transaction_consumer.py --simulate

# Terminal 2: run the Airflow pipeline manually (no scheduler needed)
cd fintech_dbt
dbt build --profiles-dir .
```

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  STREAMING LAYER (real-time)                                    │
│                                                                 │
│  transaction_producer.py                                        │
│       │  (2–10 events/sec)                                      │
│       ▼                                                         │
│  Kafka (fintech.transactions topic, 3 partitions)               │
│       │  (partitioned by customer_id — ordering guaranteed)     │
│       ▼                                                         │
│  transaction_consumer.py                                        │
│       │  (micro-batch: 50 msgs or 10s, whichever first)        │
│       ▼                                                         │
│  data/fintech_stream.duckdb  (stream.raw_transactions)         │
└─────────────────────────────────────────────────────────────────┘
         │
         │  Airflow checks stream health every 5 minutes
         │  (streaming_monitor DAG)
         ▼
┌─────────────────────────────────────────────────────────────────┐
│  BATCH LAYER (hourly)                                           │
│                                                                 │
│  Airflow: fintech_pipeline DAG                                  │
│       health_check                                              │
│            │                                                    │
│       validate_raw_data                                         │
│            │                                                    │
│       dbt_staging  (5 views)                                    │
│            │                                                    │
│       dbt_intermediate  (2 views)                               │
│            │                                                    │
│       ┌────┴──────────┬──────────┐                              │
│   payments       customers     risk   ← parallel               │
│       └────┬──────────┴──────────┘                              │
│            │                                                    │
│       dbt_tests  (30 tests)                                     │
│            │                                                    │
│       export_dashboard → dashboard.html                         │
│            │                                                    │
│       pipeline_summary                                          │
└─────────────────────────────────────────────────────────────────┘
```

## Key Concepts

### Why partition by customer_id?
All events from the same customer always go to the same Kafka partition.
This guarantees that events for a given customer are processed in order.
Critical for fraud velocity calculations (30-day rolling window).

### Why manual offset commit?
The consumer commits Kafka offsets only AFTER successfully writing to DuckDB.
If the consumer crashes mid-batch, it re-reads from the last committed offset.
This gives at-least-once delivery — no messages are lost.

### Why micro-batching instead of one-at-a-time?
Writing one row at a time to DuckDB is slow. Buffering 50 messages and
writing them in one transaction is 50x faster and reduces write amplification.

### Why is the pipeline hourly instead of streaming end-to-end?
dbt models read from raw.transactions (the batch table) not the stream table.
In production you would run dbt models on top of the stream table too.
For a portfolio, the hybrid batch+stream architecture is the right pattern to show.
