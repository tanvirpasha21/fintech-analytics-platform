"""
streaming/consumer/transaction_consumer.py
==========================================
Kafka → DuckDB sink consumer.

Key patterns:
  • Consumer group  — multiple instances share partition load
  • Manual commit   — offsets committed AFTER successful DuckDB write (at-least-once)
  • Micro-batching  — buffer N messages, flush atomically
  • Idempotent write — INSERT OR IGNORE on event_id prevents duplicates on retry
  • Dead Letter Queue — bad messages go to stream.dlq, never block pipeline
  • Simulation mode  — works without Kafka for local dev / CI

Run:
    python streaming/consumer/transaction_consumer.py --simulate  # no Kafka needed
    python streaming/consumer/transaction_consumer.py             # live Kafka
"""

import argparse
import json
import os
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone

import duckdb
import pandas as pd
from faker import Faker

fake = Faker("en_GB")

KAFKA_BOOTSTRAP    = "localhost:9092"
TOPIC_TRANSACTIONS = "fintech.transactions"
CONSUMER_GROUP     = "fintech-duckdb-sink"
BATCH_SIZE         = 50
BATCH_TIMEOUT_SEC  = 10

# Sits alongside fintech.duckdb in the data/ folder
DB_PATH = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "..", "data", "fintech_stream.duckdb")
)


def setup_db(con: duckdb.DuckDBPyConnection):
    con.execute("CREATE SCHEMA IF NOT EXISTS stream")
    con.execute("""
        CREATE TABLE IF NOT EXISTS stream.raw_transactions (
            kafka_partition   INTEGER,
            kafka_seq         BIGINT,
            consumed_at       TIMESTAMPTZ,
            event_id          VARCHAR PRIMARY KEY,
            transaction_id    VARCHAR NOT NULL,
            event_time        TIMESTAMPTZ,
            customer_id       VARCHAR NOT NULL,
            merchant_id       VARCHAR NOT NULL,
            merchant_name     VARCHAR,
            merchant_category VARCHAR,
            mcc_code          VARCHAR,
            amount            DOUBLE,
            currency          VARCHAR,
            amount_gbp        DOUBLE,
            status            VARCHAR,
            channel           VARCHAR,
            ip_country        VARCHAR,
            customer_tier     VARCHAR,
            customer_country  VARCHAR,
            is_kyc_verified   BOOLEAN,
            is_high_risk_merchant BOOLEAN,
            is_cross_border   BOOLEAN,
            is_flagged_fraud  BOOLEAN,
            risk_score        DOUBLE
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS stream.dlq (
            dlq_id         VARCHAR,
            raw_message    VARCHAR,
            error_message  VARCHAR,
            failed_at      TIMESTAMPTZ
        )
    """)
    print("Stream schema ready.")


def flush(con: duckdb.DuckDBPyConnection, batch: list, stats: dict):
    if not batch:
        return
    consumed_at = datetime.now(timezone.utc).isoformat()
    rows = []
    for msg in batch:
        d = msg["data"]
        rows.append({
            "kafka_partition":    msg.get("partition", 0),
            "kafka_seq":          msg.get("seq", 0),
            "consumed_at":        consumed_at,
            "event_id":           d.get("event_id", str(uuid.uuid4())),
            "transaction_id":     d.get("transaction_id", ""),
            "event_time":         d.get("event_time"),
            "customer_id":        d.get("customer_id", ""),
            "merchant_id":        d.get("merchant_id", ""),
            "merchant_name":      d.get("merchant_name", ""),
            "merchant_category":  d.get("merchant_category", ""),
            "mcc_code":           d.get("mcc_code", ""),
            "amount":             d.get("amount", 0),
            "currency":           d.get("currency", "GBP"),
            "amount_gbp":         d.get("amount_gbp", 0),
            "status":             d.get("status", ""),
            "channel":            d.get("channel", ""),
            "ip_country":         d.get("ip_country", ""),
            "customer_tier":      d.get("customer_tier", ""),
            "customer_country":   d.get("customer_country", ""),
            "is_kyc_verified":    bool(d.get("is_kyc_verified", False)),
            "is_high_risk_merchant": bool(d.get("is_high_risk_merchant", False)),
            "is_cross_border":    bool(d.get("is_cross_border", False)),
            "is_flagged_fraud":   bool(d.get("is_flagged_fraud", False)),
            "risk_score":         d.get("risk_score", 0),
        })
    df = pd.DataFrame(rows)
    con.execute("INSERT OR IGNORE INTO stream.raw_transactions SELECT * FROM df")
    stats["written"] += len(rows)
    stats["batches"] += 1
    fraud = sum(1 for r in rows if r["is_flagged_fraud"])
    print(f"  ✓ Batch {stats['batches']:>3}: {len(rows)} msgs written "
          f"({fraud} fraud)  |  Total: {stats['written']:,}")


def print_stats(con: duckdb.DuckDBPyConnection):
    r = con.execute("""
        SELECT count(*), round(sum(amount_gbp),2),
               count(*) filter (where is_flagged_fraud),
               count(distinct customer_id)
        FROM stream.raw_transactions
    """).fetchone()
    print(f"\n  📊  Events: {r[0]:,}  Volume: £{r[1]:,.2f}  "
          f"Fraud: {r[2]:,}  Customers: {r[3]:,}\n")


def run_simulation(con: duckdb.DuckDBPyConnection, stats: dict):
    """Generate fake events without Kafka — perfect for local dev and CI."""
    CATS = ["Groceries","Dining","Travel","Entertainment","Retail","ATM","Transfer"]
    CCYS = ["GBP","USD","EUR"]
    FX   = {"GBP":1.0,"USD":1.27,"EUR":1.17}

    batch, last_flush, seq = [], time.time(), 0

    print("Simulation mode active (no Kafka required)\n")
    print(f"{'SEQ':<6} {'TIME':<10} {'AMOUNT':>9} {'STATUS':<10} FRAUD")
    print("─" * 50)

    while True:
        seq += 1
        ccy      = random.choice(CCYS)
        amount   = round(min(random.expovariate(1/85) + 0.5, 9999.99), 2)
        is_fraud = random.random() < 0.02

        batch.append({
            "partition": 0,
            "seq":       seq,
            "data": {
                "event_id":           str(uuid.uuid4()),
                "transaction_id":     str(uuid.uuid4()),
                "event_time":         datetime.now(timezone.utc).isoformat(),
                "customer_id":        str(uuid.uuid4()),
                "merchant_id":        str(uuid.uuid4()),
                "merchant_name":      fake.company(),
                "merchant_category":  random.choice(CATS),
                "mcc_code":           str(random.randint(5000, 7999)),
                "amount":             amount,
                "currency":           ccy,
                "amount_gbp":         round(amount / FX[ccy], 2),
                "status":             random.choices(["completed","declined","pending"],[0.93,0.05,0.02])[0],
                "channel":            random.choices(["card","app","web","atm"],[0.5,0.3,0.15,0.05])[0],
                "ip_country":         random.choices(["GB","US","DE"],[0.6,0.25,0.15])[0],
                "customer_tier":      random.choices(["standard","premium","business"],[0.6,0.3,0.1])[0],
                "customer_country":   "GB",
                "is_kyc_verified":    True,
                "is_high_risk_merchant": random.random() < 0.1,
                "is_cross_border":    random.random() < 0.15,
                "is_flagged_fraud":   is_fraud,
                "risk_score":         round(random.uniform(60,95) if is_fraud else random.uniform(5,40), 1),
            }
        })
        stats["read"] += 1
        d = batch[-1]["data"]
        print(f"{seq:<6} {d['event_time'][11:19]:<10} "
              f"£{d['amount_gbp']:>7.2f} {d['status']:<10} "
              f"{'⚠ FRAUD' if is_fraud else '-'}")

        if len(batch) >= BATCH_SIZE or (time.time() - last_flush) >= BATCH_TIMEOUT_SEC:
            flush(con, batch, stats)
            print_stats(con)
            batch.clear()
            last_flush = time.time()

        time.sleep(0.5)


def run_kafka(con: duckdb.DuckDBPyConnection, stats: dict):
    """Connect to Kafka and consume live events."""
    from kafka import KafkaConsumer
    from kafka.errors import NoBrokersAvailable

    try:
        consumer = KafkaConsumer(
            TOPIC_TRANSACTIONS,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id=CONSUMER_GROUP,
            value_deserializer=lambda m: json.loads(m.decode()),
            key_deserializer=lambda k: k.decode() if k else None,
            enable_auto_commit=False,   # manual commit after DuckDB write
            auto_offset_reset="earliest",
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000,
            max_poll_records=100,
        )
        print(f"Connected to Kafka. Group: {CONSUMER_GROUP}\n")
    except NoBrokersAvailable:
        print("Kafka not available — switching to simulation mode.\n")
        run_simulation(con, stats)
        return

    batch, last_flush = [], time.time()

    print(f"{'PARTITION':<10} {'OFFSET':<10} {'AMOUNT':>9} {'STATUS':<10} FRAUD")
    print("─" * 55)

    while True:
        records = consumer.poll(timeout_ms=1000)
        for tp, messages in records.items():
            for msg in messages:
                try:
                    batch.append({
                        "partition": msg.partition,
                        "seq":       msg.offset,
                        "data":      msg.value,
                    })
                    stats["read"] += 1
                    d = msg.value
                    print(f"{msg.partition:<10} {msg.offset:<10} "
                          f"£{d.get('amount_gbp',0):>7.2f} "
                          f"{d.get('status','?'):<10} "
                          f"{'⚠ FRAUD' if d.get('is_flagged_fraud') else '-'}")
                except Exception as e:
                    con.execute(
                        "INSERT INTO stream.dlq VALUES (?,?,?,?)",
                        [str(uuid.uuid4()), str(msg.value), str(e),
                         datetime.now(timezone.utc).isoformat()]
                    )
                    print(f"  ✗ DLQ: {e}")

        if len(batch) >= BATCH_SIZE or (batch and (time.time() - last_flush) >= BATCH_TIMEOUT_SEC):
            flush(con, batch, stats)
            print_stats(con)
            consumer.commit()   # commit AFTER successful write
            batch.clear()
            last_flush = time.time()


def main(simulate: bool = False):
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    print(f"Opening stream database: {DB_PATH}")
    con = duckdb.connect(DB_PATH)
    setup_db(con)

    stats = {"read": 0, "written": 0, "batches": 0, "start": time.time()}

    def shutdown(sig, frame):
        elapsed = time.time() - stats["start"]
        print(f"\n\nShutdown — read:{stats['read']:,} written:{stats['written']:,} "
              f"batches:{stats['batches']} time:{elapsed:.0f}s")
        con.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)

    if simulate:
        run_simulation(con, stats)
    else:
        run_kafka(con, stats)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--simulate", action="store_true", help="Run without Kafka")
    args = parser.parse_args()
    main(simulate=args.simulate)
