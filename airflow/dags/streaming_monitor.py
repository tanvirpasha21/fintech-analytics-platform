"""
airflow/dags/streaming_monitor.py
===================================
Runs every 5 minutes to monitor the full Kafka → DuckDB streaming pipeline.

Fix 3: check_kafka_lag now uses the real Kafka AdminClient to measure
        actual consumer group lag per partition — not just DuckDB freshness.

Tasks:
  check_kafka_lag       ← Fix 3: real partition-level lag via AdminClient
  check_stream_freshness   alert if no new data written to DuckDB in 10 min
  anomaly_detection        flag fraud spikes / amount anomalies in stream
  stream_summary           rolling totals printed to Airflow logs
"""

import os
from datetime import datetime, timedelta

import duckdb
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

BASE      = os.environ.get("AIRFLOW_HOME",
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
STREAM_DB = os.environ.get("FINTECH_STREAM_DB_PATH",
            os.path.join(BASE, "data", "fintech_stream.duckdb"))

# Kafka config — pulled from environment (set in docker-compose)
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CONSUMER_GROUP  = os.environ.get("KAFKA_CONSUMER_GROUP",    "fintech-duckdb-sink")
TOPIC           = "fintech.transactions"

# Alert thresholds
MAX_LAG_MESSAGES   = 5_000   # raise alert if consumer is this many messages behind
MAX_STALE_MINUTES  = 10      # raise alert if no new DuckDB writes in this many minutes
MAX_FRAUD_RATE_PCT = 10.0    # raise alert if fraud rate in last 100 events exceeds this
MAX_AVG_AMOUNT_GBP = 800.0   # raise alert if average transaction is unusually high

default_args = {
    "owner":             "tanvir",
    "retries":           1,
    "retry_delay":       timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=4),
}

with DAG(
    dag_id="streaming_monitor",
    description="Every 5 min: real Kafka lag check + stream health + anomaly detection",
    schedule="*/5 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["fintech", "streaming", "monitoring", "kafka"],
) as dag:

    # ── FIX 3: REAL KAFKA LAG CHECK ───────────────────────────────────────────
    # Previous version only read DuckDB — it had no idea what was in Kafka.
    # This version uses the Kafka AdminClient to:
    #   1. Get the committed offset for each partition in the consumer group
    #   2. Get the end (latest) offset for each partition
    #   3. Compute lag = end_offset - committed_offset per partition
    #   4. Sum across all partitions for total lag
    #   5. Alert if total lag exceeds MAX_LAG_MESSAGES
    def check_kafka_lag(**ctx):
        try:
            from kafka import KafkaAdminClient, KafkaConsumer, TopicPartition
            from kafka.errors import NoBrokersAvailable
        except ImportError:
            print("kafka-python not installed — skipping Kafka lag check.")
            ctx["ti"].xcom_push(key="lag_result", value={"status": "skipped"})
            return

        result = {
            "status":           "unknown",
            "broker_reachable": False,
            "total_lag":        None,
            "partitions":       [],
            "consumer_active":  False,
        }

        try:
            # ── Step 1: Connect to broker ──────────────────────────────────────
            admin = KafkaAdminClient(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                client_id="airflow-lag-monitor",
                request_timeout_ms=5000,
            )
            result["broker_reachable"] = True
            print(f"Connected to Kafka at {KAFKA_BOOTSTRAP} ✓")

            # ── Step 2: Check consumer group exists ────────────────────────────
            all_groups = [g[0] for g in admin.list_consumer_groups()]
            if CONSUMER_GROUP not in all_groups:
                print(f"Consumer group '{CONSUMER_GROUP}' not found.")
                print("This means the consumer hasn't run yet — that's okay.")
                result["status"] = "consumer_not_started"
                admin.close()
                ctx["ti"].xcom_push(key="lag_result", value=result)
                return

            result["consumer_active"] = True

            # ── Step 3: Get committed offsets for each partition ───────────────
            committed_offsets = admin.list_consumer_group_offsets(CONSUMER_GROUP)
            print(f"Consumer group '{CONSUMER_GROUP}' has {len(committed_offsets)} assigned partitions")

            # ── Step 4: Get end offsets (latest messages available) ────────────
            # We need a KafkaConsumer briefly just to call end_offsets()
            consumer = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP)
            topic_partitions = list(committed_offsets.keys())

            # Filter to our main transactions topic only
            txn_partitions = [tp for tp in topic_partitions if tp.topic == TOPIC]

            if not txn_partitions:
                print(f"No committed offsets found for topic '{TOPIC}'")
                result["status"] = "no_offsets"
                consumer.close()
                admin.close()
                ctx["ti"].xcom_push(key="lag_result", value=result)
                return

            end_offsets = consumer.end_offsets(txn_partitions)
            consumer.close()

            # ── Step 5: Calculate lag per partition ────────────────────────────
            total_lag = 0
            partition_details = []

            for tp in txn_partitions:
                committed = committed_offsets[tp].offset
                end       = end_offsets.get(tp, committed)
                lag       = max(end - committed, 0)
                total_lag += lag

                partition_details.append({
                    "topic":     tp.topic,
                    "partition": tp.partition,
                    "committed": committed,
                    "end":       end,
                    "lag":       lag,
                })
                print(f"  Partition {tp.partition}: committed={committed} end={end} lag={lag}")

            result["total_lag"]  = total_lag
            result["partitions"] = partition_details
            result["status"]     = "healthy" if total_lag < MAX_LAG_MESSAGES else "lagging"

            # ── Step 6: Alert if lag is too high ──────────────────────────────
            print(f"\n  Total consumer lag: {total_lag:,} messages")
            if total_lag == 0:
                print("  Consumer is fully caught up ✓")
            elif total_lag < MAX_LAG_MESSAGES:
                print(f"  Lag within acceptable threshold (< {MAX_LAG_MESSAGES:,}) ✓")
            else:
                msg = (
                    f"ALERT: Consumer lag {total_lag:,} messages exceeds threshold "
                    f"{MAX_LAG_MESSAGES:,}! "
                    "Consumer may be down or processing too slowly."
                )
                print(f"  🚨 {msg}")
                # Raise to mark this task as failed in Airflow
                raise Exception(msg)

            admin.close()

        except Exception as e:
            if "ALERT:" in str(e):
                raise  # re-raise our intentional alerts
            print(f"  ⚠  Kafka lag check failed: {e}")
            print(      "     This is non-fatal — continuing pipeline monitoring.")
            result["status"] = "error"
            result["error"]  = str(e)

        ctx["ti"].xcom_push(key="lag_result", value=result)

    t_kafka_lag = PythonOperator(
        task_id="check_kafka_lag",
        python_callable=check_kafka_lag,
    )

    # ── STREAM FRESHNESS (DuckDB side) ────────────────────────────────────────
    # Checks the DuckDB stream database — how long since the consumer
    # last wrote a record. Complements the Kafka lag check above:
    #   - Kafka lag = how many messages are waiting to be consumed
    #   - DuckDB freshness = how recently the consumer successfully wrote
    def check_stream_freshness(**ctx):
        if not os.path.exists(STREAM_DB):
            print("Stream database not found — consumer not started yet.")
            ctx["ti"].xcom_push(key="freshness", value={"status": "not_started"})
            return

        con = duckdb.connect(STREAM_DB, read_only=True)
        try:
            r = con.execute("""
                SELECT
                    count(*)                                        as total_events,
                    max(cast(event_time as timestamp))              as latest_event_time,
                    max(cast(consumed_at as timestamp))             as latest_consumed_at,
                    datediff('minute',
                        max(cast(consumed_at as timestamp)),
                        current_timestamp)                          as minutes_since_write
                FROM stream.raw_transactions
            """).fetchone()

            total, latest_event, latest_write, stale_mins = r

            print(f"  Total stream events:    {total:,}")
            print(f"  Latest event time:      {latest_event}")
            print(f"  Latest write to DuckDB: {latest_write}")
            print(f"  Minutes since write:    {stale_mins}")

            freshness = {
                "status":        "healthy",
                "total_events":  total,
                "stale_minutes": stale_mins,
            }

            if stale_mins and stale_mins > MAX_STALE_MINUTES:
                msg = (
                    f"ALERT: No new data written to stream DB for {stale_mins} minutes! "
                    f"Threshold is {MAX_STALE_MINUTES} min. Is the consumer running?"
                )
                print(f"  🚨 {msg}")
                freshness["status"] = "stale"
                raise Exception(msg)
            else:
                print("  Stream freshness OK ✓")

            ctx["ti"].xcom_push(key="freshness", value=freshness)

        finally:
            con.close()

    t_freshness = PythonOperator(
        task_id="check_stream_freshness",
        python_callable=check_stream_freshness,
        trigger_rule=TriggerRule.ALL_DONE,  # run even if Kafka lag check failed
    )

    # ── ANOMALY DETECTION ─────────────────────────────────────────────────────
    def anomaly_detection(**ctx):
        if not os.path.exists(STREAM_DB):
            print("Stream DB not found — skipping anomaly detection.")
            return

        con = duckdb.connect(STREAM_DB, read_only=True)
        try:
            # Analyse last 100 events
            r = con.execute("""
                SELECT
                    count(*)                                                    as total,
                    sum(case when is_flagged_fraud then 1 else 0 end)           as fraud_count,
                    round(100.0 * sum(case when is_flagged_fraud then 1 else 0 end)
                          / nullif(count(*), 0), 2)                             as fraud_rate_pct,
                    round(avg(amount_gbp), 2)                                   as avg_amount_gbp,
                    round(max(amount_gbp), 2)                                   as max_amount_gbp,
                    count(distinct customer_id)                                 as unique_customers,
                    count(case when status = 'declined' then 1 end)             as declined_count,
                    round(100.0 * count(case when status = 'declined' then 1 end)
                          / nullif(count(*), 0), 2)                             as decline_rate_pct
                FROM (
                    SELECT * FROM stream.raw_transactions
                    ORDER BY consumed_at DESC
                    LIMIT 100
                )
            """).fetchone()

            total, fraud, fraud_pct, avg_amt, max_amt, customers, declined, decline_pct = r

            print(f"  Last 100 stream events:")
            print(f"    Fraud rate:    {fraud_pct}%  ({fraud} events)")
            print(f"    Decline rate:  {decline_pct}%  ({declined} events)")
            print(f"    Avg amount:    £{avg_amt}")
            print(f"    Max amount:    £{max_amt}")
            print(f"    Customers:     {customers}")

            anomalies = []

            if fraud_pct and float(fraud_pct) > MAX_FRAUD_RATE_PCT:
                anomalies.append(f"Fraud rate {fraud_pct}% exceeds {MAX_FRAUD_RATE_PCT}% threshold")

            if avg_amt and float(avg_amt) > MAX_AVG_AMOUNT_GBP:
                anomalies.append(f"Avg transaction £{avg_amt} exceeds £{MAX_AVG_AMOUNT_GBP} threshold")

            if decline_pct and float(decline_pct) > 20:
                anomalies.append(f"Decline rate {decline_pct}% is unusually high (> 20%)")

            if anomalies:
                print("\n  🚨 ANOMALIES DETECTED:")
                for a in anomalies:
                    print(f"    - {a}")
            else:
                print("\n  No anomalies detected ✓")

            ctx["ti"].xcom_push(key="anomalies", value=anomalies)

        finally:
            con.close()

    t_anomaly = PythonOperator(
        task_id="anomaly_detection",
        python_callable=anomaly_detection,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # ── STREAM SUMMARY ────────────────────────────────────────────────────────
    def stream_summary(**ctx):
        lag_result  = ctx["ti"].xcom_pull(task_ids="check_kafka_lag",       key="lag_result") or {}
        freshness   = ctx["ti"].xcom_pull(task_ids="check_stream_freshness", key="freshness")  or {}
        anomalies   = ctx["ti"].xcom_pull(task_ids="anomaly_detection",      key="anomalies")  or []

        print("=" * 55)
        print("STREAM MONITOR SUMMARY")
        print("=" * 55)

        # Kafka status
        print(f"  Kafka broker:       {lag_result.get('broker_reachable', 'unknown')}")
        print(f"  Consumer active:    {lag_result.get('consumer_active', 'unknown')}")
        total_lag = lag_result.get("total_lag")
        if total_lag is not None:
            print(f"  Consumer lag:       {total_lag:,} messages")

        # DuckDB stream status
        print(f"  Stream freshness:   {freshness.get('status', 'unknown')}")
        stale = freshness.get("stale_minutes")
        if stale is not None:
            print(f"  Minutes since last write: {stale}")
        total_events = freshness.get("total_events")
        if total_events:
            print(f"  Total stream events: {total_events:,}")

        # Anomalies
        if anomalies:
            print(f"\n  ⚠  {len(anomalies)} anomaly(s) detected:")
            for a in anomalies:
                print(f"     - {a}")
        else:
            print("\n  ✓  No anomalies")

        print("=" * 55)

        if not os.path.exists(STREAM_DB):
            return

        # Full rolling stats from DuckDB
        con = duckdb.connect(STREAM_DB, read_only=True)
        try:
            r = con.execute("""
                SELECT
                    count(*)                                                    as events,
                    round(sum(amount_gbp), 2)                                   as volume_gbp,
                    round(avg(amount_gbp), 2)                                   as avg_txn_gbp,
                    count(case when is_flagged_fraud then 1 end)                as fraud_flags,
                    count(distinct customer_id)                                 as customers,
                    round(100.0 * count(case when is_flagged_fraud then 1 end)
                          / nullif(count(*), 0), 2)                             as fraud_rate_pct,
                    count(case when status = 'completed' then 1 end)            as completed,
                    round(100.0 * count(case when status = 'completed' then 1 end)
                          / nullif(count(*), 0), 2)                             as completion_pct
                FROM stream.raw_transactions
            """).fetchone()

            print("\n  Rolling Stream Totals (all time):")
            print(f"    Events:       {r[0]:,}")
            print(f"    Volume:       £{r[1]:,.2f}")
            print(f"    Avg txn:      £{r[2]:.2f}")
            print(f"    Completed:    {r[6]:,}  ({r[7]}%)")
            print(f"    Fraud flags:  {r[3]:,}  ({r[5]}%)")
            print(f"    Customers:    {r[4]:,}")
        finally:
            con.close()

    t_summary = PythonOperator(
        task_id="stream_summary",
        python_callable=stream_summary,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # ── DEPENDENCY GRAPH ──────────────────────────────────────────────────────
    #
    # check_kafka_lag          ← Fix 3: real partition-level lag measurement
    #       │
    # check_stream_freshness   ← DuckDB write freshness (ALL_DONE — runs even if lag alert)
    #       │
    # anomaly_detection        ← fraud/decline rate spikes (ALL_DONE)
    #       │
    # stream_summary           ← full report (ALL_DONE)

    t_kafka_lag >> t_freshness >> t_anomaly >> t_summary
