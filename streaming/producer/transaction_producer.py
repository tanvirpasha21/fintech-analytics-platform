"""
streaming/producer/transaction_producer.py
==========================================
Simulates a live payment API streaming transaction events into Kafka.

Run:
    python streaming/producer/transaction_producer.py           # 2 tps
    python streaming/producer/transaction_producer.py --tps 10
    python streaming/producer/transaction_producer.py --dry-run # no Kafka needed
"""

import argparse
import json
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone

from faker import Faker
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

fake = Faker("en_GB")
random.seed()

KAFKA_BOOTSTRAP    = "localhost:9092"
TOPIC_TRANSACTIONS = "fintech.transactions"
TOPIC_FRAUD_ALERTS = "fintech.fraud_alerts"

CURRENCIES = ["GBP", "USD", "EUR", "SGD", "AED", "CAD"]
FX_BASE    = {"GBP":1.0,"USD":1.27,"EUR":1.17,"SGD":1.70,"AED":4.66,"CAD":1.73}
MCC_MAP = {
    "Groceries":["5411","5412"],"Dining":["5812","5814"],
    "Travel":["4511","7011"],"Entertainment":["7832","7922"],
    "Retail":["5651","5691","5999"],"Fuel":["5541","5542"],
    "Healthcare":["8011","8049"],"Subscription":["7372","5968"],
    "ATM":["6011"],"Transfer":["6012"],
}
FRAUD_RISK = {
    "Groceries":0.005,"Dining":0.008,"Travel":0.03,"Entertainment":0.015,
    "Retail":0.012,"Fuel":0.007,"Healthcare":0.004,"Subscription":0.02,
    "ATM":0.04,"Transfer":0.05,
}

print("Building customer and merchant pools...")
CUSTOMERS = [
    {
        "customer_id":  str(uuid.uuid4()),
        "account_tier": random.choices(["standard","premium","business"],[0.6,0.3,0.1])[0],
        "country":      random.choices(["GB","US","DE","SG","AE","CA"],[0.5,0.2,0.1,0.08,0.07,0.05])[0],
        "currency":     random.choices(CURRENCIES,[0.5,0.2,0.15,0.05,0.05,0.05])[0],
        "kyc_verified": random.random() < 0.9,
    }
    for _ in range(500)
]
MERCHANTS = [
    {
        "merchant_id":   str(uuid.uuid4()),
        "merchant_name": fake.company(),
        "category":      (cat := random.choice(list(MCC_MAP.keys()))),
        "mcc_code":      random.choice(MCC_MAP[cat]),
        "country":       random.choices(["GB","US","DE","SG","AE","CA"],[0.4,0.25,0.1,0.1,0.08,0.07])[0],
        "is_high_risk":  random.random() < FRAUD_RISK[cat],
    }
    for _ in range(100)
]
print(f"  {len(CUSTOMERS)} customers, {len(MERCHANTS)} merchants ready.\n")


def make_event(customer: dict, merchant: dict) -> dict:
    ccy      = customer["currency"]
    cat      = merchant["category"]
    amount   = round(min(random.expovariate(1/85) + 0.5, 9999.99), 2)
    fraud_p  = FRAUD_RISK.get(cat, 0.01) * (2 if merchant["is_high_risk"] else 1)
    is_fraud = random.random() < fraud_p
    status   = random.choices(["completed","declined","pending"],[0.93,0.05,0.02])[0]
    if is_fraud and random.random() < 0.3:
        status = "failed"

    return {
        "event_id":             str(uuid.uuid4()),
        "transaction_id":       str(uuid.uuid4()),
        "event_time":           datetime.now(timezone.utc).isoformat(),
        "customer_id":          customer["customer_id"],
        "merchant_id":          merchant["merchant_id"],
        "merchant_name":        merchant["merchant_name"],
        "merchant_category":    cat,
        "mcc_code":             merchant["mcc_code"],
        "amount":               amount,
        "currency":             ccy,
        "amount_gbp":           round(amount / FX_BASE.get(ccy, 1.0), 2),
        "status":               status,
        "channel":              random.choices(["card","app","web","atm"],[0.5,0.3,0.15,0.05])[0],
        "ip_country":           random.choices(["GB","US","DE","SG","AE","CA","NG","RU"],[0.45,0.2,0.1,0.08,0.07,0.05,0.03,0.02])[0],
        "customer_tier":        customer["account_tier"],
        "customer_country":     customer["country"],
        "is_kyc_verified":      customer["kyc_verified"],
        "is_high_risk_merchant":merchant["is_high_risk"],
        "is_cross_border":      customer["country"] != merchant["country"],
        "is_flagged_fraud":     is_fraud,
        "risk_score":           round(random.betavariate(6,4)*100 if is_fraud else random.betavariate(2,8)*100, 1),
    }


def run(tps: float = 2.0, dry_run: bool = False):
    producer = None
    if not dry_run:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode(),
                key_serializer=lambda k: k.encode() if k else None,
                acks="all",
                retries=5,
                enable_idempotence=True,
                compression_type="gzip",
            )
            print(f"Connected to Kafka at {KAFKA_BOOTSTRAP}")
        except NoBrokersAvailable:
            print("Kafka not available — switching to dry-run mode.\n")
            dry_run = True

    stats = {"sent": 0, "fraud": 0, "start": time.time()}

    def shutdown(sig, frame):
        elapsed = time.time() - stats["start"]
        print(f"\n\nSent {stats['sent']:,} transactions ({stats['fraud']:,} fraud alerts) "
              f"in {elapsed:.0f}s — {stats['sent']/max(elapsed,1):.1f} msg/s")
        if producer:
            producer.flush()
            producer.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)

    print(f"Streaming at {tps} tps  |  Ctrl+C to stop\n")
    print(f"{'TIME':<20} {'ID':>10} {'AMOUNT':>9} {'CCY':<4} {'STATUS':<10} {'FRAUD':<6} CHANNEL")
    print("─" * 72)

    interval = 1.0 / tps
    while True:
        customer = random.choice(CUSTOMERS)
        merchant = random.choice(MERCHANTS)
        event    = make_event(customer, merchant)
        stats["sent"] += 1

        short = event["transaction_id"][:8]
        print(f"{event['event_time'][11:19]:<20} {short:>10} "
              f"£{event['amount_gbp']:>7.2f} {event['currency']:<4} "
              f"{event['status']:<10} {'⚠ YES' if event['is_flagged_fraud'] else 'no':<6} "
              f"{event['channel']}")

        if not dry_run and producer:
            producer.send(TOPIC_TRANSACTIONS, key=event["customer_id"], value=event)
            if event["is_flagged_fraud"] and event["risk_score"] > 60:
                alert = {
                    "alert_id":       str(uuid.uuid4()),
                    "transaction_id": event["transaction_id"],
                    "customer_id":    event["customer_id"],
                    "amount_gbp":     event["amount_gbp"],
                    "risk_score":     event["risk_score"],
                    "alert_time":     datetime.now(timezone.utc).isoformat(),
                    "reason":         random.choice(["velocity_check","unusual_geography","high_risk_merchant","amount_anomaly"]),
                }
                producer.send(TOPIC_FRAUD_ALERTS, key=event["transaction_id"], value=alert)
                stats["fraud"] += 1
                print(f"  {'':20} 🚨 FRAUD ALERT (risk={event['risk_score']})")

        time.sleep(interval)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tps", type=float, default=2.0, help="Transactions per second")
    parser.add_argument("--dry-run", action="store_true", help="Print events, don't send to Kafka")
    args = parser.parse_args()
    run(tps=args.tps, dry_run=args.dry_run)
