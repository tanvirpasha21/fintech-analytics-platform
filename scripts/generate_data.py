"""
generate_data.py
Generates synthetic fintech data and loads it into DuckDB.
Run: python scripts/generate_data.py
"""
import random, uuid, os
from datetime import datetime, timedelta
from faker import Faker
import pandas as pd
import duckdb

fake = Faker('en_GB')
random.seed(42)
Faker.seed(42)

N_CUSTOMERS    = 2_000
N_MERCHANTS    = 300
N_TRANSACTIONS = 150_000
START_DATE     = datetime(2023, 1, 1)
END_DATE       = datetime(2024, 12, 31)

CURRENCIES = ['GBP','USD','EUR','SGD','AED','CAD']
FX_BASE    = {'GBP':1.0,'USD':1.27,'EUR':1.17,'SGD':1.70,'AED':4.66,'CAD':1.73}
MCC_MAP = {
    'Groceries':['5411','5412'],'Dining':['5812','5814'],'Travel':['4511','7011'],
    'Entertainment':['7832','7922'],'Retail':['5651','5691','5999'],
    'Fuel':['5541','5542'],'Healthcare':['8011','8049'],
    'Subscription':['7372','5968'],'ATM':['6011'],'Transfer':['6012'],
}
RISK_BY_MCC = {
    'Groceries':0.005,'Dining':0.008,'Travel':0.03,'Entertainment':0.015,
    'Retail':0.012,'Fuel':0.007,'Healthcare':0.004,'Subscription':0.02,
    'ATM':0.04,'Transfer':0.05,
}

def make_customers():
    rows = []
    for _ in range(N_CUSTOMERS):
        signup = fake.date_time_between(start_date='-3y', end_date='-6m')
        tier   = random.choices(['standard','premium','business'],[0.6,0.3,0.1])[0]
        rows.append({
            'customer_id':    str(uuid.uuid4()),
            'full_name':      fake.name(),
            'email':          fake.email(),
            'phone':          fake.phone_number(),
            'country':        random.choices(['GB','US','DE','SG','AE','CA'],[0.5,0.2,0.1,0.08,0.07,0.05])[0],
            'account_tier':   tier,
            'signup_date':    signup.date().isoformat(),
            'kyc_status':     random.choices(['verified','pending','failed'],[0.9,0.07,0.03])[0],
            'date_of_birth':  fake.date_of_birth(minimum_age=18, maximum_age=75).isoformat(),
            'currency':       random.choices(CURRENCIES,[0.5,0.2,0.15,0.05,0.05,0.05])[0],
        })
    return pd.DataFrame(rows)

def make_merchants():
    rows = []
    for _ in range(N_MERCHANTS):
        category = random.choice(list(MCC_MAP.keys()))
        rows.append({
            'merchant_id':     str(uuid.uuid4()),
            'merchant_name':   fake.company(),
            'category':        category,
            'mcc_code':        random.choice(MCC_MAP[category]),
            'country':         random.choices(['GB','US','DE','SG','AE','CA'],[0.4,0.25,0.1,0.1,0.08,0.07])[0],
            'is_high_risk':    random.random() < RISK_BY_MCC[category],
            'onboarded_date':  fake.date_between(start_date='-4y', end_date='-1y').isoformat(),
        })
    return pd.DataFrame(rows)

def make_fx_rates():
    rows = []
    d = START_DATE
    while d <= END_DATE:
        for ccy, base in FX_BASE.items():
            rows.append({'rate_date': d.date().isoformat(), 'currency': ccy,
                         'rate_to_gbp': round(base * (1 + random.gauss(0, 0.005)), 6)})
        d += timedelta(days=1)
    return pd.DataFrame(rows)

def make_transactions(customers, merchants):
    cust_ids  = customers['customer_id'].tolist()
    cust_ccys = dict(zip(customers['customer_id'], customers['currency']))
    merch_ids = merchants['merchant_id'].tolist()
    merch_cats= dict(zip(merchants['merchant_id'], merchants['category']))
    merch_risk= dict(zip(merchants['merchant_id'], merchants['is_high_risk']))
    delta = (END_DATE - START_DATE).days
    rows = []
    for _ in range(N_TRANSACTIONS):
        cid = random.choice(cust_ids)
        mid = random.choice(merch_ids)
        cat = merch_cats[mid]
        ccy = cust_ccys[cid]
        amount = round(min(random.expovariate(1/85) + 1, 9999.99), 2)
        ts = START_DATE + timedelta(days=random.randint(0,delta),
                                    hours=random.randint(0,23),
                                    minutes=random.randint(0,59),
                                    seconds=random.randint(0,59))
        fraud_p = RISK_BY_MCC.get(cat,0.01) * (2 if merch_risk[mid] else 1)
        is_fraud = random.random() < fraud_p
        status = 'failed' if is_fraud and random.random()<0.4 else \
                 random.choices(['completed','declined','pending'],[0.93,0.05,0.02])[0]
        rows.append({
            'transaction_id':  str(uuid.uuid4()),
            'customer_id':     cid, 'merchant_id': mid,
            'amount':          amount, 'currency': ccy,
            'transaction_at':  ts.isoformat(), 'status': status,
            'is_flagged_fraud':is_fraud, 'channel': random.choices(['card','app','web','atm'],[0.5,0.3,0.15,0.05])[0],
            'ip_country':      random.choices(['GB','US','DE','SG','AE','CA','NG','RU'],[0.45,0.2,0.1,0.08,0.07,0.05,0.03,0.02])[0],
        })
    return pd.DataFrame(rows)

def make_disputes(transactions):
    flagged = transactions[transactions['is_flagged_fraud'] & (transactions['status']=='completed')]
    rows = []
    for _, tx in flagged.sample(frac=0.35, random_state=42).iterrows():
        opened = datetime.fromisoformat(tx['transaction_at']) + timedelta(days=random.randint(1,30))
        rows.append({
            'dispute_id':   str(uuid.uuid4()), 'transaction_id': tx['transaction_id'],
            'customer_id':  tx['customer_id'], 'opened_at': opened.isoformat(),
            'reason':       random.choice(['unauthorised','duplicate','item_not_received','amount_incorrect']),
            'status':       random.choices(['won','lost','pending'],[0.6,0.25,0.15])[0],
            'amount':       tx['amount'], 'currency': tx['currency'],
        })
    return pd.DataFrame(rows)

if __name__ == '__main__':
    print("Generating synthetic fintech data...")
    customers    = make_customers()
    merchants    = make_merchants()
    fx_rates     = make_fx_rates()
    transactions = make_transactions(customers, merchants)
    disputes     = make_disputes(transactions)

    os.makedirs('data/raw', exist_ok=True)
    for name, df in [('customers',customers),('merchants',merchants),
                     ('fx_rates',fx_rates),('transactions',transactions),('disputes',disputes)]:
        df.to_csv(f'data/raw/{name}.csv', index=False)

    con = duckdb.connect('data/fintech.duckdb')
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    for name, df in [('customers',customers),('merchants',merchants),
                     ('fx_rates',fx_rates),('transactions',transactions),('disputes',disputes)]:
        con.execute(f"DROP TABLE IF EXISTS raw.{name}")
        con.execute(f"CREATE TABLE raw.{name} AS SELECT * FROM df")
        print(f"  raw.{name}: {con.execute(f'SELECT COUNT(*) FROM raw.{name}').fetchone()[0]:,} rows")
    con.close()
    print("Done.")
