import duckdb, json, re

con = duckdb.connect('data/fintech.duckdb')

queries = {
    'monthly_volume': '''
        select strftime(month, '%Y-%m') as month,
            round(sum(total_volume_gbp),2) as volume_gbp,
            sum(total_transactions) as transactions,
            round(avg(completion_rate)*100,2) as completion_pct,
            round(avg(fraud_rate)*100,3) as fraud_pct,
            sum(fraud_flagged_count) as fraud_events
        from main_marts_payments.fct_monthly_payment_performance
        group by 1 order by 1
    ''',
    'kpis': '''
        select round(sum(total_volume_gbp)/1000000,2) as total_volume_m,
            sum(total_transactions) as total_transactions,
            round(avg(completion_rate)*100,1) as avg_completion_pct,
            round(avg(fraud_rate)*100,2) as avg_fraud_pct,
            sum(fraud_flagged_count) as total_fraud_events
        from main_marts_payments.fct_monthly_payment_performance
    ''',
    # ... add the rest of your queries here
}

data = {}
for name, sql in queries.items():
    data[name] = con.execute(sql).df().to_dict(orient='records')

con.close()

# Read dashboard template, replace data
with open('dashboard.html', 'r') as f:
    html = f.read()

new_data = f'const D = {json.dumps(data)};'
html = re.sub(r'const D = \{.*?\};', new_data, html, flags=re.DOTALL)

with open('dashboard.html', 'w') as f:
    f.write(html)

print("Dashboard updated.")