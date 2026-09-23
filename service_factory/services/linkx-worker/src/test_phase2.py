import re

file_path = '/var/www/linkx-backend/service_factory/services/linkx-worker/src/batch_manager/analyzing/LA_rules_script.py'

with open(file_path, 'r') as f:
    content = f.read()

# Verify that is_evidence and anomaly_score are present in SMURFING
if 'r.is_evidence = true, r.anomaly_score' in content and 'SMURFING' in content:
    print("SUCCESS: SMURFING has evidence logic.")
else:
    print("FAILED: SMURFING lacks evidence logic.")

# Verify that get_fraud_aggregator_query exists
if 'def get_fraud_aggregator_query' in content:
    print("SUCCESS: get_fraud_aggregator_query exists.")
else:
    print("FAILED: get_fraud_aggregator_query missing.")

consumer_path = '/var/www/linkx-backend/service_factory/services/linkx-worker/src/linkx_worker/xvigilance_consumer.py'
with open(consumer_path, 'r') as f:
    consumer_content = f.read()

if 'FRAUD_AGGREGATOR' in consumer_content:
    print("SUCCESS: xvigilance_consumer.py runs FRAUD_AGGREGATOR.")
else:
    print("FAILED: xvigilance_consumer.py does not run FRAUD_AGGREGATOR.")
