file_path = '/var/www/linkx-backend/service_factory/services/linkx-worker/src/batch_manager/analyzing/LA_rules_script.py'

with open(file_path, 'r') as f:
    content = f.read()

# Replace MERGE with CREATE for the relationships in FRAUD_AGGREGATOR to avoid O(D^2) degree checking
content = content.replace(
    "MERGE (a)-[fa:FRAUD_ALERT_TARGET {session_id:$session_id}]->(t)",
    "CREATE (a)-[fa:FRAUD_ALERT_TARGET {session_id:$session_id}]->(t)"
)

with open(file_path, 'w') as f:
    f.write(content)

print("Replaced relationship MERGE with CREATE in FRAUD_AGGREGATOR.")
