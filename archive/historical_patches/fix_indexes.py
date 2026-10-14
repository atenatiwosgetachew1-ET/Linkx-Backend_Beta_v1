import re

file_path = '/var/www/linkx-backend/service_factory/services/linkx-worker/src/linkx_worker/xvigilance_consumer.py'

with open(file_path, 'r') as f:
    content = f.read()

# Add AccountAlert index creation
index_code = """
            session.run(f"CREATE INDEX idx_ben_phone IF NOT EXISTS FOR (n:`{node_label}`) ON (n.BENTELNO)")
            session.run("CREATE INDEX idx_alert_acc IF NOT EXISTS FOR (a:AccountAlert) ON (a.account_no)")
            session.run("CREATE INDEX idx_alert_sess IF NOT EXISTS FOR (a:AccountAlert) ON (a.session_id)")
"""

content = content.replace('session.run(f"CREATE INDEX idx_ben_phone IF NOT EXISTS FOR (n:`{node_label}`) ON (n.BENTELNO)")', index_code.strip())

with open(file_path, 'w') as f:
    f.write(content)

print("Added AccountAlert indexes to xvigilance_consumer.py")
