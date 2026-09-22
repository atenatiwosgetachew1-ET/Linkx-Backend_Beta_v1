import re

def fix_indexes(file_path):
    with open(file_path, "r") as f:
        content = f.read()

    # In xvigilance_consumer.py
    if "xvigilance_consumer.py" in file_path:
        target = 'session.run(f"CREATE INDEX idx_ben_account_no IF NOT EXISTS FOR (n:`{node_label}`) ON (n.BENACCOUNTNO)")'
        replacement = target + """\n            session.run(f"CREATE INDEX idx_logical_acc IF NOT EXISTS FOR (n:`{node_label}`) ON (n.LOGICAL_ACCOUNTNO)")\n            session.run(f"CREATE INDEX idx_logical_ben IF NOT EXISTS FOR (n:`{node_label}`) ON (n.LOGICAL_BENACCOUNTNO)")"""
        content = content.replace(target, replacement)
    
    # In LA_rules_script.py
    if "LA_rules_script.py" in file_path:
        target = '"BENACCOUNTNO",'
        replacement = target + ' "LOGICAL_ACCOUNTNO", "LOGICAL_BENACCOUNTNO",'
        content = content.replace(target, replacement)
        
    with open(file_path, "w") as f:
        f.write(content)

fix_indexes("/var/www/linkx-backend/service_factory/services/linkx-worker/src/linkx_worker/xvigilance_consumer.py")
fix_indexes("/var/www/linkx-backend/service_factory/services/linkx-worker/src/batch_manager/analyzing/LA_rules_script.py")
print("Done")
