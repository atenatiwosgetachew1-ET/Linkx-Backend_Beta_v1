import re

def fix_circular(file_path):
    with open(file_path, "r") as f:
        content = f.read()

    # We want to add 'AND NOT a.LOGICAL_BENACCOUNTNO IN $pass_through_accounts'
    # right after 'AND a.LOGICAL_BENACCOUNTNO IS NOT NULL AND a.LOGICAL_BENACCOUNTNO <> \'\''
    
    target = "AND a.LOGICAL_BENACCOUNTNO IS NOT NULL AND a.LOGICAL_BENACCOUNTNO <> ''"
    
    # In xvigilance_consumer.py, the parameter is $pass_through_accounts
    if "xvigilance_consumer.py" in file_path:
        replacement = target + "\n                  AND NOT a.LOGICAL_BENACCOUNTNO IN $pass_through_accounts"
    else:
        # In LA_rules_script.py, the parameter is $pt
        replacement = target + "\n                  AND NOT a.LOGICAL_BENACCOUNTNO IN $pt"
        
    content = content.replace(target, replacement)
    
    with open(file_path, "w") as f:
        f.write(content)

fix_circular("/var/www/linkx-backend/service_factory/services/linkx-worker/src/linkx_worker/xvigilance_consumer.py")
fix_circular("/var/www/linkx-backend/service_factory/services/linkx-worker/src/batch_manager/analyzing/LA_rules_script.py")
print("Done")
