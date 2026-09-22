import re

def fix_all(file_path):
    with open(file_path, "r") as f:
        content = f.read()

    # Fix the missing quotes
    content = content.replace("coalesce(outbound.TRANSACTIONDATE, ) = coalesce(inbound.TRANSACTIONDATE, )", "coalesce(outbound.TRANSACTIONDATE, '') = coalesce(inbound.TRANSACTIONDATE, '')")
    content = content.replace("coalesce(outbound.TRANSACTIONTIME, ) >= coalesce(inbound.TRANSACTIONTIME, )", "coalesce(outbound.TRANSACTIONTIME, '') >= coalesce(inbound.TRANSACTIONTIME, '')")
    content = content.replace("AND outbound.BENACCOUNTNO <> \n", "AND outbound.BENACCOUNTNO <> ''\n")

    # Fix the CALL { issue in LA_rules_script.py
    content = content.replace("CALL {\n                WITH inbound", "CALL {{\n                WITH inbound")
    content = content.replace("LIMIT 1\n            }\n\n            MERGE", "LIMIT 1\n            }}\n\n            MERGE")

    with open(file_path, "w") as f:
        f.write(content)

fix_all("/var/www/linkx-backend/service_factory/services/linkx-worker/src/batch_manager/analyzing/LA_rules_script.py")
print("Done")
