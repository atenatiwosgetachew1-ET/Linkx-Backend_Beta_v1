#!/usr/bin/env python3
"""Apply P1 fixes and CIRCULAR_FLOW/FUND_FLOW performance fixes"""

RULES_FILE = "/var/www/linkx-backend/service_factory/services/linkx-worker/src/batch_manager/analyzing/LA_rules_script.py"
with open(RULES_FILE, "r") as f:
    rules_content = f.read()

# Fix f-string brace escaping for the subqueries
rules_content = rules_content.replace(
"""    CALL {
       WITH a
       MATCH (check:{label}) 
       WHERE check.ACCOUNTNO = a.LOGICAL_BENACCOUNTNO
       RETURN count(check) AS ben_out_count
    }""", 
"""    CALL {{
       WITH a
       MATCH (check:{label}) 
       WHERE check.ACCOUNTNO = a.LOGICAL_BENACCOUNTNO
       RETURN count(check) AS ben_out_count
    }}""")

rules_content = rules_content.replace(
"""    CALL {
       WITH a
       MATCH (check:{label}) 
       WHERE check.LOGICAL_BENACCOUNTNO = a.ACCOUNTNO
       RETURN count(check) AS a_sender_count
    }""",
"""    CALL {{
       WITH a
       MATCH (check:{label}) 
       WHERE check.LOGICAL_BENACCOUNTNO = a.ACCOUNTNO
       RETURN count(check) AS a_sender_count
    }}""")

with open(RULES_FILE, "w") as f:
    f.write(rules_content)
