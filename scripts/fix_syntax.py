#!/usr/bin/env python3
import sys

RULES_FILE = "/var/www/linkx-backend/service_factory/services/linkx-worker/src/batch_manager/analyzing/LA_rules_script.py"
with open(RULES_FILE, "r") as f:
    content = f.read()

# Fix CIRCULAR FLOW
content = content.replace(
"""    }}
    YIELD ben_out_count
    WHERE ben_out_count < 1000""",
"""    }}
    WITH acc, a, ben_out_count
    WHERE ben_out_count < 1000""")

# Fix FUND FLOW
content = content.replace(
"""    }}
    YIELD a_sender_count
    WHERE a_sender_count < 1000""",
"""    }}
    WITH acc, a, a_sender_count
    WHERE a_sender_count < 1000""")

with open(RULES_FILE, "w") as f:
    f.write(content)
