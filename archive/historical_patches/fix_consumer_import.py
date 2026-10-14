with open('/var/www/linkx-backend/service_factory/services/linkx-worker/src/linkx_worker/xvigilance_consumer.py', 'r') as f:
    text = f.read()

import_str = """
from batch_manager.analyzing.LA_rules_script import (
    get_smurfing_query,
    get_circular_flow_query,
    get_fund_flow_query,
    get_dormant_to_active_query,
    get_abnormal_balance_query,
    get_hub_and_spoke_out_query,
    get_hub_and_spoke_in_query,
    get_shared_identifier_query,
    get_rapid_withdrawal_query,
    get_account_activity_spike_query,
    get_high_risk_link_query
)
"""

text = text.replace("import sys", "import sys\n" + import_str, 1)

with open('/var/www/linkx-backend/service_factory/services/linkx-worker/src/linkx_worker/xvigilance_consumer.py', 'w') as f:
    f.write(text)

