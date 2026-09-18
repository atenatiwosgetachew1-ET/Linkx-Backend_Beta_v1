import sys
import os
import datetime

api_path = os.path.join(os.path.dirname(__file__), 'service_factory/services/linkx-api/src')
sys.path.insert(0, api_path)
worker_path = os.path.join(os.path.dirname(__file__), 'service_factory/services/linkx-worker/src')
sys.path.insert(0, worker_path)

from batch_manager.config_defaults import _auto_load_dotenv
_auto_load_dotenv()

from session_config_store import load_session_config
from linkx_worker.xvigilance_consumer import (
    fast_ingest_batch,
    run_full_graph_analysis,
    _extract_pass_through_accounts
)
import linkx_worker.xvigilance_consumer as xc
import pandas as pd

def run_test():
    print("\n--- 1. Overriding DB with Hardcoded Pass-Through Accounts ---")
    entities = {
        "trusted_entities": [
            {"key": "ACCOUNTNO", "value": "338766", "pass_through": True, "name": "Feres Wallet Kaafi"},
            {"key": "ACCOUNTNO", "value": "600035", "pass_through": True, "name": "GODE SUPERMARKET"},
            {"key": "ACCOUNTNO", "value": "785238", "pass_through": True, "name": "ALJAZEERA Restaurant"}
        ]
    }
    # Override the module-level function explicitly without unittest.mock
    xc.fetch_global_entities = lambda: entities
    
    # Let's verify it worked!
    test_fetch = xc.fetch_global_entities()
    print(f"Mock verify: {len(test_fetch.get('trusted_entities', []))} trusted entities")
    
    pass_throughs = _extract_pass_through_accounts(test_fetch)
    print(f"Found {len(pass_throughs)} pass-throughs extracted.")

    # WAIT! We also need to fix the Neo4j query bug where pass_through_accounts is passed as a list of dicts!
    # The neo4j query expects a list of values, e.g. ['338766', '600035']
    # If the backend code in xvigilance_consumer.py passes the list of dicts, it will fail silently in Neo4j!
    # I will patch xvigilance_consumer.py itself to fix that bug!

if __name__ == "__main__":
    run_test()
