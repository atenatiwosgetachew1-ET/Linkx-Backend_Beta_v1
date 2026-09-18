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
    fetch_global_entities,
    _extract_pass_through_accounts
)
import pandas as pd

def run_test():
    print("\n--- 1. Fetching Classified Pass-Through Accounts ---")
    entities = fetch_global_entities()
    pass_throughs = _extract_pass_through_accounts(entities)
    print(f"Found {len(pass_throughs)} pass-throughs in database.")
    
    if len(pass_throughs) == 0:
        print("Warning: Did not find pass-throughs in DB. We will temporarily mock them for this test.")
        entities = {
            "trusted_entities": [
                {"key": "ACCOUNTNO", "value": "338766", "pass_through": True, "name": "Feres Wallet Kaafi"},
                {"key": "ACCOUNTNO", "value": "600035", "pass_through": True, "name": "GODE SUPERMARKET"},
                {"key": "ACCOUNTNO", "value": "785238", "pass_through": True, "name": "ALJAZEERA Restaurant"}
            ]
        }
    
    print("\n--- 2. Generating Target Transactions ---")
    now = datetime.datetime.now(datetime.UTC)
    
    all_rows = [
        {"ACCOUNTNO": "CRIMINAL_1", "BENACCOUNTNO": "338766", "AMOUNT": "10000", "TRANSACTIONDATE": now.strftime("%Y-%m-%d"), "TRANSACTIONTIME": "10:00:00"},
        {"ACCOUNTNO": "338766", "BENACCOUNTNO": "CENTRAL_HUB", "AMOUNT": "9900", "TRANSACTIONDATE": now.strftime("%Y-%m-%d"), "TRANSACTIONTIME": "10:10:00"},
        {"ACCOUNTNO": "CRIMINAL_2", "BENACCOUNTNO": "600035", "AMOUNT": "15000", "TRANSACTIONDATE": now.strftime("%Y-%m-%d"), "TRANSACTIONTIME": "10:15:00"},
        {"ACCOUNTNO": "600035", "BENACCOUNTNO": "CENTRAL_HUB", "AMOUNT": "14900", "TRANSACTIONDATE": now.strftime("%Y-%m-%d"), "TRANSACTIONTIME": "10:20:00"},
        {"ACCOUNTNO": "CRIMINAL_3", "BENACCOUNTNO": "785238", "AMOUNT": "5000", "TRANSACTIONDATE": now.strftime("%Y-%m-%d"), "TRANSACTIONTIME": "10:25:00"},
        {"ACCOUNTNO": "785238", "BENACCOUNTNO": "CENTRAL_HUB", "AMOUNT": "4900", "TRANSACTIONDATE": now.strftime("%Y-%m-%d"), "TRANSACTIONTIME": "10:30:00"},
        {"ACCOUNTNO": "CENTRAL_HUB", "BENACCOUNTNO": "CASHOUT_WALLET", "AMOUNT": "29000", "TRANSACTIONDATE": now.strftime("%Y-%m-%d"), "TRANSACTIONTIME": "11:00:00"},
    ]
        
    df = pd.DataFrame(all_rows)
    print(f"Compiled {len(df)} transactions.")
    
    print("\n--- 3. Pushing to Neo4j ---")
    
    # Grab Neo4j credentials from the live active session 185230 as requested!
    existing_config = load_session_config("185230") or {}
    credentials = existing_config.get("active_storage_address")
    
    if not credentials:
        print("ERROR: Could not find active_storage_address in session 185230!")
        sys.exit(1)
        
    session_id = "TEST_EFFECTIVE_FLOW_888"
    credentials["session_id"] = session_id
    node_label = "TestPassThrough"
    
    import linkx_worker.xvigilance_consumer
    linkx_worker.xvigilance_consumer.fetch_global_entities = lambda: entities
    
    fast_ingest_batch(credentials, session_id, df, 1, node_label)
    print(f"Ingested into Neo4j with label '{node_label}'")
    
    print("\n--- 4. Running Graph Analysis ---")
    run_full_graph_analysis(credentials, session_id, node_label)
    
    print(f"\n✅ Analysis complete!")
    print(f"You can now open Neo4j Browser and run:")
    print(f"MATCH (n:{node_label}) RETURN n")
    print(f"MATCH (s:{node_label})-[r:EFFECTIVE_FLOW]->(t:{node_label}) RETURN s, r, t")

if __name__ == "__main__":
    run_test()
