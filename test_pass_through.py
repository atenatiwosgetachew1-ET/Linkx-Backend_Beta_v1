import sys
import os
import datetime

api_path = os.path.join(os.path.dirname(__file__), 'service_factory/services/linkx-api/src')
sys.path.insert(0, api_path)
worker_path = os.path.join(os.path.dirname(__file__), 'service_factory/services/linkx-worker/src')
sys.path.insert(0, worker_path)

from batch_manager.config_defaults import _auto_load_dotenv
_auto_load_dotenv()

from batch_manager.utils.elastic_utils import es_keyword_search
from linkx_worker.xvigilance_consumer import (
    fast_ingest_batch,
    run_full_graph_analysis,
    fetch_global_entities,
    _extract_pass_through_accounts
)
import pandas as pd

def run_test():
    api_url = os.getenv("LINKX_ELASTIC_API_URL", "http://172.27.23.106:9200") # fallback
    auth_header = os.getenv("ELASTIC_AUTH_HEADER", None)
    
    print("\n--- 1. Fetching Classified Pass-Through Accounts ---")
    entities = fetch_global_entities()
    pass_throughs = _extract_pass_through_accounts(entities)
    print(f"Found {len(pass_throughs)} pass-throughs in database.")
    
    print("\n--- 2. Fetching Real Transactions from Elasticsearch ---")
    # We query ES for accounts that match Feres Wallet Kaafi (338766) or Gode Supermarket (600035) or ALJAZEERA (785238)
    accounts_to_find = ["338766", "600035", "785238"]
    
    all_rows = []
    for acc in accounts_to_find:
        print(f"Searching ES for account: {acc}...")
        try:
            res_sender = es_keyword_search("test", api_url, acc, "ACCOUNTNO", "strict", "TRANSACTIONDATE", limit=50, auth_header=auth_header)
            res_receiver = es_keyword_search("test", api_url, acc, "BENACCOUNTNO", "strict", "TRANSACTIONDATE", limit=50, auth_header=auth_header)
            all_rows.extend(res_sender)
            all_rows.extend(res_receiver)
        except Exception as e:
            print(f"Failed to search ES: {e}")

    if not all_rows:
        print("No transactions found! Generating mock data instead to test Neo4j...")
        now = datetime.datetime.now(datetime.UTC)
        all_rows = [
            {"ACCOUNTNO": "CRIMINAL_1", "BENACCOUNTNO": "338766", "AMOUNT": "10000", "TRANSACTIONDATE": now.strftime("%Y-%m-%d"), "TRANSACTIONTIME": "10:00:00"},
            {"ACCOUNTNO": "338766", "BENACCOUNTNO": "HUB_WALLET", "AMOUNT": "9900", "TRANSACTIONDATE": now.strftime("%Y-%m-%d"), "TRANSACTIONTIME": "10:10:00"},
            {"ACCOUNTNO": "CRIMINAL_2", "BENACCOUNTNO": "600035", "AMOUNT": "8000", "TRANSACTIONDATE": now.strftime("%Y-%m-%d"), "TRANSACTIONTIME": "10:15:00"},
            {"ACCOUNTNO": "600035", "BENACCOUNTNO": "HUB_WALLET", "AMOUNT": "7900", "TRANSACTIONDATE": now.strftime("%Y-%m-%d"), "TRANSACTIONTIME": "10:20:00"},
        ]
        
    df = pd.DataFrame(all_rows).drop_duplicates()
    print(f"Compiled {len(df)} transactions.")
    
    print("\n--- 3. Pushing to Neo4j ---")
    session_id = "TEST_EFFECTIVE_FLOW_888"
    credentials = {"session_id": session_id}
    node_label = f"TestPassThrough_{session_id}"
    
    fast_ingest_batch(credentials, session_id, df, 1, node_label)
    print(f"Ingested into Neo4j with label '{node_label}'")
    
    print("\n--- 4. Running Graph Analysis ---")
    run_full_graph_analysis(credentials, session_id, node_label)
    
    print(f"\n✅ Analysis complete!")
    print(f"You can now open Neo4j Browser and run:")
    print(f"MATCH (n:{node_label}) RETURN n LIMIT 50")
    print(f"Or to view the flagged EFFECTIVE_FLOW anomalies:")
    print(f"MATCH (s:{node_label})-[r:EFFECTIVE_FLOW]->(t:{node_label}) RETURN s, r, t")

if __name__ == "__main__":
    run_test()
