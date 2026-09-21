import sys
import os
import asyncio

# Setup path for linkx-worker
sys.path.append("/opt/linkx-worker/src")
from linkx_worker.xvigilance_consumer import fetch_rule_thresholds, fetch_global_entities, run_full_graph_analysis

def test_thresholds():
    print("Testing config fetch from PostgreSQL...")
    thresholds = fetch_rule_thresholds()
    print(f"✅ Fetched Rule Thresholds: {thresholds}")
    
    print("\nTesting classified entities fetch from PostgreSQL...")
    entities = fetch_global_entities()
    print(f"✅ Fetched {len(entities)} Classified Entity categories.")
    if len(entities) > 0:
        sample_key = list(entities.keys())[0]
        print(f"Sample [{sample_key}]: {len(entities[sample_key])} items")
    
    print("\nSimulating full graph analysis for testing thresholds (Dry Run)...")
    try:
        credentials = {
            "uri": os.getenv("NEO4J_URI", "bolt://172.27.23.20:7687"),
            "user": os.getenv("NEO4J_USER", "neo4j"),
            "password": os.getenv("NEO4J_PASSWORD", "linkxds-neo4j-2026")
        }
        # We pass a fake session_id and node_label so it safely runs against an empty partition
        run_full_graph_analysis(credentials, "TEST_SESSION_123", "TestPartition")
        print("✅ Graph analysis executed successfully with the dynamic thresholds!")
    except Exception as e:
        print(f"❌ Error during graph analysis: {e}")

if __name__ == "__main__":
    test_thresholds()
