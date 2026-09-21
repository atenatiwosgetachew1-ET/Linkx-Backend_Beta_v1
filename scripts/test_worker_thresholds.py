import sys
import os
import asyncio

# Setup path for linkx-worker
sys.path.append("/opt/linkx-worker/src")
from linkx_worker.xvigilance_consumer import fetch_rule_thresholds, fetch_global_entities, run_full_graph_analysis
from batch_manager.services.risk_scoring_kafka_service import _neo4j_credentials

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
        session_id = "158144"
        credentials = _neo4j_credentials(session_id)
        # We pass a fake node_label so it safely runs against an empty partition
        run_full_graph_analysis(credentials, session_id, "TestPartition")
        print("✅ Graph analysis executed successfully with the dynamic thresholds!")
    except Exception as e:
        print(f"❌ Error during graph analysis: {e}")

if __name__ == "__main__":
    test_thresholds()
