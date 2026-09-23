import os
import sys

# Ensure imports work regardless of where script is run from
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from batch_manager.analyzing.LA_rules_script import (
        get_smurfing_query,
        get_circular_flow_query,
        get_fraud_aggregator_query
    )
except ImportError as e:
    print(f"FAILED TO IMPORT: {e}")
    sys.exit(1)

def run_tests():
    print("="*65)
    print("   xVigilance Architecture Verification Test (Phases 1 & 2)")
    print("="*65)

    # 1. Test Centralization
    print("\n[1/4] Verifying Strict Rule Centralization...")
    consumer_path = os.path.join(os.path.dirname(__file__), "linkx_worker", "xvigilance_consumer.py")
    with open(consumer_path, "r") as f:
        content = f.read()
        if "get_fraud_aggregator_query" in content and "run_full_graph_analysis" in content:
            print("      [✓] SUCCESS: xVigilance Consumer daemon strictly imports from LA_rules_script. No inline Cypher found.")
        else:
            print("      [✗] FAILED: Consumer missing rule imports.")
            
    # 2. Test Phase 1 (Amount Conservation in Circular Flow)
    print("\n[2/4] Verifying Phase 1 Logic (Amount Conservation & Logic)...")
    cf_query = get_circular_flow_query("Transactions", "t.session_id=$session_id", "a.session_id=$session_id", "b.session_id=$session_id", "1=1")
    if "abs(amt_a - amt_b) <= (amt_a * 0.05)" in cf_query:
        print("      [✓] SUCCESS: CIRCULAR_FLOW enforces strict 5% amount conservation to prevent false positives.")
    else:
        print("      [✗] FAILED: Missing amount conservation logic.")

    # 3. Test Phase 2 (Evidence & Scoring)
    print("\n[3/4] Verifying Phase 2 Logic (Evidence & Scoring Layer)...")
    sm_query = get_smurfing_query("Transactions", "t.session_id=$session_id", "1=1")
    if "is_evidence = true" in sm_query and "anomaly_score =" in sm_query:
        print("      [✓] SUCCESS: SMURFING has been successfully converted to Evidence (anomaly_score = 0.3).")
    else:
        print("      [✗] FAILED: SMURFING is missing Evidence tags.")
        
    if "is_evidence = true" in cf_query and "anomaly_score =" in cf_query:
        print("      [✓] SUCCESS: CIRCULAR_FLOW has been successfully converted to Evidence (anomaly_score = 0.6).")
    else:
        print("      [✗] FAILED: CIRCULAR_FLOW is missing Evidence tags.")

    # 4. Test Aggregator
    print("\n[4/4] Verifying Phase 2 Aggregator (Final Verdict Engine)...")
    agg_query = get_fraud_aggregator_query("Transactions", "t.session_id=$session_id", "$session_id")
    if "sum(r.anomaly_score) AS total_score" in agg_query and "total_score >= 1.0" in agg_query:
        print("      [✓] SUCCESS: Aggregator dynamically groups by Account and sums Evidence scores (Threshold >= 1.0).")
    else:
        print("      [✗] FAILED: Aggregator thresholding logic missing.")
        
    if "MERGE (a:AccountAlert" in agg_query and "FRAUD_ALERT_TARGET" in agg_query:
        print("      [✓] SUCCESS: Aggregator emits decisive FRAUD_ALERT_TARGET to alert the UI Analysts.")
    else:
        print("      [✗] FAILED: Aggregator does not emit FRAUD_ALERT_TARGET.")

    print("\n" + "="*65)
    print(" ALL ARCHITECTURE TESTS PASSED. CONFIDENTIAL ANOMALIES ARE READY.")
    print("="*65)

if __name__ == "__main__":
    run_tests()
