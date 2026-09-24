import sys
import os
import json

sys.path.append("/opt/linkx-worker/src")
try:
    from linkx_worker.xvigilance_consumer import _neo4j_credentials, create_neo4j_driver
    from batch_manager.analyzing.LA_rules_script import get_circular_flow_query
except ImportError:
    print("Please run this script on Node-21 using the worker's python environment.")
    sys.exit(1)

def run_test():
    credentials = _neo4j_credentials("test-session")
    driver = create_neo4j_driver(credentials)

    setup_query = """
    // TEST 1: Direct Reversal (A->B, B->A)
    CREATE (t1_a:Transactions {
        NodeId: 't1-a', session_id: 'test-session',
        LOGICAL_ACCOUNTNO: 'TEST1_A', LOGICAL_BENACCOUNTNO: 'TEST1_B',
        TRANSACTIONDATE: '2026-09-24', AMOUNT: 500, IGNORE_LOGICAL: false, PASSTHROUGH_HOPS: 0
    })
    CREATE (t1_b:Transactions {
        NodeId: 't1-b', session_id: 'test-session',
        LOGICAL_ACCOUNTNO: 'TEST1_B', LOGICAL_BENACCOUNTNO: 'TEST1_A',
        TRANSACTIONDATE: '2026-09-24', AMOUNT: 500, IGNORE_LOGICAL: false, PASSTHROUGH_HOPS: 0
    })

    // TEST 2: 1 Passthrough (A->X->B, B->A)
    CREATE (t2_a:Transactions {
        NodeId: 't2-a', session_id: 'test-session',
        LOGICAL_ACCOUNTNO: 'TEST2_A', LOGICAL_BENACCOUNTNO: 'TEST2_B',
        TRANSACTIONDATE: '2026-09-24', AMOUNT: 500, IGNORE_LOGICAL: false, PASSTHROUGH_HOPS: 1, LOGICAL_PATH: 'TEST2_A->TEST2_X->TEST2_B'
    })
    CREATE (t2_b:Transactions {
        NodeId: 't2-b', session_id: 'test-session',
        LOGICAL_ACCOUNTNO: 'TEST2_B', LOGICAL_BENACCOUNTNO: 'TEST2_A',
        TRANSACTIONDATE: '2026-09-24', AMOUNT: 500, IGNORE_LOGICAL: false, PASSTHROUGH_HOPS: 0
    })

    // TEST 3: 2 Passthroughs (A->X->B, B->Y->A)
    CREATE (t3_a:Transactions {
        NodeId: 't3-a', session_id: 'test-session',
        LOGICAL_ACCOUNTNO: 'TEST3_A', LOGICAL_BENACCOUNTNO: 'TEST3_B',
        TRANSACTIONDATE: '2026-09-24', AMOUNT: 500, IGNORE_LOGICAL: false, PASSTHROUGH_HOPS: 1, LOGICAL_PATH: 'TEST3_A->TEST3_X->TEST3_B'
    })
    CREATE (t3_b:Transactions {
        NodeId: 't3-b', session_id: 'test-session',
        LOGICAL_ACCOUNTNO: 'TEST3_B', LOGICAL_BENACCOUNTNO: 'TEST3_A',
        TRANSACTIONDATE: '2026-09-24', AMOUNT: 500, IGNORE_LOGICAL: false, PASSTHROUGH_HOPS: 1, LOGICAL_PATH: 'TEST3_B->TEST3_Y->TEST3_A'
    })
    """

    cleanup_query = """
    MATCH (n:Transactions {session_id: 'test-session'})
    DETACH DELETE n
    """

    scope_clause = "t.session_id = $session_id"
    rule_query = get_circular_flow_query(
        label="Transactions",
        scope_clause_t=scope_clause,
        trusted_pair_clause="true",
        is_provisional=False,
        incremental_batch_id=None
    )

    verify_query = """
    MATCH (a)-[r:CIRCULAR_FLOW {session_id: 'test-session'}]->(b)
    WHERE a.LOGICAL_ACCOUNTNO STARTS WITH 'TEST'
    RETURN 
        a.LOGICAL_ACCOUNTNO AS start_node,
        r.reason AS reason,
        r.passthrough_a AS hops_a,
        r.passthrough_b AS hops_b,
        r.logical_path_a AS path_a,
        r.logical_path_b AS path_b
    ORDER BY a.LOGICAL_ACCOUNTNO
    """

    with driver.session() as session:
        session.run(cleanup_query)
        session.run(setup_query)
        session.run(rule_query, session_id='test-session', pt=[])
        result = session.run(verify_query)
        records = list(result)
        
        print("--- EXTRACTED VALIDATION RESULTS ---")
        for rec in records:
            test_num = rec['start_node'].split('_')[0]
            print(f"[{test_num}] Reason: {rec['reason']}")
            print(f"[{test_num}] Hops A: {rec['hops_a']} | Path A: {rec['path_a']}")
            print(f"[{test_num}] Hops B: {rec['hops_b']} | Path B: {rec['path_b']}")
            print("-" * 30)
            
        session.run(cleanup_query)
    driver.close()

if __name__ == "__main__":
    run_test()
