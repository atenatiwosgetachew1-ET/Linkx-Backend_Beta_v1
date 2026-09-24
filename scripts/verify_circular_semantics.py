import sys
import os
import json

# Add worker src to path so we can import the rule generator
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

    # 1. Create dummy data simulating A -> X -> B and B -> Y -> A
    # We assign them PASSTHROUGH_HOPS and LOGICAL_PATH as if the logical layer already processed them
    setup_query = """
    CREATE (a:Transactions {
        NodeId: 'test-1',
        session_id: 'test-session',
        batch_id: 'test-batch',
        ACCOUNTNO: 'USER_A',
        BENACCOUNTNO: 'USER_X',
        LOGICAL_ACCOUNTNO: 'USER_A',
        LOGICAL_BENACCOUNTNO: 'USER_B',
        TRANSACTIONDATE: '2026-09-24',
        AMOUNT: 500,
        IGNORE_LOGICAL: false,
        PASSTHROUGH_HOPS: 1,
        LOGICAL_PATH: 'USER_A->USER_X->USER_B'
    })
    CREATE (b:Transactions {
        NodeId: 'test-2',
        session_id: 'test-session',
        batch_id: 'test-batch',
        ACCOUNTNO: 'USER_B',
        BENACCOUNTNO: 'USER_Y',
        LOGICAL_ACCOUNTNO: 'USER_B',
        LOGICAL_BENACCOUNTNO: 'USER_A',
        TRANSACTIONDATE: '2026-09-24',
        AMOUNT: 500,
        IGNORE_LOGICAL: false,
        PASSTHROUGH_HOPS: 1,
        LOGICAL_PATH: 'USER_B->USER_Y->USER_A'
    })
    """

    cleanup_query = """
    MATCH (n:Transactions {session_id: 'test-session'})
    DETACH DELETE n
    """

    # Generate the actual rule query dynamically just like the daemon does
    scope_clause = "t.session_id = $session_id"
    rule_query = get_circular_flow_query(
        label="Transactions",
        scope_clause_t=scope_clause,
        trusted_pair_clause="true", # bypass trusted check for test
        is_provisional=False,
        incremental_batch_id=None
    )

    verify_query = """
    MATCH (a)-[r:CIRCULAR_FLOW {session_id: 'test-session'}]->(b)
    RETURN 
        r.reason AS reason,
        r.passthrough_a AS hops_a,
        r.passthrough_b AS hops_b,
        r.logical_path_a AS path_a,
        r.logical_path_b AS path_b,
        r.amount_a AS amt_a,
        r.amount_b AS amt_b
    """

    with driver.session() as session:
        # Clean first just in case
        session.run(cleanup_query)
        
        # Insert dummy logical flows
        print("1. Injecting dummy logical pass-through transactions...")
        session.run(setup_query)
        
        # Run the Circular Flow rule
        print("2. Running the CIRCULAR_FLOW rule engine...")
        session.run(rule_query, session_id='test-session', pt=[])
        
        # Verify the generated edges
        print("3. Extracting generated relationships...")
        result = session.run(verify_query)
        records = list(result)
        
        print(f"\nFound {len(records)} derived CIRCULAR_FLOW edges. Validating semantics:\n")
        for idx, rec in enumerate(records):
            print(f"--- Edge {idx + 1} ---")
            print(f"Reason: {rec['reason']}")
            print(f"Passthrough Hops A: {rec['hops_a']}")
            print(f"Logical Path A:     {rec['path_a']}")
            print(f"Passthrough Hops B: {rec['hops_b']}")
            print(f"Logical Path B:     {rec['path_b']}")
            print("-" * 20)
            
        # Clean up
        print("\n4. Cleaning up test data...")
        session.run(cleanup_query)
        print("Done.")

    driver.close()

if __name__ == "__main__":
    run_test()
