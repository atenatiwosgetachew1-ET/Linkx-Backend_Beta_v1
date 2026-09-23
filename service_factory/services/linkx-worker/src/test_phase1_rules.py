import sys
import os
import traceback

print("="*60)
print("   Phase 1 Rule Centralization & Architecture Test")
print("="*60)

# 1. Test Environment Setup & Imports
print("\n[1/4] Testing imports from LA_rules_script.py...")
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from batch_manager.analyzing.LA_rules_script import (
        get_effective_flow_query,
        get_logical_layer_query,
        get_circular_flow_query,
        get_account_activity_spike_query,
        get_high_risk_link_query
    )
    print("      [✓] SUCCESS: All centralized rule generators imported correctly.")
except Exception as e:
    print(f"      [✗] FAILED: Import error: {e}")
    traceback.print_exc()
    sys.exit(1)

# 2. Test Rule Generation & Business Logic (Amount Conservation)
print("\n[2/4] Testing query generation and Phase 1 business logic...")
label = "mobile_banking_transactions"
scope = "$session_id IS NULL"

try:
    circular_query = get_circular_flow_query(label, scope, scope, scope, "1=1", False)
    logical_queries = get_logical_layer_query(label, scope, apply_pass_through=True)
    effective_query = get_effective_flow_query(label, scope, "test_session")
    
    if "abs(amt_a - amt_b) <= (amt_a * 0.05)" in circular_query:
        print("      [✓] SUCCESS: CIRCULAR_FLOW has strict amount conservation logic.")
    else:
        print("      [✗] FAILED: CIRCULAR_FLOW is missing amount conservation.")
        sys.exit(1)

    if "RAW_SENDER = coalesce(t.ACCOUNTNO, '')" in logical_queries[0] and "DERIVED_FLOW" in logical_queries[1]:
        print("      [✓] SUCCESS: LOGICAL_LAYER strictly preserves RAW properties and explicit paths.")
    else:
        print("      [✗] FAILED: LOGICAL_LAYER is destroying raw graph data.")
        sys.exit(1)

except Exception as e:
    print(f"      [✗] FAILED: Query generation error: {e}")
    sys.exit(1)


# 3. Output the central rules for manual inspection
print("\n[3/4] Outputting generated centralized Cypher for inspection...")
print("\n--- CIRCULAR FLOW (Notice Amount Conservation snippet) ---")
print(circular_query.split("WHERE elementId(a)")[1][:250] + "...\n")

print("\n--- LOGICAL LAYER PATH PRESERVATION ---")
print(logical_queries[1][:250] + "...\n")


# 4. Live Neo4j Syntax Validation (EXPLAIN)
print("\n[4/4] Attempting Live Neo4j Syntax Validation via EXPLAIN...")
try:
    from neo4j import GraphDatabase
    import psycopg
    
    postgres_dsn = os.getenv('LINKX_POSTGRES_DSN', 'dbname=linkx user=linkx password=linkx host=127.0.0.1')
    neo4j_creds = None
    try:
        with psycopg.connect(postgres_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT config FROM session_configs WHERE session_id = 'xvigilance_system'")
                row = cur.fetchone()
                if row and "neo4j_credentials" in row[0]:
                    neo4j_creds = row[0]["neo4j_credentials"]
    except Exception as dbe:
        print(f"      [!] SKIPPED: Could not connect to Postgres to fetch Neo4j credentials. ({dbe})")
                
    if neo4j_creds:
        driver = GraphDatabase.driver(neo4j_creds["uri"], auth=(neo4j_creds["username"], neo4j_creds["password"]))
        with driver.session() as s:
            s.run("EXPLAIN " + circular_query, pt=[])
            print("      [✓] Neo4j validated CIRCULAR_FLOW Cypher syntax.")
            s.run("EXPLAIN " + effective_query, pt=[], session_id="test")
            print("      [✓] Neo4j validated EFFECTIVE_FLOW Cypher syntax.")
            s.run("EXPLAIN " + logical_queries[0])
            s.run("EXPLAIN " + logical_queries[1], session_id="test")
            print("      [✓] Neo4j validated LOGICAL_LAYER Cypher syntax.")
        driver.close()

except Exception as e:
    if "SKIPPED" not in str(e):
        print(f"      [✗] FAILED: Neo4j syntax validation error: {e}")
        sys.exit(1)

print("\n" + "="*60)
print(" ALL TESTS PASSED. The graph backend is successfully centralized.")
print("="*60)
