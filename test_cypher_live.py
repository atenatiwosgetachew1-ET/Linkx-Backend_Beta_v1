import sys
import os
from neo4j import GraphDatabase

sys.path.insert(0, '/var/www/linkx-backend/service_factory/services/linkx-worker/src')
from batch_manager.analyzing.LA_rules_script import get_circular_flow_query, get_fund_flow_query

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "test1234"))

q_circ = get_circular_flow_query(
    label="bank_transactions",
    scope_clause_t="t.session_id = 'test'",
    scope_clause_a="a.session_id = 'test'",
    scope_clause_b="b.session_id = 'test'",
    trusted_pair_clause="TRUE",
    is_provisional=False
)
q_fund = get_fund_flow_query(
    label="bank_transactions",
    scope_clause_t="t.session_id = 'test'",
    scope_clause_a="a.session_id = 'test'",
    scope_clause_b="b.session_id = 'test'",
    trusted_pair_clause="TRUE",
    is_provisional=False
)

with driver.session() as session:
    try:
        session.run("EXPLAIN " + q_circ, pt=[], session_id="test")
        print("CIRCULAR FLOW: OK!")
    except Exception as e:
        print("CIRCULAR FLOW: ERROR:", e)
        
    try:
        session.run("EXPLAIN " + q_fund, pt=[], session_id="test")
        print("FUND FLOW: OK!")
    except Exception as e:
        print("FUND FLOW: ERROR:", e)

