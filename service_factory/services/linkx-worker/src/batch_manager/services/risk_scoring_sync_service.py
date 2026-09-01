"""
Synchronous Risk Scoring Worker Handler.

This handler is invoked by the worker when a job of type 'risk_scoring_sync'
is claimed from the queue. It runs the full LinkX graph analysis pipeline
and returns the result directly — no webhook callback is fired.
"""

import time
import json
from batch_manager.utils.postgres_utils import get_postgres_connection
from batch_manager.utils.neo4j_utils import create_neo4j_driver, load_session_neo4j_credentials


def process_sync_job(payload):
    account_no = payload.get("account_no")
    response_type = str(payload.get("response_type") or "flagged").lower()

    if not account_no:
        return {"status": "failed", "error": "Missing account_no"}

    start_time = time.time()
    
    # Ensure trace_id is unique per run to avoid overwriting graphs if called rapidly
    trace_id = f"sync-{account_no}-{int(time.time())}"

    analysis_payload = {
        "meta": {
            "trace_id": trace_id,
            "correlation_id": trace_id,
        },
        "data": {
            "entity_id": account_no,
            "analysis_type": "transaction_graph",
        },
    }

    try:
        from batch_manager.services.risk_scoring_kafka_service import execute_formal_link_analysis

        response_event, status = execute_formal_link_analysis(analysis_payload)

        if not response_event or not response_event.get("data"):
            duration_ms = round((time.time() - start_time) * 1000, 2)
            return {
                "status": "failed",
                "account_no": account_no,
                "error": str(status),
                "processing": {"duration_ms": duration_ms},
            }

        findings = response_event["data"]
        
        # Cleanup fields we are replacing
        findings.pop("linked_entities", None)
        findings.pop("flagged_entity_links", None)
        
        # Extract the exact session_id used for ingestion
        session_id = response_event.get("meta", {}).get("session_id")
        if not session_id:
            raise ValueError("execute_formal_link_analysis did not return a session_id in meta")
        
        # Connect to Neo4j to pull exact nodes & edges
        nodes_dict = {}
        edges_list = []
        flagged_rels_set = set()
        all_rels_count = 0

        try:
            credentials = load_session_neo4j_credentials(session_id, purpose="graph_fetch")
            driver = create_neo4j_driver(credentials)
            
            with driver.session() as session:
                # 1. Total relationship count
                count_res = session.run(
                    "MATCH ()-[r]->() WHERE r.session_id = $session_id RETURN count(r) AS c",
                    session_id=session_id
                )
                record = count_res.single()
                if record:
                    all_rels_count = record["c"]
                
                # 2. Get relationships based on response_type
                if response_type == "full":
                    query = """
                    MATCH (n)-[r]->(m)
                    WHERE r.session_id = $session_id
                    RETURN n, r, m
                    """
                else:
                    query = """
                    MATCH (n)-[r]->(m)
                    WHERE r.session_id = $session_id
                      AND coalesce(r.is_flagged, false) = true
                    RETURN n, r, m
                    """
                
                for record in session.run(query, session_id=session_id):
                    a = record["n"]
                    b = record["m"]
                    r = record["r"]
                    
                    r_props = dict(r)
                    if r_props.get("is_flagged") is True:
                        flagged_rels_set.add(r.type)

                    a_id = getattr(a, "element_id", str(a.id))
                    if a_id not in nodes_dict:
                        nodes_dict[a_id] = {"id": a_id, "label": a.get("BENACCOUNTNO") or a.get("ACCOUNTNO") or a_id, **dict(a)}
                    
                    b_id = getattr(b, "element_id", str(b.id))
                    if b_id not in nodes_dict:
                        nodes_dict[b_id] = {"id": b_id, "label": b.get("BENACCOUNTNO") or b.get("ACCOUNTNO") or b_id, **dict(b)}
                    
                    edges_list.append({
                        "id": getattr(r, "element_id", f"{a_id}_{b_id}"),
                        "from": a_id,
                        "to": b_id,
                        "label": r.type,
                        **r_props
                    })
                    
            driver.close()
        except Exception as neo_exc:
            print(f"[RiskScoringSync] Failed to fetch graph from neo4j: {neo_exc}")
        
        findings["all_relationships"] = all_rels_count
        findings["flagged_relationships"] = list(flagged_rels_set)
        
        if not nodes_dict and not edges_list:
            findings["graph_entities"] = {}
        else:
            findings["graph_entities"] = {
                "nodes": list(nodes_dict.values()),
                "edges": edges_list
            }

        duration_ms = round((time.time() - start_time) * 1000, 2)
        return {
            "status": "success",
            "account_no": account_no,
            "source": "link",
            "data": findings,
            "processing": {"duration_ms": duration_ms},
        }

    except Exception as e:
        duration_ms = round((time.time() - start_time) * 1000, 2)
        print(f"[RiskScoringSync] Analysis failed for {account_no}: {e}")
        return {
            "status": "failed",
            "account_no": account_no,
            "error": str(e),
            "processing": {"duration_ms": duration_ms},
        }
