"""
Synchronous Risk Scoring Worker Handler.

This handler is invoked by the worker when a job of type 'risk_scoring_sync'
is claimed from the queue. It runs the full LinkX graph analysis pipeline
and returns the result directly — no webhook callback is fired.
"""

import time
import json
from batch_manager.utils.postgres_utils import get_postgres_connection
from batch_manager.utils.neo4j_utils import create_neo4j_driver


def process_sync_job(payload):
    entity_id = payload.get("entity_id")
    entity_type = payload.get("entity_type") or "accountno"
    response_type = str(payload.get("response_type") or "flagged").lower()

    if not entity_id:
        return {"status": "failed", "error": "Missing entity_id"}

    start_time = time.time()
    
    # Ensure trace_id is unique per run to avoid overwriting graphs if called rapidly
    trace_id = f"sync-{entity_id}-{int(time.time())}"
    
    # --- 3-MINUTE CACHE CHECK ---
    try:
        from batch_manager.utils.postgres_utils import get_postgres_connection
        with get_postgres_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                SELECT response_payload
                FROM link_analysis_evidence
                WHERE entity_id = %s 
                  AND entity_type = %s
                  AND event_type = %s
                  AND analyzed_at >= NOW() - INTERVAL '3 minutes'
                ORDER BY analyzed_at DESC
                LIMIT 1
                """, (str(entity_id), str(entity_type), f"sync.{response_type}"))
                row = cur.fetchone()
                if row and row[0]:
                    cached_response = row[0]
                    duration_ms = round((time.time() - start_time) * 1000, 2)
                    if "processing" in cached_response:
                        cached_response["processing"]["duration_ms"] = duration_ms
                        cached_response["processing"]["cached"] = True
                    print(f"[RiskScoringSync] Cache hit for {entity_type}={entity_id} (response_type={response_type})")
                    return cached_response
    except Exception as e:
        print(f"[RiskScoringSync] Cache read error: {e}")
    # ----------------------------

    analysis_payload = {
        "meta": {
            "trace_id": trace_id,
            "correlation_id": trace_id,
            "aggregation_key": {
                "type": entity_type,
                "value": entity_id
            }
        },
        "data": {
            "entity_id": entity_id,
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
                "entity_id": entity_id,
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
            
        # Connect to Neo4j to pull exact nodes & edges via the existing graph fetch endpoint logic
        from batch_manager.analyzing.LA_graphs_script import fetch_graph
        
        # Step 5: Fetch graph using the exact same function the UI uses
        # We pass rel_type="*" to get all relationships, then filter down if response_type is "flagged"
        graph_result = fetch_graph("relationship", "fetch", session_id, "*", batch="")
        
        if graph_result.get("error"):
            print(f"[RiskScoringSync] Warning: Graph fetch returned error: {graph_result['error']}")
            
        all_nodes = graph_result.get("nodes", [])
        all_edges = graph_result.get("edges", [])
        
        all_rels_count = len(all_edges)
        flagged_rels_set = {e["label"] for e in all_edges if e.get("is_flagged") is True}
        
        if response_type == "flagged":
            filtered_edges = [e for e in all_edges if e.get("is_flagged") is True]
            connected_node_ids = set()
            for e in filtered_edges:
                connected_node_ids.add(e["from"])
                connected_node_ids.add(e["to"])
            filtered_nodes = [n for n in all_nodes if n["id"] in connected_node_ids]
            
            graph_entities = {
                "nodes": filtered_nodes,
                "edges": filtered_edges
            }
        else:
            graph_entities = {
                "nodes": all_nodes,
                "edges": all_edges
            }
            
        if not graph_entities["nodes"] and not graph_entities["edges"]:
            findings["graph_entities"] = {}
        else:
            findings["graph_entities"] = graph_entities

        findings["all_relationships"] = all_rels_count
        findings["flagged_relationships"] = list(flagged_rels_set)

        duration_ms = round((time.time() - start_time) * 1000, 2)
        final_response = {
            "status": "success",
            "entity_id": entity_id,
            "source": "link",
            "data": findings,
            "processing": {"duration_ms": duration_ms},
        }

        # --- CACHE WRITE ---
        try:
            with get_postgres_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                    INSERT INTO link_analysis_evidence (
                        trace_id, correlation_id, entity_id, entity_type,
                        session_id, event_type, is_flagged, linked_accounts_count,
                        request_payload, response_payload, duration_ms, analyzed_at
                    ) VALUES (
                        %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s::jsonb, %s::jsonb, %s, NOW()
                    )
                    """, (
                        trace_id, trace_id, str(entity_id), str(entity_type),
                        session_id, f"sync.{response_type}", bool(flagged_rels_set), len(all_nodes),
                        json.dumps({"entity_id": entity_id, "entity_type": entity_type, "response_type": response_type}),
                        json.dumps(final_response),
                        duration_ms
                    ))
                conn.commit()
        except Exception as e:
            print(f"[RiskScoringSync] Cache write error: {e}")
        # -------------------

        return final_response

    except Exception as e:
        duration_ms = round((time.time() - start_time) * 1000, 2)
        print(f"[RiskScoringSync] Analysis failed for {entity_id}: {e}")
        return {
            "status": "failed",
            "entity_id": entity_id,
            "error": str(e),
            "processing": {"duration_ms": duration_ms},
        }
