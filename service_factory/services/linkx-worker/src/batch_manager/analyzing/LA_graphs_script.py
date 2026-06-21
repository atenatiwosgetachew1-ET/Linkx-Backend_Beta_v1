from globals import create_file,save_temp_config,load_temp_config
from connection_utils import tools
import json
import os
import re
import time
from flask import jsonify
try:
    from neo4j import Query
except Exception:
    Query = None

def build_node_properties_full(node):
    # node is a Neo4j Node object
    if not node:
        return ""
    if hasattr(node, 'properties'):
        node_props = node.properties
    else:
        # fallback if node is a dict (unlikely in your case)
        node_props = node.get('properties', {})
    if not node_props:
        return ""
    lines = []
    for key, value in node_props.items():
        lines.append(f"{key}: {value}")
    return '\n'.join(lines)

def prepare_graph_data_full(records):
    nodes_dict = {}
    edges = []

    for rec in records:
        a_node = rec['a']
        b_node = rec['b']
        rel = rec['r']

        # Access properties directly from Neo4j Node objects
        a_props = a_node.properties if hasattr(a_node, 'properties') else {}
        b_props = b_node.properties if hasattr(b_node, 'properties') else {}

        a_id = a_props.get('element_id') or getattr(a_node, 'element_id', None)
        b_id = b_props.get('element_id') or getattr(b_node, 'element_id', None)

        # Use 'element_id' property or fallback to node ID
        a_element_id = a_props.get('element_id') or getattr(a_node, 'element_id', None)
        b_element_id = b_props.get('element_id') or getattr(b_node, 'element_id', None)

        # Make sure IDs are valid
        if not a_element_id or not b_element_id:
            continue

        # Build nodes
        if a_element_id not in nodes_dict:
            title_str = build_node_properties_full(a_node)
            label_value = a_props.get('BENACCOUNTNO', a_element_id)
            nodes_dict[a_element_id] = {
                'id': a_element_id,
                'label': label_value,
                'title': title_str,
                'color': '#97C2FC'
            }

        if b_element_id not in nodes_dict:
            title_str_b = build_node_properties_full(b_node)
            label_value_b = b_props.get('BENACCOUNTNO', b_element_id)
            nodes_dict[b_element_id] = {
                'id': b_element_id,
                'label': label_value_b,
                'title': title_str_b,
                'color': '#97C2FC'
            }

        # Build edges
        edges.append({'from': a_element_id, 'to': b_element_id})

    return list(nodes_dict.values()), edges

def fetch_graph(id,action,source_id,value,batch):        
    print(id,source_id,value)
    if id == "relationship":
        driver = None
        try:
            tool = load_temp_config("tool", source_id) or load_temp_config("active_tool", source_id) or "neo4j"
            driver = tools(str(tool).lower(), "check", {"session_id": source_id})
            if not driver:
                return {"nodes": [], "edges": [], "error": "Neo4j credentials not found for graph session"}

            nodes = {}
            edges = []
            rel_type = str(value or "").strip()
            batch_id = batch
            
            if not rel_type:
                print("No relationship type provided")
                return {"nodes": [], "edges": [], "error": "No relationship type provided"}
            if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", rel_type):
                return {"nodes": [], "edges": [], "error": "Invalid relationship type"}
            try:
                graph_limit = int(
                    load_temp_config("graph_fetch_limit", source_id)
                    or os.getenv("LINKX_GRAPH_FETCH_LIMIT", "5000")
                )
            except (TypeError, ValueError):
                graph_limit = 5000
            graph_limit = max(0, graph_limit)
            try:
                fetch_timeout_seconds = int(os.getenv("LINKX_GRAPH_FETCH_TIMEOUT_SECONDS", "90"))
            except (TypeError, ValueError):
                fetch_timeout_seconds = 90
            fetch_timeout_seconds = max(1, fetch_timeout_seconds)
            print("dawg:",source_id)
            # query = f"""
            #         MATCH (a)-[r:{rel_type}]->(b)
            #         RETURN a, r, b
            #         LIMIT 100000
            #         """
            # Scope graph fetches to the exact window/source instance stored on the relationship.
            limit_clause = "LIMIT $limit" if graph_limit > 0 else ""
            query = f"""
                    MATCH (a)-[r:{rel_type}]->(b)
                    WHERE r.session_id = $source_id
                    RETURN a, r, b
                    {limit_clause}
                    """
            query_params = {"source_id": str(source_id)}
            if graph_limit > 0:
                query_params["limit"] = graph_limit
            started_at = time.monotonic()
            timed_out = False
                  
            query_to_run = Query(query, timeout=fetch_timeout_seconds) if Query else query
            print(f"[graph_fetch] query start session={source_id} relationship={rel_type} limit={graph_limit} timeout={fetch_timeout_seconds}s", flush=True)

            with driver.session() as session:
                for record in session.run(query_to_run, **query_params):
                    if time.monotonic() - started_at >= fetch_timeout_seconds:
                        timed_out = True
                        break
                    a = record["a"]
                    b = record["b"]
                    r = record["r"]

                    # include all Neo4j node properties
                    nodes[a.id] = {
                        "id": a.id,
                        "label": a.get("NodeId", str(a.id)),
                        **dict(a)  # flatten all node properties
                    }
                    nodes[b.id] = {
                        "id": b.id,
                        "label": b.get("NodeId", str(b.id)),
                        **dict(b)
                    }

                    # include all relationship properties
                    edges.append({
                        "from": a.id,
                        "to": b.id,
                        "label": r.type,  # or type(r).__name__
                        **dict(r)  # flatten all relationship properties
                    })

            print(f"[graph_fetch] query done session={source_id} relationship={rel_type} nodes={len(nodes)} edges={len(edges)} timed_out={timed_out}", flush=True)
            return {
                "nodes": list(nodes.values()),
                "edges": edges,
                "partial": timed_out,
                "timed_out": timed_out,
                "fetch_timeout_seconds": fetch_timeout_seconds,
                "graph_limit": graph_limit,
            }
        except Exception as e:
            print("Relationship graph error:", e)
            return {"nodes": [], "edges": [], "error": str(e)}
        finally:
            if driver:
                driver.close()

    if id == "uploads":
        pass
