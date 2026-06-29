from globals import create_file,save_temp_config,load_temp_config
from batch_manager.utils.neo4j_utils import Neo4jCredentialConfigError, create_neo4j_driver, load_session_neo4j_credentials
import json
import os
import re
import time
from flask import jsonify
try:
    from neo4j import Query
except Exception:
    Query = None


def _json_safe_value(value):
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(k): _json_safe_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe_value(item) for item in value]
    if hasattr(value, "iso_format"):
        return value.iso_format()
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _json_safe_properties(values):
    return {str(k): _json_safe_value(v) for k, v in dict(values or {}).items()}

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

def fetch_graph(id,action,source_id,value,batch, chunk_callback=None, chunk_size=None):        
    print(id,source_id,value)
    if id == "relationship":
        driver = None
        try:
            tool = load_temp_config("tool", source_id) or load_temp_config("active_tool", source_id) or "neo4j"
            if str(tool).lower() != "neo4j":
                return {"nodes": [], "edges": [], "error": "Graph fetch requires Neo4j tool credentials"}
            try:
                credentials = load_session_neo4j_credentials(source_id, purpose="graph_fetch")
                driver = create_neo4j_driver(credentials)
            except Neo4jCredentialConfigError as exc:
                print(f"[graph_fetch] credential configuration failed session={source_id}: {exc}", flush=True)
                return {"nodes": [], "edges": [], "error": str(exc)}

            nodes = {}
            edges = []
            chunk_nodes = {}
            chunk_edges = []
            emitted_node_ids = set()
            node_ids = set()
            chunk_count = 0
            total_edges = 0
            rel_type = str(value or "").strip()
            batch_id = batch
            
            if not rel_type:
                print("No relationship type provided")
                return {"nodes": [], "edges": [], "error": "No relationship type provided"}
            fetch_all_relationships = rel_type == "*"
            if not fetch_all_relationships and not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", rel_type):
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
            relationship_clause = "" if fetch_all_relationships else f":{rel_type}"
            query = f"""
                    MATCH (a)-[r{relationship_clause}]->(b)
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

                    # include all Neo4j node properties, normalized for job JSON storage
                    a_props = _json_safe_properties(dict(a))
                    b_props = _json_safe_properties(dict(b))
                    r_props = _json_safe_properties(dict(r))
                    a_node = {
                        "id": a.id,
                        "label": a_props.get("NodeId", str(a.id)),
                        **a_props
                    }
                    b_node = {
                        "id": b.id,
                        "label": b_props.get("NodeId", str(b.id)),
                        **b_props
                    }
                    if chunk_callback:
                        node_ids.add(a.id)
                        node_ids.add(b.id)
                    else:
                        nodes[a.id] = a_node
                        nodes[b.id] = b_node

                    # include all relationship properties
                    edge = {
                        "from": a.id,
                        "to": b.id,
                        "label": r.type,  # or type(r).__name__
                        **r_props
                    }
                    if not chunk_callback:
                        edges.append(edge)
                    total_edges += 1

                    if chunk_callback:
                        if a.id not in emitted_node_ids:
                            chunk_nodes[a.id] = a_node
                            emitted_node_ids.add(a.id)
                        if b.id not in emitted_node_ids:
                            chunk_nodes[b.id] = b_node
                            emitted_node_ids.add(b.id)
                        chunk_edges.append(edge)
                        if len(chunk_edges) >= max(1, int(chunk_size or 250)):
                            chunk_count += 1
                            chunk_callback({
                                "chunk_index": chunk_count,
                                "source_id": str(source_id),
                                "relationship": rel_type,
                                "nodes": list(chunk_nodes.values()),
                                "edges": chunk_edges,
                                "partial": True,
                            })
                            chunk_nodes = {}
                            chunk_edges = []

                if chunk_callback and (chunk_nodes or chunk_edges):
                    chunk_count += 1
                    chunk_callback({
                        "chunk_index": chunk_count,
                        "source_id": str(source_id),
                        "relationship": rel_type,
                        "nodes": list(chunk_nodes.values()),
                        "edges": chunk_edges,
                        "partial": True,
                    })

            total_nodes = len(node_ids) if chunk_callback else len(nodes)
            print(f"[graph_fetch] query done session={source_id} relationship={rel_type} nodes={total_nodes} edges={total_edges} timed_out={timed_out}", flush=True)
            if chunk_callback:
                return {
                    "nodes": [],
                    "edges": [],
                    "total_nodes": total_nodes,
                    "total_edges": total_edges,
                    "chunk_count": chunk_count,
                    "partial": timed_out,
                    "timed_out": timed_out,
                    "fetch_timeout_seconds": fetch_timeout_seconds,
                    "graph_limit": graph_limit,
                }
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
