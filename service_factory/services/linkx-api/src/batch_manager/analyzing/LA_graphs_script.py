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


def _env_int(name, default):
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def _stop_requested(stop_event):
    return bool(stop_event and hasattr(stop_event, 'is_set') and stop_event.is_set())

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
    seen_edges = set()

    def add_node(node_id, label, title=None, color='#97C2FC'):
        if not node_id: return
        node_id = str(node_id)
        if node_id not in nodes_dict:
            nodes_dict[node_id] = {
                'id': node_id,
                'label': str(label),
                'title': title or str(label),
                'color': color
            }

    for rec in records:
        a_node = rec.get('a')
        b_node = rec.get('b')
        rel = rec.get('r')
        if not a_node or not b_node or not rel:
            continue

        a_props = a_node.properties if hasattr(a_node, 'properties') else {}
        b_props = b_node.properties if hasattr(b_node, 'properties') else {}
        rel_props = rel.properties if hasattr(rel, 'properties') else {}
        rel_type = getattr(rel, 'type', 'UNKNOWN')

        # Visual Formatting based on Rule/Type
        bgcolor = rel_props.get('bgcolor', '#848484')
        reason = rel_props.get('reason', rel_type)
        is_anomaly = rel_type not in ['TRANSACTION', 'FUNDS_TRANSFER', 'CREATED']
        
        edge_style = {
            'color': {'color': bgcolor},
            'dashes': True if is_anomaly else False,
            'arrows': '' if rel_type in ['HUB_AND_SPOKE', 'SHARED_IDENTIFIER', 'CIRCULAR_FLOW'] else 'to',
            'label': rel_type,
            'title': f"{rel_type}: {reason}"
        }

        def process_entity(node, props):
            sender = props.get('ACCOUNTNO') or props.get('SENDERACCOUNTNO') or props.get('CALLING_NO')
            receiver = props.get('BENACCOUNTNO') or props.get('RECEIVERACCOUNTNO') or props.get('CALLED_NO')
            
            if sender and receiver:
                add_node(sender, sender, title=f"Account {sender}")
                add_node(receiver, receiver, title=f"Account {receiver}")
                return str(sender), str(receiver), True
            else:
                n_id = props.get('element_id') or getattr(node, 'element_id', None)
                if n_id:
                    title_str = build_node_properties_full(node)
                    label_val = props.get('NAME') or props.get('ACCOUNTNO') or props.get('BENACCOUNTNO') or str(n_id)
                    add_node(n_id, label_val, title=title_str)
                return str(n_id), None, False

        a_src, a_tgt, a_is_tx = process_entity(a_node, a_props)
        b_src, b_tgt, b_is_tx = process_entity(b_node, b_props)

        if a_is_tx and b_is_tx:
            edge_a_key = f"{a_src}-{a_tgt}-{rel_type}"
            if edge_a_key not in seen_edges:
                edges.append({'from': a_src, 'to': a_tgt, **edge_style})
                seen_edges.add(edge_a_key)
            
            edge_b_key = f"{b_src}-{b_tgt}-{rel_type}"
            if edge_b_key not in seen_edges:
                edges.append({'from': b_src, 'to': b_tgt, **edge_style})
                seen_edges.add(edge_b_key)
                
        elif not a_is_tx and not b_is_tx:
            if a_src and b_src:
                edge_key = f"{a_src}-{b_src}-{rel_type}"
                if edge_key not in seen_edges:
                    edges.append({'from': a_src, 'to': b_src, **edge_style})
                    seen_edges.add(edge_key)
        else:
            acc_id = a_src if not a_is_tx else b_src
            tx_src, tx_tgt = (b_src, b_tgt) if not a_is_tx else (a_src, a_tgt)
            if acc_id and tx_src and tx_tgt:
                edges.append({'from': acc_id, 'to': tx_src, **edge_style})

    return list(nodes_dict.values()), edges

def fetch_graph(id,action,source_id,value,batch, chunk_callback=None, chunk_size=None, first_chunk_size=None, stop_event=None):        
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
            pages_fetched = 0
            last_relationship_cursor = -1
            complete = False
            truncated_by = None
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
            fetch_page_size = max(1, _env_int("LINKX_GRAPH_FETCH_PAGE_SIZE", 5000))
            try:
                fetch_timeout_seconds = int(os.getenv("LINKX_GRAPH_FETCH_TIMEOUT_SECONDS", "90"))
            except (TypeError, ValueError):
                fetch_timeout_seconds = 90
            fetch_timeout_seconds = max(1, fetch_timeout_seconds)

            def flush_chunk(partial=True):
                nonlocal chunk_count, chunk_nodes, chunk_edges
                if not chunk_callback or not (chunk_nodes or chunk_edges):
                    return
                chunk_count += 1
                chunk_callback({
                    "chunk_index": chunk_count,
                    "source_id": str(source_id),
                    "relationship": rel_type,
                    "nodes": list(chunk_nodes.values()),
                    "edges": chunk_edges,
                    "partial": partial,
                    "fetch_page_size": fetch_page_size,
                    "pages_fetched": pages_fetched,
                    "last_relationship_cursor": last_relationship_cursor,
                })
                chunk_nodes = {}
                chunk_edges = []

            relationship_clause = "" if fetch_all_relationships else f":{rel_type}"
            query = f"""
                    MATCH (a)-[r{relationship_clause}]->(b)
                    WHERE r.session_id = $source_id
                      AND id(r) > $after_rel_id
                    RETURN id(r) AS rel_cursor, a, r, b
                    ORDER BY rel_cursor ASC
                    LIMIT $page_size
                    """
            started_at = time.monotonic()
            timed_out = False

            print(
                f"[graph_fetch] query start session={source_id} relationship={rel_type} "
                f"limit={graph_limit} page_size={fetch_page_size} timeout={fetch_timeout_seconds}s",
                flush=True,
            )

            with driver.session() as session:
                while True:
                    if _stop_requested(stop_event):
                        truncated_by = "cancelled"
                        break
                    elapsed = time.monotonic() - started_at
                    if elapsed >= fetch_timeout_seconds:
                        timed_out = True
                        truncated_by = "timeout"
                        break
                    if graph_limit > 0 and total_edges >= graph_limit:
                        truncated_by = "graph_limit"
                        break

                    page_size = fetch_page_size
                    if graph_limit > 0:
                        page_size = min(page_size, graph_limit - total_edges)
                    if page_size <= 0:
                        truncated_by = "graph_limit"
                        break

                    remaining_timeout = max(1, int(fetch_timeout_seconds - elapsed))
                    query_to_run = Query(query, timeout=remaining_timeout) if Query else query
                    page_count = 0
                    for record in session.run(
                        query_to_run,
                        source_id=str(source_id),
                        after_rel_id=int(last_relationship_cursor),
                        page_size=int(page_size),
                    ):
                        if _stop_requested(stop_event):
                            truncated_by = "cancelled"
                            break
                        if time.monotonic() - started_at >= fetch_timeout_seconds:
                            timed_out = True
                            truncated_by = "timeout"
                            break

                        a = record["a"]
                        b = record["b"]
                        r = record["r"]
                        last_relationship_cursor = int(record["rel_cursor"])
                        page_count += 1

                        a_props = _json_safe_properties(dict(a))
                        b_props = _json_safe_properties(dict(b))
                        r_props = _json_safe_properties(dict(r))
                        
                        r_type = r.type
                        is_anomaly = r_type not in ['TRANSACTION', 'FUNDS_TRANSFER', 'CREATED']
                        bgcolor = r_props.get('bgcolor', '#848484')
                        reason = r_props.get('reason', r_type)
                        
                        edge_base = {
                            "label": r_type,
                            "title": f"{r_type}: {reason}",
                            "color": {"color": bgcolor},
                            "dashes": True if is_anomaly else False,
                            "arrows": '' if r_type in ['HUB_AND_SPOKE', 'SHARED_IDENTIFIER', 'CIRCULAR_FLOW'] else 'to',
                            **r_props
                        }

                        def process_entity(node, props):
                            sender = props.get('ACCOUNTNO') or props.get('SENDERACCOUNTNO') or props.get('CALLING_NO')
                            receiver = props.get('BENACCOUNTNO') or props.get('RECEIVERACCOUNTNO') or props.get('CALLED_NO')
                            if sender and receiver:
                                return str(sender), str(receiver), True
                            return str(node.id), None, False

                        def make_node(nid, label_hint):
                            return {"id": nid, "label": str(nid), "title": str(label_hint), "color": "#97C2FC"}

                        a_src, a_tgt, a_is_tx = process_entity(a, a_props)
                        b_src, b_tgt, b_is_tx = process_entity(b, b_props)
                        
                        gen_nodes = {}
                        gen_edges = []
                        
                        if a_is_tx and b_is_tx:
                            if r_type in ['SHARED_IDENTIFIER', 'CIRCULAR_FLOW']:
                                gen_edges.append({"from": a_src, "to": b_src, **edge_base})
                                gen_nodes[a_src] = make_node(a_src, f"Account {a_src}")
                                gen_nodes[b_src] = make_node(b_src, f"Account {b_src}")
                            else:
                                gen_edges.append({"from": a_src, "to": a_tgt, **edge_base})
                                gen_edges.append({"from": b_src, "to": b_tgt, **edge_base})
                                gen_nodes[a_src] = make_node(a_src, f"Account {a_src}")
                                gen_nodes[a_tgt] = make_node(a_tgt, f"Account {a_tgt}")
                                gen_nodes[b_src] = make_node(b_src, f"Account {b_src}")
                                gen_nodes[b_tgt] = make_node(b_tgt, f"Account {b_tgt}")
                        elif not a_is_tx and not b_is_tx:
                            gen_edges.append({"from": a.id, "to": b.id, **edge_base})
                            gen_nodes[a.id] = {"id": a.id, "label": a_props.get("NodeId", str(a.id)), **a_props}
                            gen_nodes[b.id] = {"id": b.id, "label": b_props.get("NodeId", str(b.id)), **b_props}
                        else:
                            acc_id = a.id if not a_is_tx else b.id
                            tx_src, tx_tgt = (b_src, b_tgt) if not a_is_tx else (a_src, a_tgt)
                            gen_edges.append({"from": acc_id, "to": tx_src, **edge_base})
                            gen_nodes[acc_id] = {"id": acc_id, "label": acc_id}
                            gen_nodes[tx_src] = make_node(tx_src, f"Account {tx_src}")

                        if chunk_callback:
                            for nid, n_obj in gen_nodes.items():
                                node_ids.add(nid)
                                if nid not in emitted_node_ids:
                                    chunk_nodes[nid] = n_obj
                                    emitted_node_ids.add(nid)
                            for e in gen_edges:
                                chunk_edges.append(e)
                                
                            target_chunk_size = first_chunk_size if chunk_count == 0 and first_chunk_size else chunk_size
                            if len(chunk_edges) >= max(1, int(target_chunk_size or 250)):
                                flush_chunk(partial=True)
                        else:
                            for nid, n_obj in gen_nodes.items():
                                nodes[nid] = n_obj
                            for e in gen_edges:
                                edges.append(e)

                        total_edges += 1
                        if graph_limit > 0 and total_edges >= graph_limit:
                            truncated_by = "graph_limit"
                            break

                    if page_count:
                        pages_fetched += 1
                    if truncated_by:
                        break
                    if page_count < page_size:
                        complete = True
                        break

                flush_chunk(partial=not complete)

            total_nodes = len(node_ids) if chunk_callback else len(nodes)
            partial = bool(timed_out or truncated_by)
            print(
                f"[graph_fetch] query done session={source_id} relationship={rel_type} "
                f"nodes={total_nodes} edges={total_edges} pages={pages_fetched} "
                f"complete={complete} truncated_by={truncated_by} timed_out={timed_out}",
                flush=True,
            )
            base_result = {
                "partial": partial,
                "timed_out": timed_out,
                "fetch_timeout_seconds": fetch_timeout_seconds,
                "graph_limit": graph_limit,
                "fetch_page_size": fetch_page_size,
                "pages_fetched": pages_fetched,
                "last_relationship_cursor": last_relationship_cursor if last_relationship_cursor >= 0 else None,
                "complete": complete,
                "truncated_by": truncated_by,
            }
            if chunk_callback:
                return {
                    "nodes": [],
                    "edges": [],
                    "total_nodes": total_nodes,
                    "total_edges": total_edges,
                    "chunk_count": chunk_count,
                    **base_result,
                }
            return {
                "nodes": list(nodes.values()),
                "edges": edges,
                **base_result,
            }
        except Exception as e:
            print("Relationship graph error:", e)
            return {"nodes": [], "edges": [], "error": str(e)}
        finally:
            if driver:
                driver.close()

    if id == "uploads":
        pass
