import os
import json
import time
import pandas as pd
from datetime import datetime
import signal
import sys

from batch_manager.analyzing.LA_rules_script import (
    get_fraud_aggregator_query,
    execute_effective_flow_rule,
    get_logical_layer_query,
    get_account_activity_spike_query,
    get_high_risk_link_query,
    get_late_night_tx_query,
    get_just_below_threshold_query,
    get_smurfing_query,
    get_circular_flow_query,
    get_fund_flow_query,
    get_dormant_to_active_query,
    get_abnormal_balance_query,
    get_hub_and_spoke_out_query,
    get_hub_and_spoke_in_query,
    get_shared_identifier_query,
    get_rapid_withdrawal_query,
    get_account_activity_spike_query,
    get_high_risk_link_query
)


from batch_manager.services.risk_scoring_kafka_service import DEFAULT_KAFKA_BROKERS, _neo4j_credentials, _base_analyzer_payload
from batch_manager.analyzing.analyzer import realtime_neo4j_message_ingest, rule_to_node_label
from batch_manager.services.risk_scoring_kafka_service import create_neo4j_driver
import psycopg
import uuid
import json
from kafka import KafkaConsumer

RUNNING = True

def handle_shutdown(signum, frame):
    global RUNNING
    print("\n[xVigilance-Consumer] Shutting down gracefully...", flush=True)
    RUNNING = False



def calculate_fraud_score(anomaly_type, nodes, edges, config=None, version_id="hardcoded"):
    # Fallback default configuration
    default_config = {
        "base_scores": {
            "HIGH_RISK_LINK": 50, "CIRCULAR_FLOW": 30, "EFFECTIVE_FLOW": 25,
            "SMURFING": 20, "SHARED_IDENTIFIER": 20, "HUB_AND_SPOKE": 10,
            "RAPID_FAN_OUT": 10, "ABNORMAL_BALANCE_CHANGE": 10
        },
        "node_thresholds": [
            {"min_nodes": 10000, "add_points": 30},
            {"min_nodes": 5000, "add_points": 20},
            {"min_nodes": 1000, "add_points": 10},
            {"min_nodes": 100, "add_points": 5}
        ],
        "money_thresholds": [
            {"min_amount": 10000000, "add_points": 40},
            {"min_amount": 5000000, "add_points": 30},
            {"min_amount": 1000000, "add_points": 20},
            {"min_amount": 500000, "add_points": 10}
        ]
    }
    
    if not config:
        config = default_config
        
    score_evidence = {
        "config_version": version_id,
        "base_score_applied": 0,
        "node_count_bonus": 0,
        "financial_bonus": 0,
        "total_nodes_evaluated": len(nodes),
        "total_value_evaluated": 0.0
    }

    # 1. Base Score
    base_scores = config.get("base_scores", default_config["base_scores"])
    score = base_scores.get(anomaly_type, 10)
    score_evidence["base_score_applied"] = score
    
    # 2. Graph Size Multiplier
    node_count = len(nodes)
    node_bonus = 0
    node_thresholds = config.get("node_thresholds", default_config["node_thresholds"])
    for bucket in sorted(node_thresholds, key=lambda x: x["min_nodes"], reverse=True):
        if node_count >= bucket["min_nodes"]:
            node_bonus = bucket["add_points"]
            break
            
    score += node_bonus
    score_evidence["node_count_bonus"] = node_bonus
        
    # 3. Financial Value Multiplier
    total_value = 0.0
    for n in nodes:
        amount = n.get("TRANSFERAMOUNT") or n.get("AMOUNT") or n.get("AMOUNTINBIRR") or 0.0
        try:
            total_value += float(amount)
        except:
            pass
            
    score_evidence["total_value_evaluated"] = total_value
            
    money_bonus = 0
    money_thresholds = config.get("money_thresholds", default_config["money_thresholds"])
    for bucket in sorted(money_thresholds, key=lambda x: x["min_amount"], reverse=True):
        if total_value >= bucket["min_amount"]:
            money_bonus = bucket["add_points"]
            break
            
    score += money_bonus
    score_evidence["financial_bonus"] = money_bonus
        
    # Cap at 100
    score = min(100, int(score))
    
    # 4. Banding Logic
    if score >= 80:
        band = "Critical"
    elif score >= 50:
        band = "High"
    elif score >= 20:
        band = "Medium"
    else:
        band = "Low"
        
    return score, band, score_evidence


def promote_anomalies_to_postgres(credentials, session_id, window_id, execution_meta=None):
    driver = create_neo4j_driver(credentials)
    node_label = rule_to_node_label("bank transactions", session_id)
    safe_label = f"`{str(node_label).replace('`', '')}`"
    
    graphs = {}
    total_anomalies = 0
    try:
        with driver.session() as session:
            result = session.run(f"MATCH (n:{safe_label})-[r]->(m:{safe_label}) WHERE r.reason IS NOT NULL RETURN n, r, m")
            for record in result:
                n = record["n"]
                r = record["r"]
                m = record["m"]
                
                total_anomalies += 1
                anomaly_type = getattr(r, 'type', 'UNKNOWN_ANOMALY')
                reason_text = r.get("reason", "Multiple Anomalies Detected")
                
                if anomaly_type not in graphs:
                    graphs[anomaly_type] = {
                        "nodes": {},
                        "edges": [],
                        "reason": reason_text
                    }
                
                n_id = str(n.get("NodeId") or getattr(n, 'element_id', 'unknown_n'))
                m_id = str(m.get("NodeId") or getattr(m, 'element_id', 'unknown_m'))
                r_id = str(getattr(r, 'element_id', 'unknown_r'))
                
                n_props = dict(n)
                m_props = dict(m)
                r_props = dict(r)
                
                if n_props.get("LOGICAL_BENACCOUNTNO"):
                    n_props["BENACCOUNTNO"] = n_props["LOGICAL_BENACCOUNTNO"]
                    n_props["IS_LOGICAL_PASSTHROUGH"] = True
                    
                graphs[anomaly_type]["nodes"][n_id] = {
                    "id": n_id,
                    "label": n_props.get("NodeId", n_id),
                    **n_props
                }
                
                if m_props.get("LOGICAL_BENACCOUNTNO"):
                    m_props["BENACCOUNTNO"] = m_props["LOGICAL_BENACCOUNTNO"]
                    m_props["IS_LOGICAL_PASSTHROUGH"] = True
                    
                graphs[anomaly_type]["nodes"][m_id] = {
                    "id": m_id,
                    "label": m_props.get("NodeId", m_id),
                    **m_props
                }
                
                graphs[anomaly_type]["edges"].append({
                    "id": r_id,
                    "from": n_id,
                    "to": m_id,
                    "label": anomaly_type,
                    **r_props
                })
    except Exception as e:
        print(f"[xVigilance-Consumer] Error querying Neo4j for anomalies: {e}", flush=True)
        return
        
    if not graphs:
        print(f"[xVigilance-Consumer] No anomalies found in window {window_id}. Graph is perfectly clean.", flush=True)
        return
        
    print(f"[xVigilance-Consumer] 🚨 Detective detected {total_anomalies} anomalous records! Executing Hand-Off to Risk Scoring...", flush=True)
    
    try:
        with psycopg.connect(os.getenv('LINKX_POSTGRES_DSN')) as conn:
            with conn.cursor() as cur:
                # --- FETCH DYNAMIC SCORING CONFIG ---
                scoring_config = None
                config_version = "hardcoded_fallback"
                try:
                    cur.execute("SELECT config_data, version_id FROM risk_scoring_config ORDER BY created_at DESC LIMIT 1;")
                    row = cur.fetchone()
                    if row:
                        scoring_config = row[0]
                        config_version = f"v{row[1]}"
                except Exception as e:
                    print(f"[xVigilance-Consumer] Warning: Could not fetch dynamic scoring config (falling back to default): {e}", flush=True)
                    conn.rollback() # Crucial: rollback the failed select so subsequent inserts don't fail
                # ------------------------------------
                
                for anomaly_type, graph_data in graphs.items():
                    nodes_list = list(graph_data["nodes"].values())
                    edges_list = graph_data["edges"]
                    
                    # --- CUSTOM SCORING INJECTION ---
                    try:
                        score, band, score_evidence = calculate_fraud_score(anomaly_type, nodes_list, edges_list, scoring_config, config_version)
                    except Exception as e:
                        print(f"[xVigilance-Consumer] Warning: Scoring failed ({e}), falling back to Medium.", flush=True)
                        score, band = 50, "Medium"
                        score_evidence = {"error": str(e), "config_version": config_version}
                        
                    # --- TOP 5 ACCOUNTS EXTRACTION ---
                    account_volumes = {}
                    for n in nodes_list:
                        acc = n.get("ACCOUNTNO") or n.get("accountno")
                        try:
                            amt = float(n.get("TRANSFERAMOUNT") or n.get("AMOUNT") or n.get("AMOUNTINBIRR") or 0.0)
                        except:
                            amt = 0.0
                        if acc:
                            account_volumes[acc] = account_volumes.get(acc, 0.0) + amt
                    
                    top_5_accounts = [acc for acc, vol in sorted(account_volumes.items(), key=lambda item: item[1], reverse=True)[:5]]
                    # --------------------------------
                    
                    from collections import defaultdict
                    node_degrees = defaultdict(int)
                    for edge in edges_list:
                        node_degrees[edge["from"]] += 1
                        node_degrees[edge["to"]] += 1
                        
                    top_nodes = sorted(node_degrees.keys(), key=lambda x: node_degrees[x], reverse=True)
                    top_5_nodes = top_nodes[:5]
                    top_node_str = ", ".join(top_5_nodes)
                    primary_account = top_5_nodes[0] if top_5_nodes else "UNKNOWN"
                    
                    # --- EVIDENCE SUBGRAPH EXTRACTION (Edge-Centric) ---
                    # To prevent "orphan nodes" or "dangling edges" in the UI, we must ensure 
                    # that every edge we send has BOTH of its nodes included.
                    # 1. Sort all edges by the combined degree of their endpoints (keeps the hub activity).
                    sorted_edges = sorted(edges_list, key=lambda e: node_degrees[e["from"]] + node_degrees[e["to"]], reverse=True)
                    
                    # 2. Take the top N edges
                    MAX_EDGES = 1000
                    render_edges = sorted_edges[:MAX_EDGES]
                    
                    # 3. Extract the exact set of nodes used by these edges
                    rendered_node_ids = set()
                    for e in render_edges:
                        rendered_node_ids.add(e["from"])
                        rendered_node_ids.add(e["to"])
                        
                    # 4. Include those nodes, plus the absolute top 5 accounts just in case they were somehow missed
                    for top_acc in top_5_nodes:
                        rendered_node_ids.add(top_acc)
                        
                    render_nodes = [n for n in nodes_list if n["id"] in rendered_node_ids]
                    # ------------------------------------
                    
                    linked_entities = []
                    for edge in edges_list[:50]:
                        linked_entities.append({
                            "accountno": edge["to"],
                            "relationship": edge["label"],
                            "risk_contribution": 0.95,
                            "is_flagged": True
                        })
                    
                    trace_id = str(uuid.uuid4())
                    ts_now = datetime.now().isoformat() + "Z"
                    
                    # 1. Construct EXACT hand-off payload requested by user
                    handoff_payload = {
                        "schema_version": "1.0",
                        "event_type": "analysis.link.flagged",
                        "success": True,
                        "message": f"Link analysis flagged for accounts [{top_node_str}]: {len(edges_list)} linked",
                        "data": {
                            "accountno": top_5_nodes,
                            "entity_id": primary_account,
                            "linked_accounts_count": len(nodes_list),
                            "flagged_entity_links": len(edges_list),
                            "beneficiary_blacklisted": True,
                            "flagged_rules": [anomaly_type],
                            "network_centrality_score": 0.95,
                            "max_path_length": 2,
                            "linked_entities": linked_entities,
                            "fraud_score": score,
                            "score_band": band,
                            "score_evidence": score_evidence,
                            "top_5_accounts": [{"account": k, "volume": v} for k, v in account_volumes.items() if k in top_5_accounts],
                            "graph": {
                                "nodes": render_nodes,
                                "edges": render_edges
                            }
                        },
                        "meta": {
                            "trace_id": trace_id,
                            "span_id": str(uuid.uuid4())[:8],
                            "traceparent": f"00-{trace_id}-00-01",
                            "correlation_id": trace_id,
                            "timestamp": ts_now,
                            "service": {
                                "name": "link-analysis-service",
                                "version": "1.0.0"
                            },
                            "aggregation_key": {
                                "type": "accountno",
                                "value": primary_account
                            }
                        }
                    }
                    
                    evidence_json = json.dumps(handoff_payload, default=str)
                    # 2. POST to Risk Scoring Async Endpoint
                    try:
                        import requests
                        api_host = os.getenv("LINKX_API_HOST", "172.27.23.95")
                        api_port = os.getenv("LINKX_API_PORT", "8000")
                        api_url = f"http://{api_host}:{api_port}/api/risk_scoring/analysis_request"
                        api_key = os.getenv("LINK_ANALYSIS_API_KEY") or os.getenv("LINKX_RISK_SCORING_API_KEY", "")
                        headers = {"X-API-Key": api_key, "Content-Type": "application/json"} if api_key else {"Content-Type": "application/json"}
                        
                        resp = requests.post(api_url, data=evidence_json, headers=headers, timeout=5)
                        print(f"[xVigilance-Escalation] Posted findings to Risk Scoring API. Response: {resp.status_code}", flush=True)
                    except Exception as e:
                        print(f"[xVigilance-Escalation] Warning: External Risk Scoring API unreachable: {e}", flush=True)
                    
                    # 3. Store the graph evidence exactly once
                    evidence_json = json.dumps(handoff_payload, default=str)
                    cur.execute("""
                        INSERT INTO link_analysis_evidence (
                            trace_id, session_id, entity_id, event_type, is_flagged, 
                            response_payload, request_payload, analyzed_at
                        ) VALUES (
                            %s, %s, %s, 'analysis.link.flagged', true, 
                            %s::jsonb, '{}'::jsonb, NOW()
                        )
                    """, (trace_id, 'XVIGILANCE_FINDINGS', primary_account, evidence_json))
                    
                    # 4. Finalize xVigilance Summary with "reported_to" signature
                    report_payload = {
                        "trace_id": trace_id,
                        "entity_id": primary_account,
                        "anomaly_type": anomaly_type,
                        "reason": graph_data["reason"],
                        "reported_to": "Risk Scoring Service",
                        "execution_meta": execution_meta or {},
                        "run_id": execution_meta.get("batch_id") if execution_meta else None,
                        "fraud_score": score,
                        "score_band": band,
                        "top_5_accounts": top_5_accounts
                    }
                    cur.execute("""
                        INSERT INTO linkx_reports (report_type, source_system, external_reference_id, payload, status)
                        VALUES (%s, %s, %s, %s, %s)
                    """, ('XVIGILANCE_FINDING', 'xvigilance_worker', trace_id, json.dumps(report_payload, default=str), 'FLAGGED'))
                    
            conn.commit()
        print(f"[xVigilance-Consumer] Hand-off and database storage completed successfully for {len(graphs)} grouped anomalies!", flush=True)
    except Exception as e:
        print(f"[xVigilance-Consumer] Error inserting grouped evidence to Postgres: {e}", flush=True)




# =====================================================================
# FAST INGEST: Direct Neo4j node insertion WITHOUT incremental rules
# =====================================================================
def _neo4j_property_value(value):
    """Convert a Python value into a Neo4j-safe property value."""
    if value is None:
        return ""
    if isinstance(value, float) and value != value:
        return ""
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (dict, list, tuple, set)):
        return json.dumps(value, default=str, sort_keys=True)
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def fast_ingest_batch(credentials, session_id, df, batch_number, node_label):
    """
    Ingest a DataFrame of transactions directly into Neo4j as nodes.
    This is a FAST path that skips all incremental rule analysis.
    Rules are run ONCE at the WATERMARK for the full-hour picture.
    """
    rows = df.to_dict(orient="records") if hasattr(df, "to_dict") else []
    if not rows:
        return

    batch_id = f"{session_id}_rt_{batch_number}"
    clean_rows = []
    for row in rows:
        clean = {key: _neo4j_property_value(value) for key, value in row.items()}
        clean.setdefault("NodeId", str(uuid.uuid4()))
        clean["session_id"] = str(session_id or "")
        clean["created_by"] = "linkx"
        clean["linkx_managed"] = True
        clean["created_at"] = datetime.utcnow().isoformat()
        clean["batch_id"] = str(batch_id)
        clean["nodes_label"] = node_label
        clean_rows.append(clean)

    driver = create_neo4j_driver(credentials)
    try:
        with driver.session() as session:
            # CREATE instead of MERGE: NodeIds are unique UUIDs, no duplicates possible
            session.run(f"""
                UNWIND $rows AS row
                CREATE (n:`{node_label}`)
                SET n = row, n.node_identity = 'Entity Node'
            """, rows=clean_rows)

    finally:
        driver.close()


def fetch_global_entities():
    try:
        with psycopg.connect(os.getenv('LINKX_POSTGRES_DSN')) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT config_data FROM global_entity_classification ORDER BY created_at DESC LIMIT 1")
                row = cur.fetchone()
                if row and row[0]:
                    return row[0]
    except Exception as e:
        print(f"[xVigilance] Failed to fetch global entities: {e}")
    return {}


def fetch_rule_thresholds():
    defaults = {
        "smurfing_single_tx_threshold": 300000,
        "smurfing_min_tx_count": 3,
        "smurfing_cumulative_threshold": 900000,
        "reporting_threshold": 300000,
        "circular_flow_check_amounts": False,
        "late_night_start": 2300,
        "late_night_end": 400,
        "hub_spoke_min_counterparties": 3,
        "activity_spike_multiplier": 3,
        "activity_spike_min_daily_count": 10,
        "rapid_withdrawal_amount_tolerance": 0.1
    }
    try:
        with psycopg.connect(os.getenv('LINKX_POSTGRES_DSN')) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT config_data FROM global_rule_thresholds ORDER BY created_at DESC LIMIT 1")
                row = cur.fetchone()
                if row and row[0]:
                    merged = dict(defaults)
                    merged.update(row[0])
                    return merged
    except Exception as e:
        print(f"[xVigilance] Failed to fetch rule thresholds, using defaults: {e}")
    return defaults

def format_cypher_entries(entities):
    if not entities or not isinstance(entities, list):
        return []
    return [{str(k): str(v) for k, v in entry.items()} for entry in entities if isinstance(entry, dict)]

def _trusted_entry_match(alias):
    # Re-map alias to use logical fields for matching
    logical_alias = f'{{alias}}'

    return f"all(k IN keys(entry) WHERE toLower(k) IN ['category', 'type', 'reason'] OR toString(coalesce({alias}[k], \"\")) = toString(entry[k]))"

def _trusted_node_clause(alias):
    return f'NOT any(entry IN $trusted_entries WHERE {_trusted_entry_match(alias)})'

def _trusted_pair_clause(left_alias, right_alias):
    return (
        "NOT any(entry IN $trusted_entries WHERE "
        f"({_trusted_entry_match(left_alias)} OR {_trusted_entry_match(right_alias)}))"
    )

def _extract_pass_through_accounts(global_config):
    """Extract account numbers of entities marked as pass-through intermediaries.

    Works with both the raw DB format (bool ``True``) and the stringified
    Cypher-parameter format (``"True"``/``"true"``/``"1"``).
    """
    accounts = set()
    for entity in (global_config.get("trusted_entities") or []):
        if not isinstance(entity, dict):
            continue
        pt = entity.get("pass_through", entity.get("passthrough", ""))
        if str(pt).lower() in ("true", "1", "yes"):
            # The UI stores entities as {"key": "...", "value": "..."}
            key = str(entity.get("key") or "").upper()
            val = entity.get("value")
            
            # Also support flat key-values just in case
            if not val and entity.get("ACCOUNTNO"):
                val = entity.get("ACCOUNTNO")

            if key in ("ACCOUNTNO", "ACCOUNT_NO", "ACCOUNT") or entity.get("ACCOUNTNO"):
                if val and str(val).strip():
                    accounts.add(str(val).strip())
    return list(accounts)


def run_full_graph_analysis(credentials, session_id, node_label, mock_global_config=None):
    """
    Run ALL LA rules on the complete hourly graph.
    Each rule runs in its own transaction with error isolation,
    so one failure doesn't kill the rest.
    CIRCULAR_FLOW and FUND_FLOW use optimized index-assisted queries
    instead of cartesian products.
    """
    driver = create_neo4j_driver(credentials)
    label = f"`{str(node_label).replace('`', '')}`"
    sp = str(session_id) if session_id else ""
    rules_completed = []
    rules_failed = []

    # Fetch and format global entities
    global_config = mock_global_config if mock_global_config is not None else fetch_global_entities()
    trusted_entries = format_cypher_entries(global_config.get("trusted_entities", []))
    risk_entries = format_cypher_entries(global_config.get("risk_entities", []))
    pass_through_accounts = _extract_pass_through_accounts(global_config)
    if pass_through_accounts:
        print(f"[xVigilance-Consumer] Pass-through accounts loaded: {len(pass_through_accounts)}", flush=True)

    # Fetch rule thresholds
    thresholds = fetch_rule_thresholds()
    print(f"[xVigilance-Consumer] Rule thresholds loaded: {', '.join(f'{k}={v}' for k, v in thresholds.items())}", flush=True)

    try:
        # ---- 0. EFFECTIVE_FLOW ----
        if pass_through_accounts:
            try:
                    start_time = datetime.now()
                    with driver.session() as s:
                        edge_count = execute_effective_flow_rule(
                            session=s, 
                            label=label, 
                            scope_clause_t="$session_id IS NULL OR n.session_id = $session_id", 
                            session_id=sp, 
                            pass_through_accounts=pass_through_accounts
                        )
                    rules_completed.append("EFFECTIVE_FLOW")
                    print(f"  [Rule] EFFECTIVE_FLOW ✓ (Python accelerated: {edge_count} edges)", flush=True)
            except Exception as e:
                rules_failed.append(("EFFECTIVE_FLOW", str(e)[:100]))
                print(f"  [Rule] EFFECTIVE_FLOW ✗ {str(e)[:100]}", flush=True)
        else:
            rules_completed.append("EFFECTIVE_FLOW")
            print("  [Rule] EFFECTIVE_FLOW ✓ (skipped: no pass-through accounts configured)", flush=True)

        # ---- 0.5. LOGICAL TRANSACTION LAYER ----
        try:
            start_time = datetime.now()
            queries = get_logical_layer_query(
                label=label,
                scope_clause_t="($session_id IS NULL OR $session_id = '' OR t.session_id = $session_id)",
                apply_pass_through=bool(pass_through_accounts)
            )
            with driver.session() as s:
                for q in queries:
                    s.run(q, session_id=sp)
            rules_completed.append("LOGICAL_LAYER")
            print(f"  [Rule] LOGICAL_LAYER ✓ ({(datetime.now() - start_time).total_seconds():.2f}s)", flush=True)
        except Exception as e:
            rules_failed.append(("LOGICAL_LAYER", str(e)[:100]))
            print(f"  [Rule] LOGICAL_LAYER ✗ {str(e)[:100]}", flush=True)

        # ---- 1. SMURFING ----
        try:
            start_time = datetime.now()
            start_time = datetime.now()
            with driver.session() as s:
                query = get_smurfing_query(
                    label=label,
                    scope_clause_t="($session_id IS NULL OR $session_id = '' OR t.session_id = $session_id)",
                    trusted_pair_clause=_trusted_pair_clause('a', 'b'),
                    is_provisional=False
                )
                s.run(query, session_id=sp, trusted_entries=trusted_entries, risk_entries=risk_entries,
                     smurfing_single_tx_threshold=thresholds.get("smurfing_single_tx_threshold"),
                     smurfing_min_tx_count=thresholds.get("smurfing_min_tx_count"),
                     smurfing_cumulative_threshold=thresholds.get("smurfing_cumulative_threshold"))
            rules_completed.append("SMURFING")
            print(f"  [Rule] SMURFING ✓ ({(datetime.now() - start_time).total_seconds():.2f}s)", flush=True)
        except Exception as e:
            rules_failed.append(("SMURFING", str(e)[:100]))
            print(f"  [Rule] SMURFING ✗ {str(e)[:100]}", flush=True)

        # ---- 2. CIRCULAR_FLOW (OPTIMIZED: index-assisted, no cartesian product) ----
        try:
            start_time = datetime.now()
            start_time = datetime.now()
            with driver.session() as s:
                query = get_circular_flow_query(
                    label=label,
                    scope_clause_t="($session_id IS NULL OR $session_id = '' OR t.session_id = $session_id)",
                    scope_clause_a="($session_id IS NULL OR $session_id = '' OR a.session_id = $session_id)",
                    scope_clause_b="($session_id IS NULL OR $session_id = '' OR b.session_id = $session_id)",
                    trusted_pair_clause=_trusted_pair_clause('a', 'b'),
                    is_provisional=False
                )
                s.run(query, session_id=sp, trusted_entries=trusted_entries, risk_entries=risk_entries, pt=pass_through_accounts)
            rules_completed.append("CIRCULAR_FLOW")
            print(f"  [Rule] CIRCULAR_FLOW ✓ ({(datetime.now() - start_time).total_seconds():.2f}s)", flush=True)
        except Exception as e:
            rules_failed.append(("CIRCULAR_FLOW", str(e)[:100]))
            print(f"  [Rule] CIRCULAR_FLOW ✗ {str(e)[:100]}", flush=True)

        # ---- 3. FUND_FLOW (OPTIMIZED: index-assisted, no cartesian product) ----
        try:
            start_time = datetime.now()
            start_time = datetime.now()
            with driver.session() as s:
                query = get_fund_flow_query(
                    label=label,
                    scope_clause_t="($session_id IS NULL OR $session_id = '' OR t.session_id = $session_id)",
                    scope_clause_a="($session_id IS NULL OR $session_id = '' OR a.session_id = $session_id)",
                    scope_clause_b="($session_id IS NULL OR $session_id = '' OR b.session_id = $session_id)",
                    trusted_pair_clause=_trusted_pair_clause('a', 'b'),
                    is_provisional=False
                )
                s.run(query, session_id=sp, trusted_entries=trusted_entries, risk_entries=risk_entries, pt=pass_through_accounts)
            rules_completed.append("FUND_FLOW")
            print(f"  [Rule] FUND_FLOW ✓ ({(datetime.now() - start_time).total_seconds():.2f}s)", flush=True)
        except Exception as e:
            rules_failed.append(("FUND_FLOW", str(e)[:100]))
            print(f"  [Rule] FUND_FLOW ✗ {str(e)[:100]}", flush=True)

        # ---- 4. DORMANT_TO_ACTIVE ----
        try:
            start_time = datetime.now()
            start_time = datetime.now()
            with driver.session() as s:
                query = get_dormant_to_active_query(
                    label=label,
                    scope_clause_t="($session_id IS NULL OR $session_id = '' OR t.session_id = $session_id)",
                    is_provisional=False
                )
                s.run(query, session_id=sp, trusted_entries=trusted_entries, risk_entries=risk_entries, pt=pass_through_accounts)
            rules_completed.append("DORMANT_TO_ACTIVE")
            print(f"  [Rule] DORMANT_TO_ACTIVE ✓ ({(datetime.now() - start_time).total_seconds():.2f}s)", flush=True)
        except Exception as e:
            rules_failed.append(("DORMANT_TO_ACTIVE", str(e)[:100]))
            print(f"  [Rule] DORMANT_TO_ACTIVE ✗ {str(e)[:100]}", flush=True)

        # ---- 5. ABNORMAL_BALANCE_CHANGE ----
        try:
            start_time = datetime.now()
            start_time = datetime.now()
            with driver.session() as s:
                query = get_abnormal_balance_query(
                    label=label,
                    scope_clause_t="($session_id IS NULL OR $session_id = '' OR t.session_id = $session_id)",
                    is_provisional=False
                )
                s.run(query, session_id=sp, trusted_entries=trusted_entries, risk_entries=risk_entries, pt=pass_through_accounts, historical_baseline_days=thresholds.get("historical_baseline_days", 30))
            rules_completed.append("ABNORMAL_BALANCE_CHANGE")
            print(f"  [Rule] ABNORMAL_BALANCE_CHANGE ✓ ({(datetime.now() - start_time).total_seconds():.2f}s)", flush=True)
        except Exception as e:
            rules_failed.append(("ABNORMAL_BALANCE_CHANGE", str(e)[:100]))
            print(f"  [Rule] ABNORMAL_BALANCE_CHANGE ✗ {str(e)[:100]}", flush=True)

        # ---- 6. HUB_AND_SPOKE (outgoing) ----
        try:
            start_time = datetime.now()
            start_time = datetime.now()
            with driver.session() as s:
                query = get_hub_and_spoke_out_query(
                    label=label,
                    scope_clause_t="($session_id IS NULL OR $session_id = '' OR t.session_id = $session_id)",
                    trusted_pair_clause=_trusted_pair_clause('a', 'b'),
                    is_provisional=False
                )
                s.run(query, session_id=sp, trusted_entries=trusted_entries, risk_entries=risk_entries, pt=pass_through_accounts, hub_spoke_min_counterparties=thresholds.get("hub_spoke_min_counterparties"))
            rules_completed.append("HUB_AND_SPOKE_OUT")
            print(f"  [Rule] HUB_AND_SPOKE (outgoing) ✓ ({(datetime.now() - start_time).total_seconds():.2f}s)", flush=True)
        except Exception as e:
            rules_failed.append(("HUB_AND_SPOKE_OUT", str(e)[:100]))
            print(f"  [Rule] HUB_AND_SPOKE (outgoing) ✗ {str(e)[:100]}", flush=True)

        # ---- 7. HUB_AND_SPOKE (incoming) ----
        try:
            start_time = datetime.now()
            start_time = datetime.now()
            with driver.session() as s:
                query = get_hub_and_spoke_in_query(
                    label=label,
                    scope_clause_t="($session_id IS NULL OR $session_id = '' OR t.session_id = $session_id)",
                    trusted_pair_clause=_trusted_pair_clause('a', 'b'),
                    is_provisional=False
                )
                s.run(query, session_id=sp, trusted_entries=trusted_entries, risk_entries=risk_entries, pt=pass_through_accounts, hub_spoke_min_counterparties=thresholds.get("hub_spoke_min_counterparties"))
            rules_completed.append("HUB_AND_SPOKE_IN")
            print(f"  [Rule] HUB_AND_SPOKE (incoming) ✓ ({(datetime.now() - start_time).total_seconds():.2f}s)", flush=True)
        except Exception as e:
            rules_failed.append(("HUB_AND_SPOKE_IN", str(e)[:100]))
            print(f"  [Rule] HUB_AND_SPOKE (incoming) ✗ {str(e)[:100]}", flush=True)

        # ---- 8. SHARED_IDENTIFIER ----
        try:
            start_time = datetime.now()
            start_time = datetime.now()
            with driver.session() as s:
                query = get_shared_identifier_query(
                    label=label,
                    scope_clause_t="($session_id IS NULL OR $session_id = '' OR t.session_id = $session_id)",
                    is_provisional=False
                )
                s.run(query, session_id=sp, trusted_entries=trusted_entries, risk_entries=risk_entries, pt=pass_through_accounts)
            rules_completed.append("SHARED_IDENTIFIER")
            print(f"  [Rule] SHARED_IDENTIFIER ✓ ({(datetime.now() - start_time).total_seconds():.2f}s)", flush=True)
        except Exception as e:
            rules_failed.append(("SHARED_IDENTIFIER", str(e)[:100]))
            print(f"  [Rule] SHARED_IDENTIFIER ✗ {str(e)[:100]}", flush=True)


        # ---- 9a. LATE_NIGHT_TX ----
        try:
            start_time = datetime.now()
            with driver.session() as s:
                query = get_late_night_tx_query(
                    label=label,
                    scope_clause_t="($session_id IS NULL OR $session_id = '' OR t.session_id = $session_id)",
                    is_provisional=False
                )
                s.run(query, session_id=sp, trusted_entries=trusted_entries, risk_entries=risk_entries, pt=pass_through_accounts,
                      late_night_start=thresholds.get("late_night_start", 2300), late_night_end=thresholds.get("late_night_end", 400))
            rules_completed.append("LATE_NIGHT_TX")
            print(f"  [Rule] LATE_NIGHT_TX ✓ ({(datetime.now() - start_time).total_seconds():.2f}s)", flush=True)
        except Exception as e:
            rules_failed.append(("LATE_NIGHT_TX", str(e)[:100]))
            print(f"  [Rule] LATE_NIGHT_TX ✗ {str(e)[:100]}", flush=True)

        # ---- 9b. JUST_BELOW_THRESHOLD ----
        try:
            start_time = datetime.now()
            with driver.session() as s:
                query = get_just_below_threshold_query(
                    label=label,
                    scope_clause_t="($session_id IS NULL OR $session_id = '' OR t.session_id = $session_id)",
                    is_provisional=False
                )
                s.run(query, session_id=sp, trusted_entries=trusted_entries, risk_entries=risk_entries, pt=pass_through_accounts,
                      single_tx_threshold=thresholds.get("reporting_threshold", 300000))
            rules_completed.append("JUST_BELOW_THRESHOLD")
            print(f"  [Rule] JUST_BELOW_THRESHOLD ✓ ({(datetime.now() - start_time).total_seconds():.2f}s)", flush=True)
        except Exception as e:
            rules_failed.append(("JUST_BELOW_THRESHOLD", str(e)[:100]))
            print(f"  [Rule] JUST_BELOW_THRESHOLD ✗ {str(e)[:100]}", flush=True)

        # ---- 10. RAPID_WITHDRAWAL ----
        try:
            start_time = datetime.now()
            start_time = datetime.now()
            with driver.session() as s:
                query = get_rapid_withdrawal_query(
                    label=label,
                    scope_clause_t="($session_id IS NULL OR $session_id = '' OR t.session_id = $session_id)",
                    is_provisional=False
                )
                s.run(query, session_id=sp, trusted_entries=trusted_entries, risk_entries=risk_entries, pt=pass_through_accounts, rapid_withdrawal_amount_tolerance=thresholds.get("rapid_withdrawal_amount_tolerance"))
            rules_completed.append("RAPID_WITHDRAWAL")
            print(f"  [Rule] RAPID_WITHDRAWAL ✓ ({(datetime.now() - start_time).total_seconds():.2f}s)", flush=True)
        except Exception as e:
            rules_failed.append(("RAPID_WITHDRAWAL", str(e)[:100]))
            print(f"  [Rule] RAPID_WITHDRAWAL ✗ {str(e)[:100]}", flush=True)

        # ---- 12. ACCOUNT_ACTIVITY_SPIKE ----
        try:
            start_time = datetime.now()
            start_time = datetime.now()
            with driver.session() as s:
                query = get_account_activity_spike_query(
                    label=label,
                    scope_clause_t="($session_id IS NULL OR $session_id = '' OR t.session_id = $session_id)"
                )
                s.run(query, session_id=sp, trusted_entries=trusted_entries, risk_entries=risk_entries,
                      activity_spike_min_daily_count=thresholds.get("activity_spike_min_daily_count"),
                      activity_spike_multiplier=thresholds.get("activity_spike_multiplier"), pt=pass_through_accounts, historical_baseline_days=thresholds.get("historical_baseline_days", 30))
            rules_completed.append("ACCOUNT_ACTIVITY_SPIKE")
            print(f"  [Rule] ACCOUNT_ACTIVITY_SPIKE ✓ ({(datetime.now() - start_time).total_seconds():.2f}s)", flush=True)
        except Exception as e:
            rules_failed.append(("ACCOUNT_ACTIVITY_SPIKE", str(e)[:100]))
            print(f"  [Rule] ACCOUNT_ACTIVITY_SPIKE ✗ {str(e)[:100]}", flush=True)

        # ---- 13. HIGH_RISK_LINK (from risk_entities) ----
        try:
            start_time = datetime.now()
            start_time = datetime.now()
            with driver.session() as s:
                query = get_high_risk_link_query(
                    label=label,
                    scope_clause_t="($session_id IS NULL OR $session_id = '' OR t.session_id = $session_id)"
                )
                s.run(query, session_id=sp, risk_entries=risk_entries)
            rules_completed.append("HIGH_RISK_LINK")
            print(f"  [Rule] HIGH_RISK_LINK / PEP / SANCTION ✓ ({(datetime.now() - start_time).total_seconds():.2f}s)", flush=True)
        except Exception as e:
            rules_failed.append(("HIGH_RISK_LINK", str(e)[:100]))
            print(f"  [Rule] HIGH_RISK_LINK / PEP / SANCTION ✗ {str(e)[:100]}", flush=True)


        # ---- 14. FRAUD_AGGREGATOR ----
        try:
            start_time = datetime.now()
            start_time = datetime.now()
            with driver.session() as s:
                query = get_fraud_aggregator_query(
                    label=label,
                    scope_clause_t="($session_id IS NULL OR $session_id = '' OR t.session_id = $session_id)",
                    session_id=sp
                )
                s.run(query, session_id=sp)
            rules_completed.append("FRAUD_AGGREGATOR")
            print(f"  [Rule] FRAUD_AGGREGATOR ✓ ({(datetime.now() - start_time).total_seconds():.2f}s)", flush=True)
        except Exception as e:
            rules_failed.append(("FRAUD_AGGREGATOR", str(e)[:100]))
            print(f"  [Rule] FRAUD_AGGREGATOR ✗ {str(e)[:100]}", flush=True)


    finally:
        driver.close()

    print(f"[xVigilance-Consumer] Analysis summary: {len(rules_completed)} passed ({', '.join(rules_completed)})", flush=True)
    if rules_failed:
        print(f"[xVigilance-Consumer] {len(rules_failed)} failed: {', '.join(r[0] for r in rules_failed)}", flush=True)
# =====================================================================



def fetch_db_mapping():
    try:
        with psycopg.connect(os.getenv('LINKX_POSTGRES_DSN')) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT config FROM session_configs WHERE session_id = 'xvigilance_system' AND window_id = ''")
                row = cur.fetchone()
                if row and row[0] and "column_mapping" in row[0]:
                    return row[0]["column_mapping"]
    except Exception as e:
        print(f"[xVigilance-Consumer] Failed to load DB mapping: {e}", flush=True)
    return {}


def normalize_transaction_dataframe(df):
    import pandas as pd
    import numpy as np
    
    # 1. DB-Driven Mapping (Safe Application)
    db_mappings = fetch_db_mapping()
    if db_mappings:
        for src_col, tgt_col in db_mappings.items():
            if src_col in df.columns:
                if tgt_col in df.columns:
                    conflicts = df[tgt_col].notna() & df[src_col].notna() & (df[tgt_col] != df[src_col])
                    if conflicts.any():
                        print(f"[xVigilance-Normalizer] WARNING: {conflicts.sum()} mapping conflicts for {src_col}->{tgt_col}", flush=True)
                    df[tgt_col] = df[tgt_col].combine_first(df[src_col])
                else:
                    df[tgt_col] = df[src_col]
                    
    # 2. Canonical Fallback Mappings
    fallbacks = {
        "SENDERACCOUNTID": "ACCOUNTNO",
        "RECEIVERACCOUNTID": "BENACCOUNTNO",
        "TRANSFERAMOUNT": "AMOUNT"
    }
    for src_col, tgt_col in fallbacks.items():
        if src_col in df.columns:
            if tgt_col in df.columns:
                conflicts = df[tgt_col].notna() & df[src_col].notna() & (df[tgt_col] != df[src_col])
                if conflicts.any():
                    print(f"[xVigilance-Normalizer] WARNING: {conflicts.sum()} canonical fallback conflicts for {src_col}->{tgt_col}", flush=True)
                df[tgt_col] = df[tgt_col].combine_first(df[src_col])
            else:
                df[tgt_col] = df[src_col]
                
    # 3. Canonical Account Mapping (Stable String Formatting)
    def _format_account(x):
        if pd.isna(x):
            return x
        if isinstance(x, float) and x.is_integer():
            return str(int(x))
        return str(x)
        
    for col in ["ACCOUNTNO", "BENACCOUNTNO"]:
        if col in df.columns:
            df[col] = df[col].apply(_format_account)
            
    # 4. Amount Normalization
    if "AMOUNT" in df.columns:
        df["AMOUNT"] = pd.to_numeric(df["AMOUNT"], errors='coerce')
        
    # 5. Timestamp Normalization (Authoritative from CREATEDDATE)
    if "CREATEDDATE" in df.columns:
        numeric_dates = pd.to_numeric(df["CREATEDDATE"], errors="coerce")
        valid_mask = numeric_dates.notna() & (numeric_dates > 1000000000000)
        if valid_mask.any():
            dt_series = pd.to_datetime(numeric_dates[valid_mask], unit="ms", utc=True)
            df.loc[valid_mask, "TRANSACTIONDATE"] = dt_series.dt.strftime("%Y-%m-%d")
            df.loc[valid_mask, "TRANSACTIONTIME"] = dt_series.dt.strftime("%H:%M:%S")
            df.loc[valid_mask, "TRANSACTIONTIMESTAMP"] = dt_series.dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            
    # 6. Lowercase Aliases for Graph Mapping
    if "ACCOUNTNO" in df.columns:
        df["accountno"] = df["ACCOUNTNO"]
    if "BENACCOUNTNO" in df.columns:
        df["benaccountno"] = df["BENACCOUNTNO"]
        
    # 7. Validation Logging
    missing_acc = df["ACCOUNTNO"].isna().sum() if "ACCOUNTNO" in df.columns else len(df)
    missing_ben = df["BENACCOUNTNO"].isna().sum() if "BENACCOUNTNO" in df.columns else len(df)
    missing_amt = df["AMOUNT"].isna().sum() if "AMOUNT" in df.columns else len(df)
    missing_tdate = df["TRANSACTIONDATE"].isna().sum() if "TRANSACTIONDATE" in df.columns else len(df)
    missing_ttime = df["TRANSACTIONTIME"].isna().sum() if "TRANSACTIONTIME" in df.columns else len(df)
    
    print(
        f"[xVigilance-Normalizer]\n"
        f"rows={len(df)}\n"
        f"missing ACCOUNTNO={missing_acc}\n"
        f"missing BENACCOUNTNO={missing_ben}\n"
        f"missing AMOUNT={missing_amt}\n"
        f"missing TRANSACTIONDATE={missing_tdate}\n"
        f"missing TRANSACTIONTIME={missing_ttime}", 
        flush=True
    )
    
    return df

def consume_firehose():
    global RUNNING
    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)

    topic = "dev.xvigilance.transactions.raw.v2"
    brokers = os.getenv("LINKX_KAFKA_BOOTSTRAP_SERVERS", "172.27.23.106:9092")
    group_id = "linkx-xvigilance-super-final-500"
    session_id = "xvigilance-daemon"
    
    server_list = [b.strip() for b in brokers.split(",") if b.strip()]
    try:
        c = KafkaConsumer(
            topic,
            bootstrap_servers=server_list,
            group_id=group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            max_poll_interval_ms=300000,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v else None
        )
    except Exception as e:
        print(f"[xVigilance-Consumer] CRITICAL: Could not connect to Kafka broker at {brokers}. Error: {e}", flush=True)
        return

    print("=" * 70, flush=True)
    print(f" xVigilance Governed Ingestion Consumer Online", flush=True)
    print(f" Listening to: {topic}", flush=True)
    print(f" Mode: FAST INGEST → BATCH ANALYSIS at WATERMARK", flush=True)
    print("=" * 70, flush=True)

    buffer = []
    batch_size = 10000
    batch_number = 1

    credentials = _neo4j_credentials(session_id)
    node_label = rule_to_node_label("bank transactions", session_id)
    

    # --- STARTUP PURGE: Clean leftover nodes from previous SIGKILL'd runs ---
    try:
        purge_driver = create_neo4j_driver(credentials)
        safe_purge_label = f"`{str(node_label).replace('`', '')}`"
        purge_total = 0
        with purge_driver.session() as purge_session:
            while True:
                result = purge_session.run(f"MATCH (n:{safe_purge_label}) WITH n LIMIT 10000 DETACH DELETE n RETURN count(n) AS deleted")
                deleted_batch = result.single()["deleted"]
                purge_total += deleted_batch
                if deleted_batch == 0:
                    break
        purge_driver.close()
        if purge_total > 0:
            print(f"[xVigilance-Consumer] Startup purge: {purge_total} stale nodes removed.", flush=True)
        else:
            print("[xVigilance-Consumer] Startup purge: graph is clean.", flush=True)
    except Exception as purge_e:
        print(f"[xVigilance-Consumer] Warning: Startup purge failed: {purge_e}", flush=True)

    # --- ENSURE NEO4J INDEXES EXIST ON STARTUP ---
    try:
        driver = create_neo4j_driver(credentials)
        with driver.session() as session:
            session.run(f"CREATE INDEX idx_node_id IF NOT EXISTS FOR (n:`{node_label}`) ON (n.NodeId)")
            session.run(f"CREATE INDEX idx_batch_id IF NOT EXISTS FOR (n:`{node_label}`) ON (n.batch_id)")
            session.run(f"CREATE INDEX idx_account_no IF NOT EXISTS FOR (n:`{node_label}`) ON (n.ACCOUNTNO)")
            session.run(f"CREATE INDEX idx_ben_account_no IF NOT EXISTS FOR (n:`{node_label}`) ON (n.BENACCOUNTNO)")
            session.run(f"CREATE INDEX idx_logical_acc IF NOT EXISTS FOR (n:`{node_label}`) ON (n.LOGICAL_ACCOUNTNO)")
            session.run(f"CREATE INDEX idx_logical_ben IF NOT EXISTS FOR (n:`{node_label}`) ON (n.LOGICAL_BENACCOUNTNO)")
            session.run(f"CREATE INDEX idx_tx_date IF NOT EXISTS FOR (n:`{node_label}`) ON (n.TRANSACTIONDATE)")
            session.run(f"CREATE INDEX idx_bus_phone IF NOT EXISTS FOR (n:`{node_label}`) ON (n.BUSINESSMOBILENO)")
            session.run(f"CREATE INDEX idx_ben_phone IF NOT EXISTS FOR (n:`{node_label}`) ON (n.BENTELNO)")
            session.run("CREATE INDEX idx_alert_acc IF NOT EXISTS FOR (a:AccountAlert) ON (a.account_no)")
            session.run("CREATE INDEX idx_alert_sess IF NOT EXISTS FOR (a:AccountAlert) ON (a.session_id)")
        print(f"[xVigilance-Consumer] Neo4j Performance Indexes Verified (label: {node_label}).", flush=True)
        driver.close()
    except Exception as e:
        print(f"[xVigilance-Consumer] Warning: Could not verify Neo4j indexes: {e}", flush=True)
    # ---------------------------------------------

    while RUNNING:
        try:
            for msg in c:
                if not RUNNING:
                    break
                    
                data = msg.value
                if not data:
                    continue
                    
                # Check if it's the watermark
                is_watermark = False
                if msg.headers:
                    for k, v in msg.headers:
                        if k == "type" and v == b"watermark":
                            is_watermark = True

                if is_watermark:
                    print(f"[xVigilance-Consumer] Received WATERMARK for window {data.get('window_id')}. Flushing buffer...", flush=True)
                    if buffer:

                        df = pd.DataFrame(buffer)
                        
                        # --- NORMALIZATION APPLIED HERE ---
                        df = normalize_transaction_dataframe(df)
                        # -------------------------------------

                        print(f"[xVigilance-Consumer] Fast-ingesting {len(df)} remaining records to Neo4j...", flush=True)
                        fast_ingest_batch(credentials, session_id, df, batch_number, node_label)
                        buffer.clear()
                        batch_number += 1

                    # ============================================================
                    # FULL BATCH ANALYSIS: Run ALL rules on the complete hour graph
                    # ============================================================
                    print("[xVigilance-Consumer] Ingestion complete. Running FULL batch LA rules (Smurfing, Circular Flow, Hub&Spoke, etc.)...", flush=True)
                    t0 = time.time()
                    try:
                        run_full_graph_analysis(credentials, session_id, node_label)
                        analysis_time = time.time() - t0
                        print(f"[xVigilance-Consumer] Full batch analysis completed in {analysis_time:.1f}s.", flush=True)
                    except Exception as analysis_err:
                        print(f"[xVigilance-Consumer] Error during batch analysis: {analysis_err}", flush=True)
                        import traceback
                        traceback.print_exc()
                    # ============================================================

                    # PROMOTE: Read anomaly relationships and save to PostgreSQL
                    promote_anomalies_to_postgres(credentials, session_id, data.get('window_id'), execution_meta={'total_records': data.get('total_records'), 'batch_id': data.get('batch_id'), 'elastic_endpoint': data.get('elastic_endpoint'), 'worker_node': data.get('worker_node')})
                    
                    # EPHEMERAL GRAPH WIPE: Purge nodes for this window to protect RAM
                    print(f"[xVigilance-Consumer] Executing Ephemeral Graph Wipe for window {data.get('window_id')}...", flush=True)
                    try:
                        safe_wipe_label = f"`{str(node_label).replace('`', '')}`"
                        driver = create_neo4j_driver(credentials)
                        deleted_total = 0
                        with driver.session() as session:
                            while True:
                                result = session.run(f"MATCH (n:{safe_wipe_label}) WITH n LIMIT 10000 DETACH DELETE n RETURN count(n) AS deleted")
                                deleted_batch = result.single()["deleted"]
                                deleted_total += deleted_batch
                                if deleted_batch == 0:
                                    break
                        driver.close()
                        print(f"[xVigilance-Consumer] Ephemeral Wipe complete: {deleted_total} nodes purged.", flush=True)
                    except Exception as wipe_e:
                        print(f"[xVigilance-Consumer] Warning: Failed to execute graph wipe: {wipe_e}", flush=True)
                        
                    # UPDATE POSTGRES CHECKPOINT FOR FRONTEND UI
                    try:
                        with psycopg.connect(os.getenv('LINKX_POSTGRES_DSN')) as conn:
                            with conn.cursor() as cur:
                                cur.execute("UPDATE xvigilance_checkpoints SET total_graph_analyzed = total_graph_analyzed + %s", (data.get('total_records', 0),))
                                cur.execute("UPDATE xvigilance_slice_runs SET status = 'succeeded', finished_at = NOW() WHERE window_end = %s", (data.get('window_id'),))
                            conn.commit()
                        print(f"[xVigilance-Consumer] Checkpoint total_graph_analyzed advanced by {data.get('total_records', 0)}.", flush=True)
                    except Exception as pg_e:
                        print(f"[xVigilance-Consumer] Failed to update PostgreSQL graph checkpoint: {pg_e}", flush=True)
                        
                    print(f"[xVigilance-Consumer] Window {data.get('window_id')} finalized successfully.", flush=True)
                    batch_number = 1  # Reset batch counter for next window
                    continue

                # It's a standard transaction — buffer it
                buffer.append(data)
                
                if len(buffer) >= batch_size:
                    df = pd.DataFrame(buffer)
                    
                    # --- NORMALIZATION APPLIED HERE ---
                    df = normalize_transaction_dataframe(df)
                    # ----------------------------------
                    
                    print(f"[xVigilance-Consumer] Fast-ingesting micro-batch {batch_number} ({len(df)} records) to Neo4j...", flush=True)
                    t0 = time.time()
                    try:
                        fast_ingest_batch(credentials, session_id, df, batch_number, node_label)
                        ingest_time = time.time() - t0
                        print(f"[xVigilance-Consumer] Micro-batch {batch_number} ingested in {ingest_time:.1f}s.", flush=True)
                    except Exception as e:
                        print(f"[xVigilance-Consumer] Error during Neo4j insertion: {e}", flush=True)
                    buffer.clear()
                    batch_number += 1

        except Exception as e:
            print(f"[xVigilance-Consumer] FATAL KAFKA ERROR in consumer loop: {e}", flush=True)
            import traceback
            traceback.print_exc()
            print("[xVigilance-Consumer] Tearing down Kafka connection and rebooting consumer from scratch...", flush=True)
            try:
                c.close()
            except:
                pass
            return  # The immortal wrapper in __main__ will reboot it!

    c.close()

if __name__ == "__main__":
    import time
    while RUNNING:
        try:
            consume_firehose()
        except Exception as e:
            print(f"[xVigilance-Consumer] FATAL CRASH CAUGHT: {e}", flush=True)
            import traceback
            traceback.print_exc()
            if RUNNING:
                print("[xVigilance-Consumer] RESTARTING IN 5 SECONDS...", flush=True)
                time.sleep(5)
    print("[xVigilance-Consumer] Daemon successfully exited.", flush=True)
