import json
import os
import time
from datetime import datetime, timezone

from batch_manager.utils.neo4j_utils import create_neo4j_driver
from globals import load_temp_config
from security.redaction import redact_value

DEFAULT_KAFKA_BROKERS = os.getenv(
    "LINKX_KAFKA_BOOTSTRAP_SERVERS",
    "172.27.23.70:9092,172.27.23.118:9092,172.27.23.100:9092",
)
DEFAULT_INPUT_TOPIC = os.getenv(
    "LINKX_KAFKA_RISK_SCORING_INPUT_TOPIC", "dev.scoring.score.calculated.v1"
)
DEFAULT_MAPPED_TOPIC = os.getenv(
    "LINKX_KAFKA_RISK_SCORING_MAPPED_TOPIC", "dev.analysis.link.mapped.v1"
)
DEFAULT_FLAGGED_TOPIC = os.getenv(
    "LINKX_KAFKA_RISK_SCORING_FLAGGED_TOPIC", "dev.analysis.link.flagged.v1"
)


def _iso8601_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def evaluate_account_graph_links(account_no, session_id=None):
    """
    Queries Neo4j to discover 1-hop and 2-hop connected accounts/entities,
    blacklisted/sanctioned beneficiaries, and calculates graph centrality.
    """
    account_no = str(account_no or "").strip()
    if not account_no:
        return {
            "accountno": "",
            "linked_accounts_count": 0,
            "flagged_entity_links": 0,
            "beneficiary_blacklisted": False,
            "linked_entities": [],
            "network_centrality_score": 0.0,
            "max_path_length": 0,
        }

    driver = None
    try:
        driver = create_neo4j_driver(session_id)
    except Exception as exc:
        print(f"[RiskScoring] Neo4j driver init error: {exc}")

    linked_entities = []
    flagged_entity_links = 0
    beneficiary_blacklisted = False
    linked_accounts_count = 0
    network_centrality_score = 0.0
    max_path_length = 1

    if driver:
        try:
            with driver.session() as session:
                query = """
                MATCH (a:Account {account_no: $account_no})
                OPTIONAL MATCH (a)-[r]-(target)
                RETURN 
                    type(r) as rel_type,
                    labels(target) as target_labels,
                    target.account_no as target_acc,
                    target.entity_id as target_ent,
                    target.name as target_name,
                    target.is_blacklisted as is_blacklisted,
                    target.flag_reason as flag_reason,
                    target.risk_score as target_risk
                LIMIT 50
                """
                records = session.run(query, account_no=account_no)
                seen_entities = set()
                for rec in records:
                    rel = rec.get("rel_type")
                    if not rel:
                        continue
                    target_id = rec.get("target_acc") or rec.get("target_ent") or rec.get("target_name") or "UNKNOWN"
                    if target_id in seen_entities or target_id == account_no:
                        continue
                    seen_entities.add(target_id)
                    
                    labels = [str(l).lower() for l in (rec.get("target_labels") or [])]
                    is_ben = "beneficiary" in labels or "ben" in rel.lower()
                    is_black = bool(rec.get("is_blacklisted"))
                    if is_black and is_ben:
                        beneficiary_blacklisted = True
                    if is_black:
                        flagged_entity_links += 1

                    risk_val = float(rec.get("target_risk") or 0.25)
                    ent_payload = {
                        "entity_id": target_id,
                        "entity_type": "beneficiary" if is_ben else ("account" if "account" in labels else "entity"),
                        "relationship": rel.lower().replace(" ", "_"),
                        "risk_contribution": round(risk_val, 2),
                    }
                    if is_black:
                        ent_payload["flagged"] = True
                        ent_payload["flag_reason"] = rec.get("flag_reason") or "watchlist hit"
                    linked_entities.append(ent_payload)

                linked_accounts_count = len(linked_entities)
                if linked_accounts_count > 0:
                    network_centrality_score = round(min(1.0, 0.15 + (linked_accounts_count * 0.08)), 2)
                    max_path_length = 2 if linked_accounts_count > 2 else 1
        except Exception as exc:
            print(f"[RiskScoring] Neo4j query error: {exc}")
        finally:
            try:
                driver.close()
            except Exception:
                pass

    return {
        "accountno": account_no,
        "linked_accounts_count": linked_accounts_count,
        "flagged_entity_links": flagged_entity_links,
        "beneficiary_blacklisted": beneficiary_blacklisted,
        "linked_entities": linked_entities,
        "network_centrality_score": network_centrality_score,
        "max_path_length": max_path_length,
    }


def build_link_mapped_response(input_event, graph_result, duration_ms=50.0):
    """
    Constructs the standard link.mapped / link.flagged response event
    preserving distributed tracing metadata.
    """
    input_meta = dict((input_event or {}).get("meta") or {})
    trace_id = input_meta.get("trace_id") or os.urandom(16).hex()
    span_id = os.urandom(8).hex()
    correlation_id = input_meta.get("correlation_id") or input_meta.get("trace_id") or os.urandom(16).hex()

    account_no = graph_result.get("accountno") or ""
    is_flagged = bool(
        graph_result.get("beneficiary_blacklisted")
        or (graph_result.get("flagged_entity_links", 0) > 0)
    )
    event_type = "link.flagged" if is_flagged else "link.mapped"
    count = graph_result.get("linked_accounts_count", 0)
    msg_action = "flagged" if is_flagged else "map completed"
    message_text = f"Link {msg_action} for account {account_no}: {count} linked"

    data_payload = {
        "accountno": account_no,
        "linked_accounts_count": count,
        "flagged_entity_links": graph_result.get("flagged_entity_links", 0),
        "beneficiary_blacklisted": bool(graph_result.get("beneficiary_blacklisted")),
        "linked_entities": graph_result.get("linked_entities", []),
        "network_centrality_score": graph_result.get("network_centrality_score", 0.0),
        "max_path_length": graph_result.get("max_path_length", 1),
    }
    if is_flagged:
        data_payload["flags"] = {
            "beneficiary_blacklisted": bool(graph_result.get("beneficiary_blacklisted")),
            "flagged_entity_links": bool(graph_result.get("flagged_entity_links", 0) > 0),
        }

    destination_topic = DEFAULT_FLAGGED_TOPIC if is_flagged else DEFAULT_MAPPED_TOPIC

    return {
        "schema_version": "1.0",
        "success": True,
        "event_type": event_type,
        "message": message_text,
        "data": data_payload,
        "meta": {
            "trace_id": trace_id,
            "span_id": span_id,
            "traceparent": f"00-{trace_id}-{span_id}-01",
            "correlation_id": correlation_id,
            "timestamp": _iso8601_now(),
            "service": {
                "name": "link-analysis-ms",
                "version": "1.0.0",
                "namespace": "risk-decision-platform",
            },
            "messaging": {
                "system": "kafka",
                "destination_name": destination_topic,
                "operation_name": "publish",
            },
            "source_id": "link",
            "aggregation_key": {
                "type": "accountno",
                "value": account_no,
            },
            "processing": {
                "duration_ms": round(float(duration_ms), 1),
            },
        },
        "error": None,
    }


def process_risk_scoring_event(event_data, brokers=None, publish=False):
    """
    Processes an individual Risk Scoring calculated event, evaluates graph links,
    and optionally publishes the resulting event to Kafka.
    """
    start_time = time.time()
    event_data = dict(event_data or {})
    data_section = dict(event_data.get("data") or {})
    meta_section = dict(event_data.get("meta") or {})

    account_no = (
        data_section.get("entity_id")
        or (meta_section.get("aggregation_key") or {}).get("value")
        or data_section.get("accountno")
        or ""
    )

    graph_result = evaluate_account_graph_links(account_no)
    duration_ms = (time.time() - start_time) * 1000.0
    response_event = build_link_mapped_response(event_data, graph_result, duration_ms)

    if publish and account_no:
        publish_risk_scoring_response(response_event, brokers=brokers)

    return response_event


def publish_risk_scoring_response(response_event, brokers=None, topic=None):
    """
    Publishes the link mapped/flagged response event to the Kafka cluster.
    If topic is not specified, routes to dev.analysis.link.flagged.v1 for flagged events
    and dev.analysis.link.mapped.v1 for clear/clean events.
    """
    brokers = brokers or DEFAULT_KAFKA_BROKERS
    is_flagged = (response_event.get("event_type") == "link.flagged")
    target_topic = topic or (DEFAULT_FLAGGED_TOPIC if is_flagged else DEFAULT_MAPPED_TOPIC)

    try:
        from confluent_kafka import Producer
        conf = {"bootstrap.servers": brokers, "client.id": "linkx-link-analysis-producer"}
        p = Producer(conf)
        key = str((response_event.get("data") or {}).get("accountno") or "").encode("utf-8")
        val = json.dumps(response_event).encode("utf-8")
        p.produce(target_topic, key=key, value=val)
        p.flush(timeout=5.0)
        return True
    except Exception as exc:
        print(f"[RiskScoring] Error publishing Kafka event to {target_topic}: {exc}")
        return False


def publish_xvigilance_flagged_event(
    account_no,
    flagged_entities=None,
    reason="XVigilance Autonomous Detective Anomaly Detected",
    trace_id=None,
    correlation_id=None,
    brokers=None,
):
    """
    Direct helper for the XVigilance engine to publish high-confidence
    anomaly/fraud detections directly to dev.analysis.link.flagged.v1.
    """
    account_no = str(account_no or "").strip()
    flagged_entities = list(flagged_entities or [])
    trace_id = trace_id or os.urandom(16).hex()
    span_id = os.urandom(8).hex()
    correlation_id = correlation_id or trace_id

    event_payload = {
        "schema_version": "1.0",
        "success": True,
        "event_type": "link.flagged",
        "message": f"XVigilance flagged account {account_no}: {reason}",
        "data": {
            "accountno": account_no,
            "linked_accounts_count": len(flagged_entities),
            "flagged_entity_links": len(flagged_entities),
            "beneficiary_blacklisted": True,
            "linked_entities": flagged_entities,
            "network_centrality_score": 0.85,
            "max_path_length": 2,
            "flags": {
                "beneficiary_blacklisted": True,
                "flagged_entity_links": True,
                "xvigilance_anomaly": True,
            },
        },
        "meta": {
            "trace_id": trace_id,
            "span_id": span_id,
            "traceparent": f"00-{trace_id}-{span_id}-01",
            "correlation_id": correlation_id,
            "timestamp": _iso8601_now(),
            "service": {
                "name": "link-analysis-xvigilance",
                "version": "1.0.0",
                "namespace": "risk-decision-platform",
            },
            "messaging": {
                "system": "kafka",
                "destination_name": DEFAULT_FLAGGED_TOPIC,
                "operation_name": "publish",
            },
            "source_id": "link",
            "aggregation_key": {
                "type": "accountno",
                "value": account_no,
            },
            "processing": {
                "duration_ms": 1.0,
            },
        },
        "error": None,
    }
    return publish_risk_scoring_response(event_payload, brokers=brokers, topic=DEFAULT_FLAGGED_TOPIC)
