import json
import os
from datetime import datetime, timezone

DEFAULT_KAFKA_BROKERS = os.getenv(
    "LINKX_KAFKA_BOOTSTRAP_SERVERS",
    "172.27.23.70:9092,172.27.23.118:9092,172.27.23.100:9092",
)
DEFAULT_FLAGGED_TOPIC = os.getenv(
    "LINKX_KAFKA_RISK_SCORING_FLAGGED_TOPIC", "dev.analysis.link.flagged.v1"
)


def _iso8601_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def publish_xvigilance_flagged_event(
    account_no: str,
    reason: str = "XVigilance Anomaly Detected",
    linked_entities: list | None = None,
    trace_id: str | None = None,
    correlation_id: str | None = None,
    brokers: str | None = None,
    topic: str | None = None,
) -> bool:
    """
    Publishes an autonomous detective flagged event directly to dev.analysis.link.flagged.v1.
    """
    account_no = str(account_no or "").strip()
    if not account_no:
        return False

    linked_entities = list(linked_entities or [])
    trace_id = trace_id or os.urandom(16).hex()
    span_id = os.urandom(8).hex()
    correlation_id = correlation_id or trace_id
    brokers = brokers or DEFAULT_KAFKA_BROKERS
    target_topic = topic or DEFAULT_FLAGGED_TOPIC

    event_payload = {
        "schema_version": "1.0",
        "success": True,
        "event_type": "link.flagged",
        "message": f"XVigilance flagged account {account_no}: {reason}",
        "data": {
            "accountno": account_no,
            "linked_accounts_count": len(linked_entities),
            "flagged_entity_links": len(linked_entities) if linked_entities else 1,
            "beneficiary_blacklisted": True,
            "linked_entities": linked_entities,
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
                "destination_name": target_topic,
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

    try:
        # First save the finding in the local reports pipeline
        try:
            from linkx_xvigilance.db import connect
            with connect(application_name="xvigilance-report-ingest") as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO linkx_reports (report_type, source_system, external_reference_id, payload, status)
                        VALUES (%s, %s, %s, %s, %s)
                    """, ('XVIGILANCE_FINDING', 'linkx_xvigilance', trace_id, json.dumps(event_payload), 'NEW'))
                    conn.commit()
            print(f"[xvigilance] Saved report finding to database for {account_no}", flush=True)
        except Exception as db_exc:
            print(f"[xvigilance] Warning: Failed to save report to database: {db_exc}", flush=True)

        from confluent_kafka import Producer
        conf = {"bootstrap.servers": brokers, "client.id": "linkx-xvigilance-producer"}
        p = Producer(conf)
        key = account_no.encode("utf-8")
        val = json.dumps(event_payload).encode("utf-8")
        p.produce(target_topic, key=key, value=val)
        p.flush(timeout=5.0)
        print(f"[xvigilance] Successfully published flagged detection for {account_no} to {target_topic}", flush=True)
        return True
    except Exception as exc:
        print(f"[xvigilance] Warning: Failed to publish flagged detection to Kafka ({target_topic}): {exc}", flush=True)
        return False
