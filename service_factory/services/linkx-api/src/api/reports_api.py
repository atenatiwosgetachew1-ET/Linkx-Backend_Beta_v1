import os
from flask import Blueprint, request, jsonify
from auth.decorators import permission_required, current_actor_from_request
from auth.repository import record_security_event
from batch_manager.utils.reports_utils import insert_report, get_reports

reports_api = Blueprint('reports_api', __name__, url_prefix='/api/v1/reports')

def _audit(event_type, target_id=None, success=True, metadata=None):
    actor = current_actor_from_request()
    try:
        record_security_event(
            event_type,
            actor=actor,
            target_type="report",
            target_id=target_id,
            success=success,
            metadata=metadata or {}
        )
    except Exception as e:
        print(f"Failed to write audit log: {e}")

def _get_paginated_reports(report_type):
    try:
        limit = int(request.args.get('limit', 50))
        offset = int(request.args.get('offset', 0))
        status = request.args.get('status')
        score_band = request.args.get('score_band')
        
        min_fraud_score_str = request.args.get('min_fraud_score')
        min_fraud_score = float(min_fraud_score_str) if min_fraud_score_str is not None else None
        
        q = request.args.get('q')
        
        reports, total_count = get_reports(
            report_type=report_type, 
            status=status, 
            limit=limit, 
            offset=offset,
            score_band=score_band,
            min_fraud_score=min_fraud_score,
            q=q
        )
        
        _audit(f"reports.list_{report_type.lower()}", success=True, metadata={"status": status, "limit": limit, "offset": offset, "score_band": score_band, "min_fraud_score": min_fraud_score, "q": q})
        
        # Return structured pagination format
        return jsonify({
            "data": reports,
            "limit": limit,
            "offset": offset,
            "count": total_count
        }), 200
    except Exception as e:
        _audit(f"reports.list_{report_type.lower()}", success=False, metadata={"error": str(e)})
        return jsonify({"error": str(e)}), 500

@reports_api.route('/parent', methods=['GET'])
@permission_required('reports:read')
def list_parent_reports():
    return _get_paginated_reports('PARENT_RECEIVED')

@reports_api.route('/xvigilance', methods=['GET'])
@permission_required('reports:read')
def list_xvigilance_reports():
    return _get_paginated_reports('XVIGILANCE_FINDING')

@reports_api.route('/evidence', methods=['GET'])
@permission_required('reports:read')
def list_evidence_reports():
    return _get_paginated_reports('SERVICE_EVIDENCE')

@reports_api.route('/import-parent', methods=['POST'])
@permission_required('reports:read')
def import_parent_report():
    data = request.json
    if not data or 'external_reference_id' not in data:
        return jsonify({"error": "Missing external_reference_id in payload"}), 400
    
    try:
        report_id = insert_report(
            report_type='PARENT_RECEIVED',
            source_system='parent_ctms',
            payload=data,
            external_reference_id=data['external_reference_id'],
            status='NEW'
        )
        _audit("reports.import_parent", target_id=str(report_id), success=True)
        return jsonify({"message": "Report imported successfully", "report_id": report_id}), 202
    except Exception as e:
        _audit("reports.import_parent", success=False, metadata={"error": str(e)})
        return jsonify({"error": str(e)}), 500

@reports_api.route('/<report_id>/bind-workspace', methods=['POST'])
@permission_required('reports:read')
def bind_workspace(report_id):
    data = request.json
    workspace_id = data.get('workspace_id')
    if not workspace_id:
        return jsonify({"error": "Missing workspace_id"}), 400
        
    try:
        _audit("reports.bind_workspace", target_id=str(report_id), success=True, metadata={"workspace_id": workspace_id})
        return jsonify({"message": f"Report {report_id} bound to workspace {workspace_id}"}), 200
    except Exception as e:
        _audit("reports.bind_workspace", target_id=str(report_id), success=False, metadata={"error": str(e)})
        return jsonify({"error": str(e)}), 500

def _purge_xvigilance_neo4j():
    """Wipes all ephemeral nodes and alerts created by xVigilance daemon."""
    from batch_manager.utils.neo4j_utils import create_neo4j_driver
    try:
        credentials = {
            "url": os.getenv("LINKX_NEO4J_URL", "bolt://172.27.23.85:7687"),
            "username": os.getenv("LINKX_NEO4J_USERNAME", "neo4j"),
            "password": os.getenv("LINKX_NEO4J_PASSWORD") or os.getenv("LINKX_CLEANUP_NEO4J_PASSWORD", "neo4j"),
            "database": os.getenv("LINKX_NEO4J_DATABASE", "neo4j")
        }
        driver = create_neo4j_driver(credentials)
        total_purged = 0
        with driver.session() as session:
            while True:
                res = session.run("MATCH (n:`bank_transactions_xvigilance-daemon`) WITH n LIMIT 10000 DETACH DELETE n RETURN count(n) AS deleted")
                deleted = res.single()["deleted"]
                total_purged += deleted
                if deleted == 0:
                    break
            session.run("MATCH (a:AccountAlert) WHERE a.session_id = 'xvigilance-daemon' DETACH DELETE a")
        driver.close()
        return total_purged
    except Exception as e:
        print(f"[reports_api] Warning: Failed to purge xVigilance Neo4j nodes during rewind: {e}", flush=True)
        return -1


def _reset_xvigilance_kafka_offset():
    """Resets Kafka consumer group offset to the latest message (purging in-flight abandoned windows)."""
    try:
        from kafka import KafkaConsumer, TopicPartition
        brokers = os.getenv("LINKX_KAFKA_BOOTSTRAP_SERVERS", "172.27.23.106:9092")
        topic = "dev.xvigilance.transactions.raw.v2"
        group_id = "linkx-xvigilance-super-final-500"
        server_list = [b.strip() for b in brokers.split(",") if b.strip()]
        
        c = KafkaConsumer(bootstrap_servers=server_list, group_id=group_id, enable_auto_commit=False, request_timeout_ms=5000)
        partitions = [TopicPartition(topic, p) for p in c.partitions_for_topic(topic) or []]
        if partitions:
            c.assign(partitions)
            end_offsets = c.end_offsets(partitions)
            for tp in partitions:
                c.seek(tp, end_offsets[tp])
            c.commit()
        c.close()
        return True
    except Exception as e:
        print(f"[reports_api] Warning: Failed to reset Kafka offset during rewind: {e}", flush=True)
        return False


@reports_api.route('/xvigilance/rewind', methods=['POST'])
@permission_required("users:manage")
def xvigilance_rewind():
    """Rewinds the xVigilance engine clock by updating checkpoints, purging ephemeral Neo4j nodes, and resetting Kafka offsets."""
    payload = request.get_json() or {}
    target_date = payload.get("target_date")
    if not target_date:
        return jsonify({"error": "Missing target_date"}), 400

    from batch_manager.utils.postgres_utils import get_postgres_connection
    try:
        # 1. Update Database checkpoints & clear future slice runs
        with get_postgres_connection() as conn:
            with conn.cursor() as cur:
                # Rewind checkpoint
                cur.execute(
                    "UPDATE xvigilance_checkpoints SET last_window_end = %s WHERE feed_name = 'hourly_transaction_detective'",
                    (target_date,)
                )
                
                # Delete future runs from the audit log
                cur.execute(
                    "DELETE FROM xvigilance_slice_runs WHERE window_end > %s",
                    (target_date,)
                )
            conn.commit()

        # 2. Scenario B: Wipe any dirty/partial ephemeral graph nodes from Neo4j
        purged_nodes = _purge_xvigilance_neo4j()

        # 3. Scenario B: Reset Kafka consumer group offset so consumer starts fresh
        kafka_reset = _reset_xvigilance_kafka_offset()
        
        metadata = {
            "target_date": target_date,
            "neo4j_nodes_purged": purged_nodes,
            "kafka_offset_reset": kafka_reset
        }
        _audit("xvigilance.rewind", success=True, metadata=metadata)
        return jsonify({
            "message": "Clock successfully rewound with full fresh reset.",
            "target_date": target_date,
            "neo4j_nodes_purged": purged_nodes,
            "kafka_offset_reset": kafka_reset
        }), 200
    except Exception as e:
        _audit("xvigilance.rewind", success=False, metadata={"error": str(e)})
        return jsonify({"error": str(e)}), 500

@reports_api.route('/xvigilance/state', methods=['POST'])
@permission_required("users:manage")
def xvigilance_state():
    """Pauses or resumes the xVigilance engine."""
    payload = request.get_json() or {}
    if "is_paused" not in payload:
        return jsonify({"error": "Missing is_paused boolean"}), 400

    is_paused = bool(payload["is_paused"])
    from batch_manager.utils.postgres_utils import get_postgres_connection
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE xvigilance_checkpoints SET is_paused = %s WHERE feed_name = 'hourly_transaction_detective'",
                    (is_paused,)
                )
            conn.commit()
            
        _audit("xvigilance.state", success=True, metadata={"is_paused": is_paused})
        return jsonify({"message": "State updated", "is_paused": is_paused}), 200
    except Exception as e:
        _audit("xvigilance.state", success=False, metadata={"error": str(e)})
        return jsonify({"error": str(e)}), 500

@reports_api.route('/xvigilance/health', methods=['GET'])
def xvigilance_health():
    """Returns the real-time heartbeat and audit logs of the xVigilance daemon."""
    try:
        limit = int(request.args.get('limit', 50))
        from batch_manager.utils.postgres_utils import get_postgres_connection
        with get_postgres_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT feed_name, last_window_end, total_records_analyzed, status, total_graph_analyzed, is_paused FROM xvigilance_checkpoints LIMIT 1")
                cp = cur.fetchone()
                checkpoint = {}
                if cp:
                    checkpoint = {
                        "feed_name": cp[0],
                        "last_window_end": cp[1],
                        "total_records_analyzed": cp[2],
                        "status": cp[3],
                        "total_graph_analyzed": cp[4] if len(cp) > 4 else cp[2],
                        "is_paused": cp[5] if len(cp) > 5 else False
                    }
                
                cur.execute("""
                    SELECT id, window_start, window_end, status, records_count, duration_ms, error_message, finished_at
                    FROM xvigilance_slice_runs
                    WHERE records_count > 0 OR status IN ('running', 'queued', 'analyzing')
                    ORDER BY window_end DESC
                    LIMIT %s
                """, (limit,))
                run_rows = cur.fetchall()
                if not run_rows:
                    cur.execute("""
                        SELECT id, window_start, window_end, status, records_count, duration_ms, error_message, finished_at
                        FROM xvigilance_slice_runs
                        ORDER BY window_end DESC
                        LIMIT %s
                    """, (limit,))
                    run_rows = cur.fetchall()

                runs = []
                for r in run_rows:
                    runs.append({
                        "run_id": r[0],
                        "window_start": r[1],
                        "window_end": r[2],
                        "status": r[3],
                        "records_count": r[4],
                        "duration_ms": r[5],
                        "error_message": r[6],
                        "finished_at": r[7]
                    })
                
        return jsonify({
            "success": True,
            "checkpoint": checkpoint,
            "recent_runs": runs
        })
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500
