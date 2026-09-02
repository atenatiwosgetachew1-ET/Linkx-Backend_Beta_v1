import uuid
import json
from datetime import datetime
from batch_manager.utils.postgres_utils import get_postgres_connection
from psycopg.rows import dict_row

def insert_report(report_type, source_system, payload, external_reference_id=None, status='NEW'):
    with get_postgres_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO linkx_reports (report_type, source_system, payload, external_reference_id, status)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id
            """, (report_type, source_system, json.dumps(payload), external_reference_id, status))
            report_id = cur.fetchone()[0]
            conn.commit()
            return report_id

def get_reports(report_type=None, status=None, limit=50, offset=0):
    with get_postgres_connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            query = "SELECT * FROM linkx_reports WHERE 1=1"
            params = []
            if report_type:
                query += " AND report_type = %s"
                params.append(report_type)
            if status:
                query += " AND status = %s"
                params.append(status)
            query += " ORDER BY created_at DESC LIMIT %s OFFSET %s"
            params.extend([limit, offset])
            
            cur.execute(query, tuple(params))
            return cur.fetchall()

def insert_report_evidence(report_id, artifact_id, evidence_metadata):
    with get_postgres_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO linkx_report_evidence (report_id, artifact_id, evidence_metadata)
                VALUES (%s, %s, %s)
            """, (report_id, artifact_id, json.dumps(evidence_metadata)))
            conn.commit()

def queue_outbox_sync(report_id, target_system, payload):
    with get_postgres_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO report_sync_outbox (report_id, target_system, payload)
                VALUES (%s, %s, %s)
            """, (report_id, target_system, json.dumps(payload)))
            conn.commit()
