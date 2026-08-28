import time
import json
import requests
import urllib3
from batch_manager.utils.postgres_utils import get_postgres_connection
from batch_manager.analyzing.analyzer import analyzer

# Suppress insecure request warnings since we are using internal self-signed certs for the webhook
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

AGGREGATOR_WEBHOOK_URL = "https://risk-platform.local/api/v1/aggregate/callback"

def process_webhook_job(payload):
    """
    Worker task that runs on Server 3.
    Expects payload: {"job_id": "...", "account_no": "..."}
    """
    job_id = payload.get("job_id")
    account_no = payload.get("account_no")
    
    if not job_id or not account_no:
        return {"status": "failed", "error": "Missing job_id or account_no"}
        
    start_time = time.time()
    findings = None
    duration_ms = 0.0
    
    # --- 1. Deduplication Check ---
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                SELECT response_payload, duration_ms
                FROM link_analysis_evidence
                WHERE entity_id = %s AND analyzed_at >= NOW() - INTERVAL '2 hours'
                ORDER BY analyzed_at DESC LIMIT 1
                """, (str(account_no),))
                row = cur.fetchone()
                if row:
                    findings = row[0] if isinstance(row[0], dict) else json.loads(row[0])
                    duration_ms = float(row[1] or 0.0)
                    print(f"[RiskScoringWorker] Replaying cached evidence for {account_no}")
    except Exception as e:
        print(f"[RiskScoringWorker] Cache read error for {account_no}: {e}")

    # --- 2. Fresh Analysis ---
    if not findings:
        analysis_payload = {
            "entity": "bank",
            "search_term": account_no,
            "match_exact": True
        }
        try:
            findings = analyzer(analysis_payload)
        except Exception as e:
            findings = {"error": str(e), "status": "failed"}
            
        duration_ms = (time.time() - start_time) * 1000
        
        # --- 3. Persist Evidence to PostgreSQL ---
        try:
            is_flagged = bool(findings.get("is_flagged") or findings.get("beneficiary_blacklisted"))
            linked_count = int(findings.get("linked_accounts_count") or 0)
            flagged_rules = findings.get("flagged_rules") or []
            
            with get_postgres_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                    INSERT INTO link_analysis_evidence (
                        trace_id, correlation_id, entity_id, entity_type,
                        session_id, event_type,
                        is_flagged, flagged_rules, linked_accounts_count,
                        duration_ms, request_payload, response_payload, analyzed_at
                    )
                    VALUES (
                        %s, %s, %s, 'accountno',
                        'webhook_sync', 'link.mapped',
                        %s, %s::jsonb, %s,
                        %s, %s::jsonb, %s::jsonb, NOW()
                    )
                    ON CONFLICT (trace_id, entity_id) DO NOTHING
                    """, (
                        str(job_id), str(job_id), str(account_no),
                        is_flagged, json.dumps(flagged_rules), linked_count,
                        round(duration_ms, 2), json.dumps({"job_id": job_id, "account_no": account_no}), json.dumps(findings)
                    ))
                conn.commit()
        except Exception as e:
            print(f"[RiskScoringWorker] Failed to save evidence for {account_no}: {e}")

    # --- 4. Build and Fire Webhook Callback ---
    callback_payload = {
        "job_id": job_id,
        "schema_version": "1.0",
        "account_no": account_no,
        "source": "link",
        "data": findings,
        "processing": {
            "duration_ms": round(duration_ms, 2)
        }
    }
    
    try:
        response = requests.post(AGGREGATOR_WEBHOOK_URL, json=callback_payload, timeout=10, verify=False)
        if response.status_code >= 200 and response.status_code < 300:
            print(f"[RiskScoringWorker] Successfully sent callback for {account_no}. Status: {response.status_code}")
            return {"status": "success", "account_no": account_no}
        else:
            print(f"[RiskScoringWorker] Aggregator REJECTED callback for {account_no}. Status: {response.status_code}, Response: {response.text}")
            return {"status": "failed", "error": f"Aggregator returned {response.status_code}"}
    except Exception as e:
        print(f"[RiskScoringWorker] Failed to reach aggregator network for {account_no}: {e}")
        return {"status": "failed", "error": str(e)}
