import threading
import time
import requests
import os
from flask import Blueprint, jsonify, request
from auth.decorators import current_actor_from_request
from batch_manager.analyzing.analyzer import analyzer
import urllib3

# Suppress insecure request warnings since we are using internal self-signed certs for the webhook
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

risk_scoring_api = Blueprint("risk_scoring_api", __name__)

# The actual aggregator webhook URL with the required SNI hostname
AGGREGATOR_WEBHOOK_URL = "https://risk-platform.local/api/v1/aggregate/callback"


import json
from batch_manager.utils.postgres_utils import get_postgres_connection

def process_accounts_background(job_id, account_numbers):
    """Runs in the background, analyzes accounts, and fires webhooks."""
    for account_no in account_numbers:
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
                        print(f"[RiskScoring] Replaying cached evidence for {account_no}")
        except Exception as e:
            print(f"[RiskScoring] Cache read error for {account_no}: {e}")

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
                            is_flagged, flagged_rules, linked_accounts_count,
                            duration_ms, request_payload, response_payload, analyzed_at
                        )
                        VALUES (
                            %s, %s, %s, 'accountno',
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
                print(f"[RiskScoring] Failed to save evidence for {account_no}: {e}")

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
            requests.post(AGGREGATOR_WEBHOOK_URL, json=callback_payload, timeout=10, verify=False)
            print(f"[RiskScoring] Successfully sent callback for {account_no}")
        except Exception as e:
            print(f"[RiskScoring] Failed to send webhook for {account_no}: {e}")


@risk_scoring_api.route('/analysis_request', methods=['POST'])
def analysis_request():
    # --- Parallel Authentication Logic ---
    # 1. Check for API Key first
    api_key = request.headers.get("X-API-Key")
    valid_api_key = os.getenv("LINKX_RISK_SCORING_API_KEY")
    is_authenticated = False
    
    if api_key and valid_api_key and api_key == valid_api_key:
        is_authenticated = True
    else:
        # 2. Fallback to JWT Token check
        actor = current_actor_from_request()
        if actor:
            is_authenticated = True
            
    if not is_authenticated:
        return jsonify({"success": False, "message": "Unauthorized - Invalid API Key or JWT Token"}), 401
    # -------------------------------------

    data = request.get_json() or {}
    job_id = data.get("job_id")
    account_numbers = data.get("account_numbers") or []
    
    # Validation
    if not job_id or not account_numbers:
        return jsonify({"success": False, "message": "Missing job_id or account_numbers"}), 400
        
    # Spin up background thread so we can return 202 immediately
    thread = threading.Thread(
        target=process_accounts_background, 
        args=(job_id, account_numbers), 
        daemon=True
    )
    thread.start()
    
    # Return immediate synchronous response
    return jsonify({
        "success": True,
        "message": "Analysis request accepted and queued for processing"
    }), 202
