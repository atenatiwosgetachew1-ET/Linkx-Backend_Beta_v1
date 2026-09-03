import os
import json
from datetime import datetime, timedelta
from flask import Blueprint, jsonify, request
from service_orchestration import connect, enqueue_worker_job

unified_risk_scoring_api = Blueprint("unified_risk_scoring_api", __name__)

def _get_time_window(payload_time_window):
    """Phase 1.1: Calculate 1-Year Fallback Logic"""
    if isinstance(payload_time_window, dict):
        start = payload_time_window.get("start_date")
        end = payload_time_window.get("end_date")
        if start and end:
            return {"start_date": str(start), "end_date": str(end)}
    
    # Fallback to exactly [today - 1 year, today]
    now = datetime.utcnow()
    one_year_ago = now - timedelta(days=365)
    return {
        "start_date": one_year_ago.strftime("%Y-%m-%d"),
        "end_date": now.strftime("%Y-%m-%d")
    }

def _check_unified_cache(account_no, time_window):
    """Phase 1.2: Upgrade the Cache Query (Time Range Locking)"""
    try:
        with connect() as conn:
            with conn.cursor() as cur:
                # We check request_payload JSONB for exact matching dates
                cur.execute("""
                SELECT response_payload
                FROM link_analysis_evidence
                WHERE entity_id = %s 
                  AND event_type = 'unified_link_analysis'
                  AND request_payload->'time_window'->>'start_date' = %s
                  AND request_payload->'time_window'->>'end_date' = %s
                  AND analyzed_at >= NOW() - INTERVAL '2 hours'
                ORDER BY analyzed_at DESC
                LIMIT 1
                """, (
                    str(account_no),
                    time_window["start_date"],
                    time_window["end_date"]
                ))
                row = cur.fetchone()
                if row and row[0]:
                    return row[0]
    except Exception as e:
        print(f"[UnifiedRiskAPI] Cache read error for {account_no}: {e}")
    return None

@unified_risk_scoring_api.route('/analyze', methods=['POST'])
def analyze():
    """
    Standard Unified Risk Scoring Endpoint (v2.0)
    Phase 2: The Unified API Endpoint
    """
    # --- Authentication (LINK_ANALYSIS_API_KEY) ---
    api_key = request.headers.get("X-API-Key")
    auth_header = request.headers.get("Authorization")
    
    if auth_header and auth_header.startswith("Bearer "):
        api_key = auth_header.split(" ")[1]
        
    valid_key = os.getenv("LINK_ANALYSIS_API_KEY")
    # Fallback to Risk Scoring key temporarily to prevent instant 401s during testing
    if not valid_key:
        valid_key = os.getenv("LINKX_RISK_SCORING_API_KEY")

    if not api_key or not valid_key or api_key != valid_key:
        return jsonify({"error": {"code": "UNAUTHORIZED", "message": "Invalid or missing API key"}}), 401
        
    # --- Payload Parsing & Validation (Phase 2.2) ---
    data = request.get_json() or {}
    job_id = data.get("job_id")
    account_numbers = data.get("account_numbers") or []
    
    if not job_id:
        return jsonify({"error": {"code": "INVALID_REQUEST", "message": "Field 'job_id' is required"}}), 400
    if not account_numbers or not isinstance(account_numbers, list):
        return jsonify({"error": {"code": "INVALID_REQUEST", "message": "Field 'account_numbers' is required and must be a list"}}), 400
        
    time_window = _get_time_window(data.get("time_window"))
    
    # --- Hybrid Cache Routing (Phase 2.3) ---
    cached_accounts = []
    uncached_accounts = []
    cached_payloads = {}
    
    for acc in account_numbers:
        cached_result = _check_unified_cache(acc, time_window)
        if cached_result:
            cached_accounts.append(acc)
            cached_payloads[acc] = cached_result
        else:
            uncached_accounts.append(acc)
            
    # --- Single Fast Account Sync Mode (Phase 2.4) ---
    if len(account_numbers) == 1 and len(cached_accounts) == 1:
        acc = cached_accounts[0]
        response_json = cached_payloads[acc]
        response_json["job_id"] = job_id  # Inject the requested job_id
        return jsonify(response_json), 200
        
    # --- Batch / Slow Accounts Async Mode (Phase 2.4) ---
    
    # 1. Enqueue uncached accounts to heavy PostgreSQL worker queue
    for acc in uncached_accounts:
        try:
            enqueue_worker_job(
                queue_name="analysis",
                job_type="unified_link_analysis",
                payload={
                    "job_id": job_id,
                    "account_no": acc,
                    "time_window": time_window
                }
            )
        except Exception as e:
            print(f"[UnifiedRiskAPI] Failed to enqueue uncached job for {acc}: {e}")
            
    # 2. Enqueue lightweight "webhook only" jobs for cached accounts
    for acc in cached_accounts:
        try:
            cached_payloads[acc]["job_id"] = job_id
            enqueue_worker_job(
                queue_name="analysis",
                job_type="unified_link_analysis_webhook_only",
                payload={
                    "job_id": job_id,
                    "account_no": acc,
                    "cached_payload": cached_payloads[acc]
                }
            )
        except Exception as e:
            print(f"[UnifiedRiskAPI] Failed to enqueue cached webhook for {acc}: {e}")
            
    # 3. Return 202 Accepted immediately
    return jsonify({
        "status": "ACCEPTED",
        "job_id": job_id,
        "message": "Analysis started asynchronously"
    }), 202
