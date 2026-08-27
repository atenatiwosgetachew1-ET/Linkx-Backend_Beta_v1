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


def process_accounts_background(job_id, account_numbers):
    """Runs in the background, analyzes accounts, and fires webhooks."""
    for account_no in account_numbers:
        start_time = time.time()
        
        # 1. Format the request for our internal graph analyzer
        analysis_payload = {
            "entity": "bank",
            "search_term": account_no,
            "match_exact": True
        }
        
        # 2. Run the actual LinkX graph analysis!
        try:
            findings = analyzer(analysis_payload)
        except Exception as e:
            findings = {"error": str(e), "status": "failed"}
            
        duration_ms = (time.time() - start_time) * 1000
        
        # 3. Build the exact callback payload from their documentation
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
        
        # 4. Fire the callback back to their aggregator
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
