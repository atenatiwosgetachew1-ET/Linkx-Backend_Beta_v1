import os
from flask import Blueprint, jsonify, request
from auth.decorators import current_actor_from_request
from service_orchestration import enqueue_worker_job

risk_scoring_api = Blueprint("risk_scoring_api", __name__)

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
        
    # Offload processing to distributed worker nodes (Server 3)
    # Each account gets its own worker job for massive parallelism
    for account_no in account_numbers:
        payload = {
            "job_id": job_id,
            "account_no": account_no
        }
        try:
            # Pushing to the default queue; the worker node will pick these up automatically
            enqueue_worker_job(queue_name="default", job_type="risk_scoring_webhook", payload=payload)
        except Exception as e:
            print(f"[RiskScoring] Failed to enqueue job for {account_no}: {e}")
    
    # Return immediate synchronous response
    return jsonify({
        "success": True,
        "message": f"Analysis request accepted. {len(account_numbers)} accounts queued for distributed processing."
    }), 202
