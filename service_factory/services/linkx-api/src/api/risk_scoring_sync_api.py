"""
Synchronous Risk Scoring API.

Provides a blocking POST endpoint that accepts a single account number,
offloads the graph analysis to a distributed worker node via the PostgreSQL
job queue, polls for completion, and returns the full result in the same
HTTP response.

Existing async and Kafka-based endpoints are untouched.
"""

import os
import time
import uuid
from flask import Blueprint, jsonify, request
from auth.decorators import current_actor_from_request
from service_orchestration import enqueue_worker_job, get_worker_job

risk_scoring_sync_api = Blueprint("risk_scoring_sync_api", __name__)

# Maximum time (seconds) to wait for the worker to finish before timing out
SYNC_TIMEOUT = int(os.getenv("LINKX_SYNC_SCORING_TIMEOUT", "120"))
POLL_INTERVAL = 1.0


@risk_scoring_sync_api.route('/sync_analysis', methods=['POST'])
def sync_analysis():
    """
    Synchronous risk scoring endpoint.

    Request:
        POST /api/risk_scoring/sync_analysis
        Headers:
            X-API-Key: <api_key>   OR   Authorization: Bearer <jwt>
            Content-Type: application/json
        Body:
            {"account_no": "1007900232134"}

    Response (success):
        200 OK
        {
            "success": true,
            "account_no": "1007900232134",
            "source": "link",
            "data": {
                "accountno": "1007900232134",
                "entity_id": "1007900232134",
                "linked_accounts_count": 12,
                "flagged_entity_links": 0,
                "beneficiary_blacklisted": false,
                "linked_entities": [],
                "network_centrality_score": 0.15,
                "max_path_length": 2
            },
            "processing": {"duration_ms": 12255.85}
        }

    Response (timeout):
        504 Gateway Timeout
        {"success": false, "message": "Analysis timed out after 120s"}

    Response (failed):
        500 Internal Server Error
        {"success": false, "message": "Analysis failed", "error": "..."}
    """

    # --- Authentication (same as async endpoint) ---
    api_key = request.headers.get("X-API-Key")
    valid_api_key = os.getenv("LINKX_RISK_SCORING_API_KEY")
    is_authenticated = False

    if api_key and valid_api_key and api_key == valid_api_key:
        is_authenticated = True
    else:
        actor = current_actor_from_request()
        if actor:
            is_authenticated = True

    if not is_authenticated:
        return jsonify({"success": False, "message": "Unauthorized - Invalid API Key or JWT Token"}), 401

    # --- Validate input ---
    data = request.get_json() or {}
    account_no = data.get("account_no")
    response_type = data.get("response_type") or "flagged"

    if not account_no:
        return jsonify({"success": False, "message": "Missing account_no"}), 400

    # --- Enqueue job to the worker ---
    try:
        job_result = enqueue_worker_job(
            queue_name="analysis",
            job_type="risk_scoring_sync",
            payload={"account_no": account_no, "response_type": response_type},
        )
        internal_job_id = job_result["job_id"]
    except Exception as e:
        print(f"[RiskScoringSync] Failed to enqueue sync job for {account_no}: {e}")
        return jsonify({"success": False, "message": "Failed to enqueue analysis job", "error": str(e)}), 500

    # --- Poll for completion ---
    deadline = time.time() + SYNC_TIMEOUT
    while time.time() < deadline:
        time.sleep(POLL_INTERVAL)
        try:
            job = get_worker_job(internal_job_id)
        except Exception:
            continue

        if not job:
            continue

        status = job.get("status")

        if status == "succeeded":
            result = job.get("result")
            if isinstance(result, dict) and result.get("status") == "failed":
                return jsonify({
                    "success": False,
                    "account_no": account_no,
                    "message": "Analysis failed",
                    "error": result.get("error"),
                    "processing": result.get("processing"),
                }), 500

            return jsonify({
                "success": True,
                "account_no": account_no,
                "source": result.get("source", "link") if isinstance(result, dict) else "link",
                "data": result.get("data") if isinstance(result, dict) else result,
                "processing": result.get("processing") if isinstance(result, dict) else {},
            }), 200

        if status == "failed":
            error_msg = job.get("error_message") or "Unknown error"
            return jsonify({
                "success": False,
                "account_no": account_no,
                "message": "Analysis failed",
                "error": error_msg,
            }), 500

    # --- Timeout ---
    return jsonify({
        "success": False,
        "account_no": account_no,
        "message": f"Analysis timed out after {SYNC_TIMEOUT}s. The worker may still be processing.",
    }), 504
