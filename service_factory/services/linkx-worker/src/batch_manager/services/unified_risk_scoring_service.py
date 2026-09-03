import json
import time
import requests
import os
from batch_manager.utils.postgres_utils import get_postgres_connection

def _send_webhook(account_no, job_id, payload):
    """Phase 4: Webhook Delivery"""
    callback_url = "https://risk-platform.local/api/v1/aggregate/callback"
    api_key = os.getenv("AGGREGATOR_API_KEY")
    
    headers = {
        "Host": "risk-platform.local",
        "Content-Type": "application/json",
        "X-API-Key": api_key,
        "X-Correlation-ID": str(job_id)
    }
    
    try:
        response = requests.post(callback_url, json=payload, headers=headers, verify=False, timeout=10)
        print(f"[UnifiedRiskScoring] Webhook delivered for {account_no}. Status: {response.status_code}")
    except Exception as e:
        print(f"[UnifiedRiskScoring] Webhook failed for {account_no}: {e}")

def process_unified_job(payload):
    """
    Worker handler for 'unified_link_analysis'.
    Executes formal link analysis, maps to v2 schema, caches it, and sends the webhook.
    """
    job_id = payload.get("job_id")
    account_no = payload.get("account_no")
    time_window = payload.get("time_window")
    
    start_time = time.time()
    
    analysis_payload = {
        "meta": {
            "trace_id": f"unified-{account_no}-{int(start_time)}",
            "aggregation_key": {
                "type": "accountno",
                "value": account_no
            }
        },
        "data": {
            "entity_id": account_no,
            "analysis_type": "transaction_graph",
            "time_window": time_window  # Handled inside execute_formal_link_analysis
        }
    }
    
    try:
        from batch_manager.services.risk_scoring_kafka_service import execute_formal_link_analysis
        response_event, status = execute_formal_link_analysis(analysis_payload)
        
        # --- Map to v2.0 Schema (Phase 3.3) ---
        duration_ms = round((time.time() - start_time) * 1000, 2)
        
        v2_payload = {
            "job_id": job_id,
            "schema_version": "2.0",
            "success": True,
            "source": "link_analysis",
            "account_no": account_no,
            "time_window": time_window,
            "data": {},
            "processing_duration_ms": duration_ms,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "error": None
        }
        
        if not response_event or status != "success" or not response_event.get("data"):
            v2_payload["success"] = False
            v2_payload["error"] = {
                "code": "ANALYSIS_FAILED",
                "message": str(status)
            }
        else:
            findings = response_event["data"]
            # Rename legacy fields or pass graph entities
            v2_payload["data"] = {
                "network_centrality_score": findings.get("network_centrality_score", 0),
                "max_path_length": findings.get("max_path_length", 0),
                "linked_accounts_count": findings.get("linked_count", 0),
                "risk_score": 0 # Safe default
            }
            # Calculate a basic risk score if there are flagged relationships
            if findings.get("flagged_count", 0) > 0:
                v2_payload["data"]["risk_score"] = min(100, 50 + (findings["flagged_count"] * 10))
                
        # --- Save to Cache (Phase 1.3) ---
        try:
            with get_postgres_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                    INSERT INTO link_analysis_evidence (
                        trace_id, correlation_id, entity_id, entity_type,
                        session_id, event_type, is_flagged, linked_accounts_count,
                        request_payload, response_payload, duration_ms, analyzed_at
                    ) VALUES (
                        %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s::jsonb, %s::jsonb, %s, NOW()
                    )
                    """, (
                        analysis_payload["meta"]["trace_id"], job_id, account_no, "accountno",
                        response_event.get("meta", {}).get("session_id", ""), "unified_link_analysis",
                        bool(v2_payload["data"].get("risk_score", 0) > 0), v2_payload["data"].get("linked_accounts_count", 0),
                        json.dumps({"time_window": time_window}), json.dumps(v2_payload), duration_ms
                    ))
                conn.commit()
        except Exception as e:
            print(f"[UnifiedRiskScoring] Cache write error: {e}")
            
        # --- Send Webhook (Phase 4.3) ---
        _send_webhook(account_no, job_id, v2_payload)
        
        return {"status": "success", "account_no": account_no}
        
    except Exception as e:
        print(f"[UnifiedRiskScoring] Fatal Error: {e}")
        # Send failure webhook
        v2_payload = {
            "job_id": job_id,
            "schema_version": "2.0",
            "success": False,
            "source": "link_analysis",
            "account_no": account_no,
            "time_window": time_window,
            "data": None,
            "processing_duration_ms": round((time.time() - start_time) * 1000, 2),
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "error": {"code": "INTERNAL_ERROR", "message": str(e)}
        }
        _send_webhook(account_no, job_id, v2_payload)
        return {"status": "failed", "error": str(e)}

def process_unified_webhook_only_job(payload):
    """
    Worker handler for 'unified_link_analysis_webhook_only'.
    Used by the Fast-Lane cache bypass.
    """
    job_id = payload.get("job_id")
    account_no = payload.get("account_no")
    cached_payload = payload.get("cached_payload")
    
    # Ensure job_id is updated in the cached payload
    cached_payload["job_id"] = job_id
    cached_payload["timestamp"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    
    _send_webhook(account_no, job_id, cached_payload)
    return {"status": "success", "account_no": account_no}
