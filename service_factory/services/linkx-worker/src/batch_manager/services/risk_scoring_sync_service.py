"""
Synchronous Risk Scoring Worker Handler.

This handler is invoked by the worker when a job of type 'risk_scoring_sync'
is claimed from the queue. It runs the full LinkX graph analysis pipeline
and returns the result directly — no webhook callback is fired.

The API server polls the PostgreSQL jobs table for the result and returns it
to the caller in the same HTTP response.
"""

import time
import json
from batch_manager.utils.postgres_utils import get_postgres_connection


def process_sync_job(payload):
    """
    Worker task for synchronous risk scoring.
    Expects payload: {"account_no": "..."}
    Returns the analysis findings dict directly (no HTTP callback).
    """
    account_no = payload.get("account_no")

    if not account_no:
        return {"status": "failed", "error": "Missing account_no"}

    start_time = time.time()

    # --- Run the full graph analysis pipeline ---
    analysis_payload = {
        "meta": {
            "trace_id": f"sync-{account_no}",
            "correlation_id": f"sync-{account_no}",
        },
        "data": {
            "entity_id": account_no,
            "analysis_type": "transaction_graph",
        },
    }

    try:
        from batch_manager.services.risk_scoring_kafka_service import execute_formal_link_analysis

        response_event, status = execute_formal_link_analysis(analysis_payload)
        duration_ms = round((time.time() - start_time) * 1000, 2)

        if response_event and response_event.get("data"):
            findings = response_event["data"]
            return {
                "status": "success",
                "account_no": account_no,
                "source": "link",
                "data": findings,
                "processing": {"duration_ms": duration_ms},
            }
        else:
            return {
                "status": "failed",
                "account_no": account_no,
                "error": str(status),
                "processing": {"duration_ms": duration_ms},
            }
    except Exception as e:
        duration_ms = round((time.time() - start_time) * 1000, 2)
        print(f"[RiskScoringSync] Analysis failed for {account_no}: {e}")
        return {
            "status": "failed",
            "account_no": account_no,
            "error": str(e),
            "processing": {"duration_ms": duration_ms},
        }
