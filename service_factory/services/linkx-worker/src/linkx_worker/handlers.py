import json
import traceback

from batch_manager.batch_data_manager import batch_data_manager
from batch_manager.analyzing.analyzer import analyzer
from batch_manager.services.dataframe_workflow import create_dataframe_result
from batch_manager.services.graph_workflow import fetch_graph_result
from batch_manager.services.str_link_analysis_workflow import run_str_link_analysis
from linkx_worker.cancellation import DatabaseCancellationEvent


def _normalize_payload(payload):
    if payload is None:
        return {}
    if isinstance(payload, str):
        return json.loads(payload)
    return dict(payload)


def run_job(job_type, payload):
    payload = _normalize_payload(payload)
    job_type = str(job_type or payload.get("job_type") or payload.get("id") or "")
    session_id = payload.get("session_id")
    job_id = payload.get("job_id")

    if session_id and "stop_event" not in payload:
        payload["stop_event"] = DatabaseCancellationEvent(
            session_id=session_id,
            job_id=job_id,
        )

    if job_type in {
        "batch_data_manager",
        "batch_data",
        "load_sourceData",
        "merge",
        "search",
        "create_session",
        "start_session",
        "end_session",
    }:
        if "id" not in payload and job_type != "batch_data_manager":
            payload["id"] = job_type
        return batch_data_manager(payload)

    if job_type in {"create_DF", "create_dataframe", "dataframe"}:
        result, status = create_dataframe_result(payload, session_id)
        if status >= 400:
            result = dict(result)
            result.setdefault("status", "failed")
        return result

    if job_type in {"str_link_analysis", "STR_link_analysis"}:
        return run_str_link_analysis(payload)

    if job_type in {"graph_fetch", "get_graph"}:
        return fetch_graph_result(payload)

    if job_type in {"analyzer", "analysis", "run_analysis", "realtime_data"}:
        return analyzer(payload)

    raise ValueError(f"unsupported_job_type:{job_type}")


def run_job_safely(job_type, payload):
    try:
        result = run_job(job_type, payload)
        if result is False:
            return False, result, {"error": "job_failed", "result": result}
        if isinstance(result, dict) and str(result.get("status") or "").lower() in {"failed", "error"}:
            return False, result, {"error": result.get("error") or result.get("message") or "job_failed", "result": result}
        return True, result, None
    except Exception as exc:
        return False, None, {
            "error": str(exc),
            "traceback": traceback.format_exc(),
        }
