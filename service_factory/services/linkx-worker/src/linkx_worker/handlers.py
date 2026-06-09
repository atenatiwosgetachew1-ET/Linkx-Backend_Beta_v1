import json
import traceback

from batch_manager.batch_data_manager import batch_data_manager
from batch_manager.analyzing.analyzer import analyzer


def _normalize_payload(payload):
    if payload is None:
        return {}
    if isinstance(payload, str):
        return json.loads(payload)
    return dict(payload)


def run_job(job_type, payload):
    payload = _normalize_payload(payload)
    job_type = str(job_type or payload.get("job_type") or payload.get("id") or "")

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

    if job_type in {"analyzer", "analysis", "run_analysis", "realtime_data"}:
        return analyzer(payload)

    raise ValueError(f"unsupported_job_type:{job_type}")


def run_job_safely(job_type, payload):
    try:
        result = run_job(job_type, payload)
        return True, result, None
    except Exception as exc:
        return False, None, {
            "error": str(exc),
            "traceback": traceback.format_exc(),
        }
