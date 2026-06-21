import uuid
import threading
import subprocess
import sys
from datetime import datetime
import os
from batch_manager.analyzing.analyzer import analyzer
from logger import log_writer
from globals import _session_store
from batch_manager.utils.artifact_utils import ensure_artifact_dir, register_artifact

try:
    from service_orchestration import enqueue_cleanup_run, request_session_cancellation
except Exception:
    enqueue_cleanup_run = None
    request_session_cancellation = None

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(BASE_DIR, "..", ".."))


def schedule_session_cleanup_process(session_id, tool_credentials, log_file=None, run_id=None, wait_thread=None):
    if not session_id or not tool_credentials:
        if log_file:
            log_writer(log_file, f"[{datetime.now()}] [Warning] - Cleanup skipped: missing session or Neo4j credentials")
        return None

    required_keys = ("url", "username", "password")
    if any(not tool_credentials.get(key) for key in required_keys):
        if log_file:
            log_writer(log_file, f"[{datetime.now()}] [Warning] - Cleanup skipped: incomplete Neo4j credentials")
        return None

    def launch_cleanup_process():
        if wait_thread and wait_thread.is_alive():
            if log_file:
                log_writer(log_file, f"[{datetime.now()}] [Info] - Cleanup launcher waiting for analyzer thread to stop")
            wait_thread.join()

        cmd = [
            sys.executable,
            "-m",
            "batch_manager.jobs.cleanup_neo4j_session",
            "--session-id",
            str(session_id),
            "--batch-size",
            "10000",
        ]
        if run_id:
            cmd.extend(["--run-id", str(run_id)])
        if log_file:
            cmd.extend(["--log-file", log_file])

        env = os.environ.copy()
        env.update({
            "LINKX_CLEANUP_NEO4J_URL": str(tool_credentials["url"]),
            "LINKX_CLEANUP_NEO4J_USERNAME": str(tool_credentials["username"]),
            "LINKX_CLEANUP_NEO4J_PASSWORD": str(tool_credentials["password"]),
        })

        process = subprocess.Popen(
            cmd,
            cwd=PROJECT_ROOT,
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        if log_file:
            log_writer(log_file, f"[{datetime.now()}] [Info] - Cleanup process started with pid {process.pid}")
        return process

    if wait_thread and wait_thread.is_alive():
        launcher = threading.Thread(target=launch_cleanup_process, daemon=True)
        launcher.start()
        return launcher

    return launch_cleanup_process()


def create_session(payload):
    """
    Create a new session, store minimal metadata, and return session_id
    """
    print("session_created")
    session_id = payload["session_id"];
    if session_id in _session_store:
        return True  # Already exists -> treat as OK
    _session_store[session_id] = {
        "start_time": datetime.now().isoformat(),
        "thread": None,
        "stop_event": None
    }
    return True

def start_session(payload):
    session_id = payload["session_id"]
    current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    # Ensure session exists
    if session_id not in _session_store:
        _session_store[session_id] = {
            "start_time": datetime.now().isoformat(),
            "thread": None,
            "stop_event": None
        }

    # Log folder
    log_dir = ensure_artifact_dir("logs")
    os.makedirs(log_dir, exist_ok=True)

    log_file = payload.get("log_file") or f"logfile_{session_id}_[{current_time}].log"
    full_path = os.path.join(log_dir, log_file)
    if not os.path.exists(full_path):
        with open(full_path, "w") as f:
            f.write(f"New session started at {current_time}\n")
    register_artifact(full_path, "log", session_id=session_id, filename=log_file)

    run_id = payload.get("run_id") or uuid.uuid4().hex
    payload["log_file"] = log_file
    payload["run_id"] = run_id
    stop_event = payload.get("stop_event") or threading.Event()
    payload["stop_event"] = stop_event

    if payload.get("run_inline") is True:
        _session_store[session_id].update({
            "thread": None,
            "stop_event": stop_event,
            "log_file": log_file,
            "run_id": run_id,
            "tool_credentials": payload.get("tool_credentials"),
        })
        try:
            result = analyzer(payload)
            if result is True:
                return {"status": "success", "message": "analysis completed", "log_file": log_file, "run_id": run_id, "result": result}
            return {"status": "failed", "message": "analysis failed or was cancelled", "log_file": log_file, "run_id": run_id, "result": result}
        except Exception as e:
            print(f"[ERROR] Inline worker session failed: {e}")
            return {"status": "failed", "log_file": log_file, "run_id": run_id, "error": str(e)}

    try:
        # Start thread normally
        thread = threading.Thread(target=analyzer, args=(payload,), daemon=True)
        thread.start()
        # Update session store
        _session_store[session_id].update({
            "thread": thread,
            "stop_event": stop_event,
            "log_file": log_file,
            "run_id": run_id,
            "tool_credentials": payload.get("tool_credentials"),
        })
        return log_file

    except Exception as e:
        print(f"[ERROR] Failed to start session thread: {e}")
        return {"status": "failed", "error": str(e)}

def end_session(payload):
    session_id = payload["session_id"]
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # --- Get session object ---
    session = _session_store.get(session_id)

    cancellation_result = None
    stop_reason = payload.get("reason") or "client_end_session"
    stop_only_reasons = {"client_end_session", "socket_disconnect_abandoned", "idle_lock"}
    should_preserve_session_config = stop_reason in stop_only_reasons
    if request_session_cancellation and not should_preserve_session_config:
        try:
            cancellation_result = request_session_cancellation(
                session_id,
                reason=stop_reason,
                requested_by=payload.get("requested_by"),
                neo4j_credentials=session.get("tool_credentials") if session else None,
            )
        except Exception as exc:
            print(f"[WARN] Failed to request DB cancellation for session {session_id}: {exc}")
    if not session:
        print("3:","Session not found")
        if cancellation_result and cancellation_result.get("cancel_requested"):
            return {
                "status": "success",
                "message": "cancellation requested",
                "cleanup": "scheduled",
                "orchestration": cancellation_result,
            }
        return {"status": "failed", "message": "Session not found"}

    # --- Stop log file ---
    log_file = session.get("log_file")
    if log_file:
        PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
        log_dir = ensure_artifact_dir("logs")
        full_path = os.path.join(log_dir, log_file)

        if os.path.exists(full_path):
            with open(full_path, "a") as f:
                f.write(f"\n[ Session Ended at {current_time} ]\n")

    # --- Stop background thread ---
    thread = session.get("thread")
    stop_event = session.get("stop_event")

    cleanup_job = None
    if thread and stop_event:
        stop_event.set()
        thread.join(timeout=3)
        if should_preserve_session_config and enqueue_cleanup_run and session.get("run_id"):
            try:
                cleanup_id = enqueue_cleanup_run(
                    "run",
                    session_id=session_id,
                    run_id=session.get("run_id"),
                    reason=stop_reason,
                    neo4j_credentials=session.get("tool_credentials"),
                    payload={"event": "stop_ingestion", "preserve_session_config": True},
                )
                cleanup_job = {"cleanup_id": cleanup_id}
            except Exception as exc:
                print(f"[WARN] Failed to queue run cleanup for session {session_id}: {exc}")
        elif not (cancellation_result and cancellation_result.get("cleanup_id")):
            cleanup_job = schedule_session_cleanup_process(
                session_id,
                session.get("tool_credentials"),
                log_file=log_file,
                run_id=session.get("run_id"),
                wait_thread=thread if thread.is_alive() else None,
            )
    else:
        print("3:","Thread or stop_event missing")
        return {"status": "failed", "message": "Thread or stop_event missing"}

    # --- Clean session store ---
    del _session_store[session_id]
    print("3:","success")
    if thread.is_alive():
        return {"status": "success", "message": "stopping", "cleanup": "scheduled" if cleanup_job else "skipped"}
    return {"status": "success", "message":"success", "cleanup": "scheduled" if cleanup_job else "skipped"}


def save_temp_config(key, value, sid):
    if sid in _session_store:
        _session_store[sid][key] = value


def load_temp_config(key, sid):
    return _session_store.get(sid, {}).get(key)
