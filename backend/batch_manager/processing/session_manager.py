import uuid
import threading
from datetime import datetime
import os
from pyspark.sql import SparkSession
from batch_manager.analyzing.analyzer import analyzer
from logger import log_writer
from globals import _session_store

def create_session(payload):
    """
    Create a new session, store minimal metadata, and return session_id
    """
    print("session_created")
    session_id = payload["session_id"];
    if session_id in _session_store:
        return True  # Already exists → treat as OK
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
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    PROJECT_ROOT = os.path.abspath(os.path.join(BASE_DIR, "..", ".."))
    log_dir = os.path.join(PROJECT_ROOT, "public", "temp_logs")
    os.makedirs(log_dir, exist_ok=True)

    log_file = f"logfile_{session_id}_[{current_time}].log"
    full_path = os.path.join(log_dir, log_file)
    with open(full_path, "w") as f:
        f.write(f"New session started at {current_time}\n")

    payload["log_file"] = log_file
    stop_event = threading.Event()
    payload["stop_event"] = stop_event
    try:
        # Start thread normally
        thread = threading.Thread(target=analyzer, args=(payload,), daemon=True)
        thread.start()
        # Update session store
        _session_store[session_id].update({
            "thread": thread,
            "stop_event": stop_event,
            "log_file": log_file
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
    if not session:
        print("3:","Session not found")
        return {"status": "failed", "message": "Session not found"}

    # --- Stop log file ---
    log_file = session.get("log_file")
    if log_file:
        PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        log_dir = os.path.join(PROJECT_ROOT, "public", "temp_logs")
        full_path = os.path.join(log_dir, log_file)

        if os.path.exists(full_path):
            with open(full_path, "a") as f:
                f.write(f"\n[ Session Ended at {current_time} ]\n")

    # --- Stop background thread ---
    thread = session.get("thread")
    stop_event = session.get("stop_event")

    if thread and stop_event:
        stop_event.set()
        thread.join()
    else:
        print("3:","Thread or stop_event missing")
        return {"status": "failed", "message": "Thread or stop_event missing"}

    # --- Clean session store ---
    del _session_store[session_id]
    print("3:","success")
    return {"status": "success", "message":"success"}


def save_temp_config(key, value, sid):
    if sid in _session_store:
        _session_store[sid][key] = value


def load_temp_config(key, sid):
    return _session_store.get(sid, {}).get(key)
