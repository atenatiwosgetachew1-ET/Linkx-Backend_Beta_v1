import os
from batch_manager.utils.artifact_utils import ensure_artifact_dir
import time
import threading
from flask_socketio import SocketIO

LOG_DIR = ensure_artifact_dir("logs")
os.makedirs(LOG_DIR, exist_ok=True)


def _log_stream_wait_seconds():
    try:
        return max(0.0, float(os.getenv("LINKX_LOG_STREAM_WAIT_SECONDS", "30")))
    except (TypeError, ValueError):
        return 30.0


def _log_stream_from_start():
    return str(os.getenv("LINKX_LOG_STREAM_FROM_START", "true")).lower() not in {"0", "false", "no"}


def log_writer(filename, message):
    """Append a log line to a log file."""
    file_path = os.path.join(LOG_DIR, filename)
    with open(file_path, 'a') as f:
        f.write(message + '\n')
    return True


def log_stream_background(socketio, session_id, sid, filename, stop_event):
    """Background log tailing thread for a client."""
    if not filename:
        socketio.emit(
            'stream_logs', 
            {'error': "No filename provided.", 'session_id': session_id, 'socket_id': sid}, 
            to=sid
        )
        return

    file_path = os.path.join(LOG_DIR, filename)
    deadline = time.time() + _log_stream_wait_seconds()
    while not os.path.exists(file_path) and not stop_event.is_set() and time.time() < deadline:
        socketio.sleep(0.1)

    if not os.path.exists(file_path):
        socketio.emit(
            'stream_logs', 
            {'error': f"File {file_path} not found.", 'session_id': session_id, 'socket_id': sid}, 
            to=sid
        )
        return

    try:
        with open(file_path, 'r') as log_file:
            if not _log_stream_from_start():
                log_file.seek(0, os.SEEK_END)
            while not stop_event.is_set():
                line = log_file.readline()
                if line:
                    socketio.emit(
                        'stream_logs', 
                        {'data': line, 'session_id': session_id, 'socket_id': sid}, 
                        to=sid
                    )
                else:
                    socketio.sleep(0.1)

    except Exception as e:
        socketio.emit(
            'stream_logs', 
            {'error': str(e), 'session_id': session_id, 'socket_id': sid}, 
            to=sid
        )
