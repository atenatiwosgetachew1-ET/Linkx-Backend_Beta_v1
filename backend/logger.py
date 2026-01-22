import os
import time
import threading
from flask_socketio import SocketIO

LOG_DIR = 'public/temp_logs/'
#os.makedirs(LOG_DIR, exist_ok=True)


def log_writer(filename, message):
    """Append a log line to a log file."""
    file_path = os.path.join(LOG_DIR, filename)
    with open(file_path, 'a') as f:
        f.write(message + '\n')
    return True


def log_stream_background(socketio, session_id, sid, filename, stop_event):
    """Background log tailing thread for a client."""
    file_path = os.path.join(LOG_DIR, filename)
    if not os.path.exists(file_path):
        socketio.emit('stream_logs', {'error': f"File {file_path} not found.",'session_id': session_id, 'socket_id':sid}, to=sid)
        return

    try:
        with open(file_path, 'r') as log_file:
            log_file.seek(0, os.SEEK_END)
            while not stop_event.is_set():
                line = log_file.readline()
                if line:
                    socketio.emit('stream_logs', {'data': line,'session_id': session_id, 'socket_id':sid}, to=sid)
                else:
                    socketio.sleep(0.1)

    except Exception as e:
        socketio.emit('stream_logs', {'error': str(e),'session_id': session_id, 'socket_id':sid}, to=sid)
