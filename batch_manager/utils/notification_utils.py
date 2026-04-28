from datetime import datetime

import globals


_socketio = None


def set_notification_socketio(socketio):
    global _socketio
    _socketio = socketio


def _build_notification(session_id, level, code, message, source=None, details=None):
    return {
        "session_id": str(session_id),
        "level": level,
        "code": code,
        "message": message,
        "source": source,
        "details": details or {},
        "created_at": datetime.utcnow().isoformat() + "Z",
    }


def add_notification(session_id, level, code, message, source=None, details=None):
    if not session_id:
        return None

    notification = _build_notification(session_id, level, code, message, source, details)
    emitted = emit_notification(notification)
    if emitted == 0:
        key = str(session_id)
        globals.notifications_registry.setdefault(key, []).append(notification)
    return notification


def pop_notifications(session_id):
    return globals.notifications_registry.pop(str(session_id), [])


def emit_notification(notification, sid=None):
    if _socketio is None:
        return 0

    if sid:
        _socketio.emit("notification", notification, to=sid)
        return 1

    emitted = 0
    session_id = str(notification.get("session_id"))
    for socket_id, entry in list(globals.sockets_registry.items()):
        sessions = entry.get("notification_sessions", set())
        if session_id in sessions:
            _socketio.emit("notification", notification, to=socket_id)
            emitted += 1
    return emitted


def emit_pending_notifications(session_id, sid):
    for notification in pop_notifications(session_id):
        emit_notification(notification, sid=sid)
