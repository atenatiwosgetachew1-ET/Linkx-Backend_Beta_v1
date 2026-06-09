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


def queue_str_report_payload(payload, frontend_session_id=None):
    session_key = str(frontend_session_id) if frontend_session_id else "__broadcast__"
    globals.str_report_pending_registry.setdefault(session_key, []).append(payload)
    print(
        f"[str_report_socket] queued str_report_link_analysis "
        f"(session_key={session_key}, queue_depth={len(globals.str_report_pending_registry[session_key])})"
    )


def flush_str_report_pending(session_id, sid):
    if _socketio is None or not session_id:
        return 0

    session_id = str(session_id)
    keys = []
    if session_id in globals.str_report_pending_registry:
        keys.append(session_id)
    if "__broadcast__" in globals.str_report_pending_registry:
        keys.append("__broadcast__")

    emitted = 0
    for key in keys:
        pending = globals.str_report_pending_registry.pop(key, [])
        for payload in pending:
            _socketio.emit("str_report_link_analysis", payload, to=sid)
            emitted += 1
            print(
                f"[str_report_socket] flushed queued str_report_link_analysis "
                f"to sid={sid} (session_key={key}, request_id={payload.get('request_id')})"
            )
            notification = _build_notification(
                session_id,
                "info",
                "str_report_prepare_receiver",
                payload.get("message", "Prepare to receive STR report session results."),
                source="str_report_link_analysis",
                details=payload.get("details") or payload,
            )
            _socketio.emit("notification", notification, to=sid)
            emitted += 1
    return emitted


def subscribe_str_report_session(session_id, sid):
    if not session_id:
        return

    session_id = str(session_id)
    entry = globals.sockets_registry.setdefault(sid, {})
    entry.setdefault("notification_sessions", set()).add(session_id)
    emit_pending_notifications(session_id, sid)
    flush_str_report_pending(session_id, sid)
    flush_status_pending(session_id, sid)


def queue_status_payload(payload, frontend_session_id=None):
    session_id = str(frontend_session_id or payload.get("session_id") or "")
    status_type = str(payload.get("type") or "")
    if not session_id or not status_type:
        return

    globals.str_report_status_registry.setdefault(session_id, {})[status_type] = payload
    print(
        f"[str_report_socket] queued status payload "
        f"(session_id={session_id}, type={status_type})"
    )


def flush_status_pending(session_id, sid):
    if _socketio is None or not session_id:
        return 0

    session_id = str(session_id)
    pending = globals.str_report_status_registry.get(session_id, {})
    emitted = 0
    for status_type in ("metadata", "relationships"):
        payload = pending.get(status_type)
        if payload:
            _socketio.emit("status", payload, to=sid)
            emitted += 1
            print(
                f"[str_report_socket] flushed status payload "
                f"to sid={sid} (session_id={session_id}, type={status_type})"
            )
    return emitted


def emit_status_payload(payload, frontend_session_id=None):
    """Push one status payload to subscribed graph/STR clients, and keep it replayable."""
    session_id = str(frontend_session_id or payload.get("session_id") or "")
    queue_status_payload(payload, session_id)

    if _socketio is None:
        return 0

    emitted = 0
    for socket_id, entry in list(globals.sockets_registry.items()):
        subscribed = entry.get("notification_sessions", set())
        if not session_id or session_id in subscribed:
            _socketio.emit("status", payload, to=socket_id)
            emitted += 1
    return emitted


def emit_str_report_link_analysis(payload, frontend_session_id=None):
    """Push STR link-analysis session details to connected dashboard clients."""
    analysis_session_id = payload.get("session_id")
    request_id = payload.get("request_id")
    timestamp = datetime.utcnow().isoformat() + "Z"

    if _socketio is None:
        print(
            f"[str_report_socket] {timestamp} emit skipped: socketio not initialized "
            f"(request_id={request_id}, analysis_session_id={analysis_session_id})"
        )
        return 0

    connected_sids = list(globals.sockets_registry.keys())
    target_sids = []
    delivery_mode = "broadcast"

    if frontend_session_id:
        frontend_session_id = str(frontend_session_id)
        delivery_mode = "targeted"
        for socket_id, entry in list(globals.sockets_registry.items()):
            subscribed = entry.get("notification_sessions", set())
            if frontend_session_id in subscribed:
                target_sids.append(socket_id)
    else:
        target_sids = connected_sids

    print(
        f"[str_report_socket] {timestamp} initiating emit "
        f"(request_id={request_id}, analysis_session_id={analysis_session_id}, "
        f"frontend_session_id={frontend_session_id or 'broadcast'}, mode={delivery_mode}, "
        f"connected_sockets={len(connected_sids)}, target_sockets={len(target_sids)}, "
        f"target_sids={target_sids})"
    )

    emitted = 0
    for socket_id in target_sids:
        _socketio.emit("str_report_link_analysis", payload, to=socket_id)
        emitted += 1
        print(
            f"[str_report_socket] {timestamp} emitted str_report_link_analysis "
            f"to sid={socket_id} (request_id={request_id})"
        )

    delivered_to_expected_session = False
    if frontend_session_id and target_sids:
        for socket_id in target_sids:
            subscribed = globals.sockets_registry.get(socket_id, {}).get("notification_sessions", set())
            if frontend_session_id in subscribed:
                delivered_to_expected_session = True
                break

    if frontend_session_id:
        if delivered_to_expected_session:
            globals.str_report_pending_registry.pop(str(frontend_session_id), None)
            add_notification(
                frontend_session_id,
                "info",
                "str_report_prepare_receiver",
                payload.get("message", "Prepare to receive STR report session results."),
                source="str_report_link_analysis",
                details=payload.get("details") or payload,
            )
        else:
            queue_str_report_payload(payload, frontend_session_id)
    else:
        queue_str_report_payload(payload, None)

    if emitted == 0:
        print(
            f"[str_report_socket] {timestamp} no live socket received str_report_link_analysis "
            f"(request_id={request_id}, frontend_session_id={frontend_session_id or 'broadcast'}; "
            f"payload queued for subscribe/reconnect)"
        )
    elif not delivered_to_expected_session and frontend_session_id:
        print(
            f"[str_report_socket] {timestamp} emit reached {emitted} socket(s) but not session "
            f"{frontend_session_id}; payload queued for subscribe"
        )
    else:
        print(
            f"[str_report_socket] {timestamp} emit complete "
            f"(request_id={request_id}, delivered_to={emitted} socket(s))"
        )

    return emitted
