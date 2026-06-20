from flask import request
from flask_socketio import SocketIO

import os
import threading
import globals

from logger import log_stream_background 
from batch_manager.utils.database_utils import graph_status_stream
from batch_manager.utils.notification_utils import (
    add_notification,
    set_notification_socketio,
    subscribe_str_report_session,
    flush_status_pending,
)
from globals import load_temp_config,get_or_create_socket_entry,sockets_registry,_session_store
from connection_utils import tools
from batch_manager.processing.session_manager import end_session
from auth.repository import get_service_account_by_id, get_user_by_id, public_actor
from auth.tokens import verify_access_token

# check if socket is alive
def is_socket_alive(sid, socketio_instance=None):
    try:
        if socketio_instance is None:
            return False
        return socketio_instance.server.manager.is_connected(sid, '/')
    except Exception:
        return False


def register_socket_handlers(socketio: SocketIO):
    """
    Register all Socket.IO event handlers here.
    """
    set_notification_socketio(socketio)
    print("[str_report_socket] socket handlers registered (socketio ready)")

    disconnect_grace_seconds = int(os.getenv("LINKX_SOCKET_DISCONNECT_GRACE_SECONDS", "45"))

    def _track_socket_session(sid, session_id):
        if not session_id:
            return
        entry = get_or_create_socket_entry(sid)
        entry.setdefault("analysis_sessions", set()).add(str(session_id))

    def _socket_has_session(entry, session_id):
        session_key = str(session_id)
        if session_key in entry.get("analysis_sessions", set()):
            return True
        if session_key in entry.get("notification_sessions", set()):
            return True
        if session_key in entry.get("graph_statuses", {}):
            return True
        log_stream = entry.get("log_stream")
        return isinstance(log_stream, dict) and str(log_stream.get("session_id")) == session_key


    def _close_graph_entry(graph_entry):
        if not isinstance(graph_entry, dict):
            return
        stop_event = graph_entry.get("stop_event")
        if stop_event:
            stop_event.set()
        driver = graph_entry.get("driver")
        if driver:
            try:
                driver.close()
            except Exception as exc:
                print(f"[str_report_socket] graph driver close failed: {exc}")

    def _has_live_socket_for_session(session_id):
        session_key = str(session_id)
        for sid, entry in list(sockets_registry.items()):
            if not is_socket_alive(sid, socketio):
                continue
            if _socket_has_session(entry, session_key):
                return True
        return False

    def _parent_session_id(session_id):
        raw = str(session_id or "")
        if "_" not in raw:
            return None
        _, parent = raw.split("_", 1)
        return parent or None

    def _queue_session_lost_notification(session_id):
        session_key = str(session_id)
        details = {
            "session_id": session_key,
            "reason": "socket_disconnect_abandoned",
            "grace_seconds": disconnect_grace_seconds,
        }
        add_notification(
            session_key,
            "warning",
            "session_lost",
            "The analysis session was stopped because the browser disconnected and did not reconnect in time.",
            source="api_socket_guard",
            details=details,
        )
        parent_session = _parent_session_id(session_key)
        if parent_session:
            add_notification(
                parent_session,
                "warning",
                "source_window_session_lost",
                "A source window analysis was stopped because the browser disconnected and did not reconnect in time.",
                source="api_socket_guard",
                details={**details, "source_session_id": session_key, "parent_session_id": parent_session},
            )

    def _stop_abandoned_api_session(session_id):
        session_key = str(session_id)
        socketio.sleep(disconnect_grace_seconds)
        if _has_live_socket_for_session(session_key):
            print(f"[str_report_socket] session {session_key} reconnected before abandonment grace")
            return
        if session_key not in _session_store:
            return
        print(f"[str_report_socket] stopping abandoned API session {session_key}")
        _queue_session_lost_notification(session_key)
        try:
            result = end_session({"session_id": session_key, "reason": "socket_disconnect_abandoned"})
            print(f"[str_report_socket] abandoned session stop result session_id={session_key}: {result}")
        except Exception as exc:
            print(f"[str_report_socket] failed to stop abandoned API session {session_key}: {exc}")

    @socketio.on("connect")
    def handle_connect(auth=None):
        sid = request.sid
        token = (auth or {}).get("token")
        payload = verify_access_token(token)
        actor = None
        if payload:
            actor_type = payload.get("actor_type") or "user"
            if actor_type == "service":
                actor = get_service_account_by_id(payload.get("sub"))
            else:
                actor = get_user_by_id(payload.get("sub"))
        if not actor:
            print(f"[str_report_socket] rejected unauthenticated sid={sid}")
            return False

        entry = get_or_create_socket_entry(sid)
        entry["actor"] = public_actor(actor)
        actor_name = actor.get("username") or actor.get("client_id")
        print(f"[str_report_socket] client connected sid={sid} actor={actor_name}")

    # --------------------------
    # NOTIFICATION SUBSCRIBE
    # --------------------------
    def _handle_str_report_subscription(data, source_event):
        sid = request.sid
        session_id = data.get("session_id") if data else None
        if not session_id:
            print(f"[str_report_socket] {source_event} sid={sid} ignored: missing session_id")
            return

        entry = get_or_create_socket_entry(sid)
        _track_socket_session(sid, session_id)
        subscribe_str_report_session(session_id, sid)
        print(
            f"[str_report_socket] {source_event} sid={sid} session_id={session_id} "
            f"(subscribed_sessions={sorted(entry.get('notification_sessions', set()))})"
        )

    @socketio.on("notification_subscribe")
    def handle_notification_subscribe(data):
        _handle_str_report_subscription(data, "notification_subscribe")

    @socketio.on("str_report_register_receiver")
    def handle_str_report_register_receiver(data):
        _handle_str_report_subscription(data, "str_report_register_receiver")

    # --------------------------
    # NOTIFICATION UNSUBSCRIBE
    # --------------------------
    @socketio.on('notification_unsubscribe')
    def handle_notification_unsubscribe(data):
        sid = request.sid
        session_id = data.get("session_id") if data else None
        entry = sockets_registry.get(sid, {})
        sessions = entry.get("notification_sessions")
        if sessions and session_id:
            sessions.discard(str(session_id))

    # --------------------------
    # LOG STREAM START
    # --------------------------
    @socketio.on('log_stream_plug')
    def handle_log_start(data):
        filename = data.get('filename')
        session_id = data.get('session_id')
        sid = request.sid
        print(
            f"[str_report_socket] log_stream_plug sid={sid} "
            f"session_id={session_id} filename={filename}"
        )

        stop_event = threading.Event()
        task = socketio.start_background_task(
            log_stream_background, socketio, session_id, sid, filename, stop_event
        )

        entry = get_or_create_socket_entry(sid)
        _track_socket_session(sid, session_id)
        entry["log_stream"] = {
            "task": task,
            "stop_event": stop_event,
            "session_id": str(session_id) if session_id else None,
        }


    # --------------------------
    # LOG STREAM STOP
    # --------------------------
    @socketio.on('log_stream_unplug')
    def handle_log_stop(*args, **kwargs):
        data = args[0] if args else None  # Extract payload if provided
        sid = request.sid
        entry = sockets_registry.get(sid, {})
    
        if data and "filename" in data:
            filename = data["filename"]
            log_streams = entry.get("log_streams", {})
            log_entry = log_streams.get(filename)
            if log_entry:
                log_entry["stop_event"].set()
                log_streams.pop(filename, None)
            log_stream = entry.get("log_stream")
            if isinstance(log_stream, dict):
                log_stream["stop_event"].set()
                entry.pop("log_stream", None)
        else:
            log_streams = entry.get("log_streams", {})
            for le in log_streams.values():
                le["stop_event"].set()
            entry.pop("log_streams", None)
            log_stream = entry.get("log_stream")
            if isinstance(log_stream, dict):
                log_stream["stop_event"].set()
                entry.pop("log_stream", None)
    
        print("sockets_registry_from_logs:", sockets_registry)




    # # --------------------------
    # # Graph INFO SUBSCRIBE
    # # --------------------------
    # @socketio.on('graph_info_subscribe')
    # def handle_graph_info_subscribe(data):
    #     sid = request.sid
    #     session_id = data.get("session_id")
    
    #     if not session_id:
    #         return
    
    #     # Avoid duplicate workers
    #     existing = globals.sockets_registry.get(sid)
    #     if existing and "graph_info" in existing:
    #         return
    
    #     if session_id not in _session_store:
    #         socketio.emit(
    #             "graph_infos",
    #             {"status": "no informations", "session_id": session_id},
    #             to=sid
    #         )
    #         return
    
    #     stop_event = threading.Event()
    
    #     task = socketio.start_background_task(
    #         graph_info_worker,
    #         socketio,
    #         sid,
    #         session_id,
    #         stop_event
    #     )
    
    #     globals.sockets_registry[sid] = {
    #         "graph_info": {
    #             "task": task,
    #             "stop_event": stop_event
    #         }
    #     }
        
    # # --------------------------
    # # Graph INFO SUBSCRIBE
    # # --------------------------        
    # @socketio.on('graph_info_unsubscribe')
    # def handle_graph_info_unsubscribe():
    #     sid = request.sid
    #     entry = globals.sockets_registry.pop(sid, None)
    
    #     if entry and "graph_info" in entry:
    #         entry["graph_info"]["stop_event"].set()
    # --------------------------
    # GRAPH STATUS SUBSCRIBE
    # --------------------------
    @socketio.on('graph_status_subscribe')
    def handle_graph_status_subscribe(data):
        sid = request.sid
        session_id = data.get("session_id")
        if not session_id:
            return

        print(f"[str_report_socket] graph_status_subscribe sid={sid} session_id={session_id}")

        entry = get_or_create_socket_entry(sid)
        _track_socket_session(sid, session_id)
        replayed_status_count = flush_status_pending(session_id, sid)

        # ensure multiple session support
        if "graph_statuses" not in entry:
            entry["graph_statuses"] = {}

        if session_id in entry.get("graph_statuses", {}):
            g_entry = entry["graph_statuses"][session_id]

            if is_socket_alive(sid, socketio):
                print(f"Graph status already running for session {session_id}, sid {sid}")
                return
            else:
                print(f"Cleaning up stale graph status for session {session_id}, sid {sid}")

                task = g_entry.get("task")
                stop_event = g_entry.get("stop_event")

                # cooperative stop (eventlet style)
                if stop_event:
                    stop_event.set()

                # cleanup registry
                entry["graph_statuses"].pop(session_id, None)



        tool = load_temp_config("tool", session_id)
        if not tool:
            print("tool not found")
            return

        driver = tools(tool.lower(), "check", {"session_id": session_id})
        session_info = _session_store.get(session_id) or {}
        if not session_info:
            print(
                "[str_report_socket] graph status using persisted graph state "
                f"for session {session_id}; API stream registry is not present"
            )

        stop_event = threading.Event()
        tool_credentials = load_temp_config("tool_credentials", session_id)
        graph_entry = {
            "task": None,
            "stop_event": stop_event,
            "driver": driver,
            "tool_credentials": tool_credentials,
            "static_infos": None,
            "sent_static": False,
        }

        task = socketio.start_background_task(
            graph_status_stream,
            socketio=socketio,
            sid=sid,
            session_id=session_id,
            registry_entry=graph_entry,
            node_label=session_info.get("node_label"),
            primary_rel_type=session_info.get("primary_rel_type"),
        )
        graph_entry["task"] = task
        entry["graph_statuses"][session_id] = graph_entry

    # --------------------------
    # GRAPH STATUS UNSUBSCRIBE
    # --------------------------
    @socketio.on('graph_status_unsubscribe')
    def handle_graph_status_unsubscribe(data):
        sid = request.sid
        entry = sockets_registry.get(sid, {})

        session_id = data.get("session_id") if data else None
        if session_id:
            graph_entry = entry.get("graph_statuses", {}).get(session_id)
            if graph_entry:
                _close_graph_entry(graph_entry)
                entry["graph_statuses"].pop(session_id, None)

    # --------------------------
    # Socket DISCONNECT
    # --------------------------
    @socketio.on("disconnect")
    def handle_disconnect(*_args):
        sid = request.sid
        print(f"[str_report_socket] client disconnected sid={sid}")
        entry = sockets_registry.pop(sid, {})
        watched_sessions = set(str(value) for value in entry.get("analysis_sessions", set()) if value)
        watched_sessions.update(str(value) for value in entry.get("notification_sessions", set()) if value)
        watched_sessions.update(str(value) for value in entry.get("graph_statuses", {}).keys() if value)

        log_stream = entry.get("log_stream")
        if isinstance(log_stream, dict):
            stop_event = log_stream.get("stop_event")
            if stop_event:
                stop_event.set()
            if log_stream.get("session_id"):
                watched_sessions.add(str(log_stream.get("session_id")))

        for graph_entry in entry.get("graph_statuses", {}).values():
            _close_graph_entry(graph_entry)

        for session_id in watched_sessions:
            if session_id in _session_store:
                socketio.start_background_task(_stop_abandoned_api_session, session_id)
