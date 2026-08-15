from flask import request
from flask_socketio import SocketIO

import threading
import globals

from logger import log_stream_background 
from batch_manager.utils.database_utils import graph_status_stream
from batch_manager.batch_data_manager import _build_default_tool_credentials
from batch_manager.utils.notification_utils import (
    set_notification_socketio,
    subscribe_str_report_session,
    flush_status_pending,
)
from globals import load_temp_config,get_or_create_socket_entry,sockets_registry,_session_store
from connection_utils import tools
from auth.repository import get_service_account_by_id, get_user_by_id, get_user_by_username, public_actor
from auth.tokens import verify_access_token
from security.payload_validation import COMMON_SCHEMAS, PayloadValidationError, validate_payload
import os

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

    @socketio.on("connect")
    def handle_connect(auth=None):
        sid = request.sid
        auth_data = _validated_socket_payload(auth or {}, "socket_connect", "connect") if auth else {}
        token = auth_data.get("token") if auth_data else None
        payload = verify_access_token(token) if token else None
        actor = None
        if payload:
            actor_type = payload.get("actor_type") or "user"
            if actor_type == "service":
                actor = get_service_account_by_id(payload.get("sub"))
            else:
                actor = get_user_by_id(payload.get("sub"))

        if not actor:
            auto_admin = os.getenv("LINKX_AUTO_LOGIN_ADMIN", "true").lower() in ("1", "true", "yes")
            if auto_admin:
                actor = get_user_by_username("admin") or get_user_by_id(1)

        if not actor:
            print(f"[str_report_socket] rejected unauthenticated sid={sid}")
            return False

        entry = get_or_create_socket_entry(sid)
        entry["actor"] = public_actor(actor)
        actor_name = actor.get("username") or actor.get("client_id")
        print(f"[str_report_socket] client connected sid={sid} actor={actor_name}")

    def _validated_socket_payload(data, schema_name, source_event):
        try:
            if data is None:
                data = {}
            if not isinstance(data, dict):
                raise PayloadValidationError("socket_payload_object_required")
            return validate_payload(data, COMMON_SCHEMAS[schema_name])
        except PayloadValidationError as exc:
            print(f"[str_report_socket] {source_event} ignored: {exc.message}")
            return None

    # --------------------------
    # NOTIFICATION SUBSCRIBE
    # --------------------------
    def _handle_str_report_subscription(data, source_event):
        sid = request.sid
        data = _validated_socket_payload(data, "socket_session", source_event)
        if not data:
            return
        session_id = str(data.get("session_id") or data.get("source_id"))

        entry = get_or_create_socket_entry(sid)
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
        data = _validated_socket_payload(data, "socket_session", "notification_unsubscribe")
        session_id = str(data.get("session_id") or data.get("source_id")) if data else None
        entry = sockets_registry.get(sid, {})
        sessions = entry.get("notification_sessions")
        if sessions and session_id:
            sessions.discard(str(session_id))

    # --------------------------
    # LOG STREAM START
    # --------------------------
    @socketio.on('log_stream_plug')
    def handle_log_start(data):
        sid = request.sid
        data = _validated_socket_payload(data, "socket_log_stream", "log_stream_plug")
        if not data:
            return
        filename = data.get('filename') or data.get('log_file')
        session_id = str(data.get('session_id') or data.get('source_id'))
        print(
            f"[str_report_socket] log_stream_plug sid={sid} "
            f"session_id={session_id} filename={filename}"
        )

        stop_event = threading.Event()
        task = socketio.start_background_task(
            log_stream_background, socketio, session_id, sid, filename, stop_event
        )

        entry = get_or_create_socket_entry(sid)
        entry["log_stream"] = {
            "task": task,
            "stop_event": stop_event
        }


    # --------------------------
    # LOG STREAM STOP
    # --------------------------
    @socketio.on('log_stream_unplug')
    def handle_log_stop(*args, **kwargs):
        data = args[0] if args else None  # Extract payload if provided
        sid = request.sid
        if data is not None:
            data = _validated_socket_payload(data, "socket_log_unplug", "log_stream_unplug")
            if data is None:
                return
        entry = sockets_registry.get(sid, {})
    
        if data and ("filename" in data or "log_file" in data):
            filename = data.get("filename") or data.get("log_file")
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
    #     session_id = str(data.get("session_id") or data.get("source_id"))
    
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
        data = _validated_socket_payload(data, "socket_session", "graph_status_subscribe")
        if not data:
            return
        session_id = str(data.get("session_id") or data.get("source_id"))

        print(f"[str_report_socket] graph_status_subscribe sid={sid} session_id={session_id}")
        socketio.emit("status", {"type": "subscribed", "session_id": session_id}, to=sid)

        entry = get_or_create_socket_entry(sid)
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



        tool = load_temp_config("tool", session_id) or load_temp_config("active_tool", session_id)
        if not tool:
            socketio.emit("status", {"type": "error", "error": "Tool not configured", "session_id": session_id}, to=sid)
            return

        tool_credentials = load_temp_config("tool_credentials", session_id)
        if not tool_credentials:
            tool_credentials = _build_default_tool_credentials(session_id, tool)
        if not load_temp_config("tool_credentials", session_id) and tool_credentials:
            from globals import save_temp_config
            save_temp_config("tool_credentials", tool_credentials, session_id)
            save_temp_config("tool", tool, session_id)

        driver = tools(str(tool).lower(), "check", {"session_id": session_id})
        if not driver:
            socketio.emit("status", {"type": "error", "error": "Tool connection unavailable", "session_id": session_id}, to=sid)
            return

        session_info = _session_store.get(session_id) or {}
        stop_event = threading.Event()
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
        data = _validated_socket_payload(data, "socket_session", "graph_status_unsubscribe")
        entry = sockets_registry.get(sid, {})

        session_id = str(data.get("session_id") or data.get("source_id")) if data else None
        if session_id:
            graph_entry = entry.get("graph_statuses", {}).get(session_id)
            if graph_entry:
                graph_entry["stop_event"].set()
                entry["graph_statuses"].pop(session_id, None)

    # --------------------------
    # Socket DISCONNECT
    # --------------------------
    @socketio.on("disconnect")
    def handle_disconnect(*_args):
        sid = request.sid
        print(f"[str_report_socket] client disconnected sid={sid}")
        entry = sockets_registry.pop(sid, {})

        log_stream = entry.get("log_stream")
        if isinstance(log_stream, dict):
            stop_event = log_stream.get("stop_event")
            if stop_event:
                stop_event.set()

        for graph_entry in entry.get("graph_statuses", {}).values():
            if isinstance(graph_entry, dict):
                stop_event = graph_entry.get("stop_event")
                if stop_event:
                    stop_event.set()
