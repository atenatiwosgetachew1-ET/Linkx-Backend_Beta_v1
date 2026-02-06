from flask import request
from flask_socketio import SocketIO

import threading
import globals

from logger import log_stream_background 
from batch_manager.utils.database_utils import graph_status_stream
from globals import load_temp_config,get_or_create_socket_entry,sockets_registry,_session_store
from connection_utils import tools

# check if socket is alive
def is_socket_alive(sid):
    try:
        return socketio.server.manager.is_connected(sid, '/')
    except Exception:
        return False


def register_socket_handlers(socketio: SocketIO):
    """
    Register all Socket.IO event handlers here.
    """

    # --------------------------
    # LOG STREAM START
    # --------------------------
    @socketio.on('log_stream_plug')
    def handle_log_start(data):
        filename = data.get('filename')
        session_id = data.get('session_id')
        sid = request.sid

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
        entry = sockets_registry.get(sid, {})
    
        if data and "filename" in data:
            filename = data["filename"]
            log_streams = entry.get("log_streams", {})
            log_entry = log_streams.get(filename)
            if log_entry:
                log_entry["stop_event"].set()
                log_streams.pop(filename, None)
        else:
            log_streams = entry.get("log_streams", {})
            for le in log_streams.values():
                le["stop_event"].set()
            entry.pop("log_streams", None)
    
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

        entry = get_or_create_socket_entry(sid)

        # ensure multiple session support
        if "graph_statuses" not in entry:
            entry["graph_statuses"] = {}

        if session_id in entry.get("graph_statuses", {}):
            g_entry = entry["graph_statuses"][session_id]

            if is_socket_alive(sid):
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
        session_info = _session_store.get(session_id)
        if not session_info:
            socketio.emit("status", {"type": "waiting", "session_id": session_id}, to=sid)
            return

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
                graph_entry["stop_event"].set()
                entry["graph_statuses"].pop(session_id, None)

    # --------------------------
    # Socket DISCONNECT
    # --------------------------
    @socketio.on("disconnect")
    def handle_disconnect():
        sid = request.sid
        entry = sockets_registry.pop(sid, {})

        for worker in entry.values():
            stop_event = worker.get("stop_event")
            if stop_event:
                stop_event.set()

