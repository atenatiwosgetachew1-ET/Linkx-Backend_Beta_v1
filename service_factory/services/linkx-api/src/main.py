import eventlet
import eventlet.wsgi
eventlet.monkey_patch()

from flask import Flask, request, jsonify, session, render_template, current_app
from flask_socketio import SocketIO, emit

import os
from werkzeug.utils import secure_filename
import time
import shutil
from flask_cors import CORS
from kafka import KafkaConsumer #Kafka consumer
import pandas as pd
from datetime import datetime, timedelta
import random
import threading
import py_compile

from globals import create_file,save_uploaded_file,save_temp_config,load_temp_config,_session_store
from connection_utils import kafka_broker, rest_api, HDFSstorage, tools

from batch_manager.batch_data_manager import batch_data_manager
from batch_manager.config_defaults import get_default_session_config
from batch_manager.services.dataframe_workflow import create_dataframe_response
from batch_manager.processing.realtime_source_loader import load_latest_kafka_message, load_realtime_api, load_kafka_batch_messages
from batch_manager.utils.schema_utils import align_schemas
from batch_manager.utils.postgres_utils import check_postgres_connection
from batch_manager.utils.artifact_utils import ensure_artifact_dir, register_artifact, register_artifact_dir
from batch_manager.processing.merger import merge_pandas_and_save, merge_spark_and_save
from batch_manager.processing.rules_validator import validate_rules_json
from batch_manager.processing.rules_compiler import generate_python_rule, normalize_rule_key
from batch_manager.analyzing.LA_graphs_script import fetch_graph
from batch_manager.analyzing.analyzer import analyzer
from logger import log_writer,log_stream_background
from io_sockets import register_socket_handlers
from api.STR_link_analysis import STR_link_analysis_api
from session_config_store import create_session_config, duplicate_window_config, get_user_config, save_user_config
from auth.decorators import auth_required, current_actor_from_request
from auth.repository import bind_analysis_session_actor
from auth.routes import auth_api
from security.payload_validation import (
    COMMON_SCHEMAS,
    PayloadValidationError,
    validate_json_payload,
    validate_payload,
    validate_uploaded_files,
    validated_json,
)
import globals #Globally used by multible pages (functions and variables) #Contains the front end url



app = Flask(__name__)
allowed_origins = os.getenv("LINKX_CORS_ORIGINS", "*")
cors_origins = "*" if allowed_origins == "*" else [origin.strip() for origin in allowed_origins.split(",") if origin.strip()]
CORS(app, origins=cors_origins)  # Allow configured clients
app.secret_key = os.getenv("LINKX_FLASK_SECRET_KEY", "dev-only-change-me")
app.config["MAX_CONTENT_LENGTH"] = int(os.getenv("LINKX_MAX_UPLOAD_BYTES", "104857600"))
socketio = SocketIO(app, cors_allowed_origins=cors_origins, async_mode="eventlet") #Socket listners are found inside 'logger.py' page
# Register socket
register_socket_handlers(socketio)
# Register auth API blueprint
app.register_blueprint(auth_api, url_prefix="/auth")
# Register external API blueprint
app.register_blueprint(STR_link_analysis_api, url_prefix="/api")



def _validation_error_response(exc):
    body = {"message": "validation_error", "detail": exc.message}
    if exc.field:
        body["field"] = exc.field
    return jsonify(body), 400

def _is_spark_df(df):
    return "pyspark.sql.dataframe.DataFrame" in str(type(df))


def _dataframe_info_from_df(df, session_id):
    if df is None:
        return None

    path_to_save = ensure_artifact_dir("dfparts")
    if isinstance(df, pd.DataFrame):
        num_rows = len(df)
        columns = list(df.columns)
        merge_pandas_and_save([df], path_to_save, session_id)
    elif _is_spark_df(df):
        num_rows = df.count()
        columns = df.columns
        merge_spark_and_save([df], path_to_save, session_id)
    else:
        return None

    return {
        "columns": columns,
        "num_columns": len(columns),
        "num_rows": num_rows,
        "storage_url": load_temp_config("active_storage_address", session_id),
        "broker_url": load_temp_config("active_kafka_adress", session_id),
        "api_url": load_temp_config("active_REST_API", session_id),
        "topic": load_temp_config("active_kafka_topic", session_id),
        "tool": load_temp_config("active_tool", session_id),
        "actions": ["Store data", "Source / Target Relationship", "Link Analysis"],
        "rules": load_temp_config("rule_names", session_id),
    }


def _source_connected_response(df, session_id, message="Connection established!"):
    info = _dataframe_info_from_df(df, session_id)
    if info is None:
        return jsonify({'status': 'warning', 'message': 'Connection established, but no latest message was found.'}), 200
    return jsonify({'status': 'success', 'message': message, 'results': info}), 200


def _is_kafka_batch_topic(topic):
    topic_name = str(topic or "")
    return topic_name.endswith(".batches") or topic_name.endswith("batches")

@app.route('/db/health', methods=['GET'])
def db_health():
    try:
        check_postgres_connection()
        return jsonify({'status': 'success'}), 200
    except Exception as e:
        current_app.logger.warning("PostgreSQL health check failed: %s", e)
        return jsonify({'status': 'error'}), 500

@app.route('/init', methods=['POST'])
@auth_required
@validate_json_payload(COMMON_SCHEMAS["init"])
def init():
    print("Initializing ....")
    data = validated_json()
    current_actor = current_actor_from_request()
    old_session = data.get('existing_session')
    if old_session:
        if not bind_analysis_session_actor(old_session, current_actor):
            return jsonify({'message': 'forbidden'}), 403
        configs = load_temp_config("data", old_session)
        if configs is not None:
            return jsonify({'results': old_session, 'configurations': configs, 'message': 'success!'}), 200

    try:
        max_value = 1000000
        min_value = 0
        session_id = random.randint(min_value, max_value - 1)
        configs = get_default_session_config(session_id)
        if not bind_analysis_session_actor(session_id, current_actor):
            return jsonify({'message': 'failed!', 'results': 'Could not bind session to user.'}), 500
        stored_new_configs = create_session_config(session_id, current_actor, default_config=configs)
        return jsonify({'results': session_id, 'configurations': stored_new_configs, 'message': 'success!'}), 200
    except Exception as e:
        print(e)
        return jsonify({'results': str(e), 'message': 'failed!'}), 200

@app.route('/account/configuration', methods=['GET'])
@auth_required
def account_configuration_load():
    actor = current_actor_from_request()
    if not actor or actor.get("actor_type") != "user":
        return jsonify({"message": "user_required"}), 403
    defaults = get_default_session_config(actor.get("id") or "default")
    config = get_user_config(actor.get("id"), default_config=defaults)
    return jsonify({'results': {'data': config}, 'configurations': config, 'message': 'success!'}), 200


@app.route('/account/configuration', methods=['POST'])
@auth_required
def account_configuration_save():
    actor = current_actor_from_request()
    if not actor or actor.get("actor_type") != "user":
        return jsonify({"message": "user_required"}), 403
    raw_data = request.get_json(silent=True)
    if raw_data is None or not isinstance(raw_data, dict):
        return jsonify({'message': 'validation_error', 'detail': 'json_object_required'}), 400
    config = raw_data.get("config") or raw_data.get("data") or raw_data
    if not isinstance(config, dict):
        return jsonify({'message': 'validation_error', 'detail': 'config_object_required'}), 400
    save_user_config(actor.get("id"), config)
    return jsonify({'results': {'data': config}, 'configurations': config, 'message': 'success!'}), 200


@app.route('/configuration', methods=['POST'])
def configuration():
    data = {}
    files = {}
    if request.is_json:
        raw_data = request.get_json(silent=True)
        if raw_data is None or not isinstance(raw_data, dict):
            return jsonify({'message': 'validation_error', 'detail': 'json_object_required'}), 400
        try:
            data = validate_payload(raw_data, COMMON_SCHEMAS["configuration"])
        except PayloadValidationError as exc:
            return _validation_error_response(exc)
    else:
        data = request.form.to_dict() #Passed datas
        files = request.files.to_dict()  #Uploaded files -> FileStorage object
        files = {key: file for key, file in files.items() if file and file.filename}
        # If any fields are JSON-encoded strings, try parsing
        for key, value in list(data.items()):
            try:
                import json
                data[key] = json.loads(value)
            except (ValueError, TypeError):
                pass
        try:
            data = validate_payload(data, COMMON_SCHEMAS["configuration"])
        except PayloadValidationError as exc:
            return _validation_error_response(exc)

    session_id = data.get("session_id")
    if data.get("id") == "load":
        try:
            config_data = load_temp_config("all", session_id)
            # config_data is already the dict inside "value"
            return jsonify({'results': config_data, 'message': 'success!'}), 200
        except Exception as e:
            return jsonify({'results': str(e), 'message': 'failed!'}), 200
    elif data.get("id") == "save":
        print("Form fields:", data)
        #uploaded file
        if files:
            try:
                safe_files = validate_uploaded_files(list(files.values()), allowed_extensions={"json"}, max_files=5)
            except PayloadValidationError as exc:
                return _validation_error_response(exc)
            for file, filename, _ext in safe_files:
                print(f"Uploaded file: {filename}")
                #Check uploading folder exists
                upload_dir = ensure_artifact_dir("uploads", session_id)
                os.makedirs(upload_dir, exist_ok=True)
                #save upload into Temp folder
                file_path = os.path.join(upload_dir, f"{session_id}_{filename}")
                file.save(file_path)
                register_artifact(file_path, "rule", session_id=session_id, filename=filename, metadata={"source": "uploaded_rule_json"})
                #Validate rule (the uploaded rule)
                try:
                    rule_json = validate_rules_json(file_path)
                    if rule_json:
                        print("The rule is valid:", filename)
                        uploaded_rule_name = rule_json.get("rule_name") or filename.rsplit(".", 1)[0]
                        rule_name = data.get("rule_name", "").strip() or uploaded_rule_name
                        rule_key = normalize_rule_key(rule_name)
                        rule_file_name = f"{rule_key}_rules"

                        # Save Python version of rule
                        rules_dir = ensure_artifact_dir("rules", session_id)
                        os.makedirs(rules_dir, exist_ok=True)
                        output_py = os.path.join(rules_dir, f"{rule_file_name}.py")
                        generate_python_rule(rule_json, output_py)
                        py_compile.compile(output_py, doraise=True)
                        register_artifact(output_py, "rule", session_id=session_id, filename=os.path.basename(output_py), metadata={"source": "compiled_rule"})

                        # Register rule into configuration
                        print("Rule uploaded", session_id)
                        config = load_temp_config("all", session_id)
                        config_dict = config.get("data", {}) or {}

                        # Ensure lists exist
                        config_dict.setdefault("rule_names", [])
                        config_dict.setdefault("rule_file_names", [])

                        # Avoid duplicates
                        if rule_name not in config_dict["rule_names"]:
                            config_dict["rule_names"].append(rule_name)
                        if rule_file_name not in config_dict["rule_file_names"]:
                            config_dict["rule_file_names"].append(rule_file_name)

                        # Activate the new rule
                        config_dict["active_rule"] = [rule_name]

                        # Merge back into configuration
                        save_temp_config("all", config_dict, session_id)

                        return jsonify({
                            'results': "",
                            'configurations': config_dict,
                            'message': 'success!'
                        }), 200
                    else:
                        print("The rule is invalid")
                        return jsonify({'results': "Invalid rule file.", 'message': 'failed!'}), 200
                except Exception as e:
                    print(f"Failed to upload rule: {e}")
                    return jsonify({'results': str(e), 'message': 'failed!'}), 200
        config = load_temp_config("all", session_id)
        config_dict = config.get("data", {}) if config else {}
        if data:
            for key, value in data.items():
                if key in {"id", "session_id", "rule_name"}:
                    continue
                if key == "active_rule":
                    config_dict[key] = value if isinstance(value, list) else [value]
                else:
                    config_dict[key] = value
            save_temp_config("all", config_dict, session_id)
        return jsonify({
            'results': "",
            'configurations': config_dict,
            'message': 'success!'
        }), 200
    elif data.get("id") == "remove_rule":
        rule_name = str(data.get("rule_name") or "").strip()
        if not rule_name:
            return jsonify({'results': "No rule selected.", 'message': 'failed!'}), 400

        config = load_temp_config("all", session_id)
        config_dict = config.get("data", {}) if config else {}
        rule_names = list(config_dict.get("rule_names") or [])
        rule_file_names = list(config_dict.get("rule_file_names") or [])

        if rule_name not in rule_names:
            return jsonify({'results': f"Rule '{rule_name}' not found.", 'message': 'failed!'}), 404

        index = rule_names.index(rule_name)
        removed_file_name = rule_file_names[index] if index < len(rule_file_names) else f"{normalize_rule_key(rule_name)}_rules"
        config_dict["rule_names"] = [name for name in rule_names if name != rule_name]
        config_dict["rule_file_names"] = [
            name for idx, name in enumerate(rule_file_names)
            if idx != index and name != removed_file_name
        ]

        active_rule = config_dict.get("active_rule") or []
        if isinstance(active_rule, str):
            active_rule = [active_rule]
        if rule_name in active_rule:
            config_dict["active_rule"] = [config_dict["rule_names"][0]] if config_dict["rule_names"] else []

        session_rules_dir = ensure_artifact_dir("rules", session_id)
        removed_paths = []
        candidate = os.path.join(session_rules_dir, f"{removed_file_name}.py")
        if os.path.isfile(candidate):
            os.remove(candidate)
            removed_paths.append(candidate)

        pycache_dir = os.path.join(session_rules_dir, "__pycache__")
        if os.path.isdir(pycache_dir):
            pyc_prefix = f"{removed_file_name}."
            for filename in os.listdir(pycache_dir):
                if filename.startswith(pyc_prefix) and filename.endswith(".pyc"):
                    pyc_path = os.path.join(pycache_dir, filename)
                    os.remove(pyc_path)
                    removed_paths.append(pyc_path)

        save_temp_config("all", config_dict, session_id)
        return jsonify({
            'results': {'removed_rule': rule_name, 'removed_files': removed_paths},
            'configurations': config_dict,
            'message': 'success!'
        }), 200
    else:
        print("Unknown action:", data)
        return jsonify({'results': "unknown action", 'message': 'failed!'}), 400

@app.route('/init_source', methods=['POST'])
@validate_json_payload(COMMON_SCHEMAS["init_source"])
def init_source():
    print("Initializing source window....")
    data = validated_json()
    active_session = data.get('session_id')
    window_id = data.get('window_id')
    current_actor = current_actor_from_request()
    if not current_actor:
        return jsonify({'message': 'unauthorized'}), 401
    child_session_id = f"{window_id}_{active_session}"
    try:
        if not bind_analysis_session_actor(child_session_id, current_actor, parent_session_id=str(active_session)):
            return jsonify({'results': "Could not bind source window session to user.", 'message': 'failed!'}), 500

        copied_config = duplicate_window_config(active_session, window_id)
        if copied_config is not None:
            return jsonify({'message': 'success!'}), 200

        config_folder = "public/temp_config"
        file_path = f'{config_folder}/{window_id}_{active_session}_temp_config.json'
        if os.path.isfile(file_path):
            return jsonify({'message': 'success!'}), 200
        file_path = f'{config_folder}/{active_session}_temp_config.json'
        if os.path.isfile(file_path):
            duplicated_file = os.path.join(config_folder, f"{window_id}_{active_session}_temp_config.json")
            shutil.copyfile(file_path, duplicated_file)
            return jsonify({'message': 'success!'}), 200
        return jsonify({'results': "Base session config not found", 'message': 'failed!'}), 404
    except Exception as e:
        print(e)
        return jsonify({'results': str(e), 'message': 'failed!'}), 200

@app.route('/connect_to_source', methods=['POST'])
@validate_json_payload(COMMON_SCHEMAS["connect_to_source"])
def connect_to_source():
    data = validated_json() or {}
    address_type = data.get('addressType') or data.get('type')
    address = data.get('address') or data.get('broker') or data.get('broker_url') or data.get('api') or data.get('url')
    storage = data.get('storage') or data.get('hdfs') #passed hdfs_ip:port
    topic = data.get('topic') or data.get('kafka_topic')
    session_id = data.get('session_id') or data.get('source_id')

    if topic and address_type == "api" and not str(address or "").startswith(("http://", "https://")):
        address_type = "broker"

    if not address_type:
        if topic:
            address_type = 'broker'
        elif data.get('api') or str(address or '').startswith(('http://', 'https://')):
            address_type = 'api'
        elif data.get('broker') or address:
            address_type = 'broker'

    if address_type == "broker":
        if not address:
            return jsonify({'status': 'error', 'message': 'Connection failed! Missing broker address.'}), 400
        if kafka_broker("check", address, session_id, topic=topic) is True:
            print("broker verified")
            save_temp_config("active_source_type", "broker", session_id)
            save_temp_config("active_source_mode", "batch" if _is_kafka_batch_topic(topic) else "realtime", session_id)
            save_temp_config("dataframe_ready", False, session_id)
            if topic:
                try:
                    if _is_kafka_batch_topic(topic):
                        df = load_kafka_batch_messages(address, topic, session_id, max_messages=200, max_rows=1000)
                    else:
                        df = load_latest_kafka_message(address, topic, session_id)
                    return _source_connected_response(df, session_id)
                except Exception as e:
                    print(f"[Kafka latest message error] {e}")
                    return jsonify({'status': 'warning', 'message': 'Broker connected, but latest message could not be loaded.'}), 200
            return jsonify({'status': 'success', 'message': 'Connection established!'}), 200
        return jsonify({'status': 'error', 'message': 'Connection failed!'}), 200

    elif address_type == "api":
        if not address:
            return jsonify({'status': 'error', 'message': 'Connection failed! Missing API address.'}), 400
        if rest_api("check", address, session_id) is True:
            print("api verified")
            save_temp_config("active_source_type", "api", session_id)
            save_temp_config("active_source_mode", "realtime", session_id)
            save_temp_config("dataframe_ready", False, session_id)
            try:
                df = load_realtime_api(address, session_id)
                return _source_connected_response(df, session_id)
            except Exception as e:
                print(f"[API latest message error] {e}")
                return jsonify({'status': 'warning', 'message': 'API connected, but latest message could not be loaded.'}), 200
        return jsonify({'status': 'error', 'message': 'Connection failed!'}), 200

    elif storage:
        if ":" in storage:
            source_port = storage.split(":", 1)[1]
            if source_port != "9870":
                return jsonify({'status': 'Warning', 'message': 'Connection failed! No storage found.'}), 200
        else:
            hdfs_port = load_temp_config("hadoop_rcp_port", session_id)
            storage = f"{storage}:{hdfs_port}"

        if HDFSstorage("check", storage, session_id) is True:
            return jsonify({'status': 'success', 'message': 'Connection established!'}), 200
        return jsonify({'status': 'Warning', 'message': 'Connection failed! No storage found.'}), 200

    else:
        return jsonify({'status': 'error', 'message': 'Connection failed!'}), 400

@app.route('/disconnect_source', methods=['POST'])
@validate_json_payload(COMMON_SCHEMAS["disconnect_source"])
def disconnect_source():
    data = validated_json()
    broker = data.get('broker')
    hdfs = data.get('hdfs')
    session_id = data.get('session_id')
    if broker or hdfs:
        try:
            if kafka_broker("disconnect",broker,session_id) and HDFSstorage("disconnect",hdfs,session_id) is True:
                return jsonify({'status': 'success', 'message': 'Disconnected!'}), 200
            else:
                return jsonify({'status': 'error', 'message': 'Disconnecting failed!'}), 200
        except Exception as e:
            print(e)
            return jsonify({'status': 'error', 'message': 'Disconnecting failed!'}), 500
    else:
        return jsonify({'status': 'error', 'message': 'Disconnecting failed!'}), 400

@app.route('/connect_to_tool', methods=['POST'])
@validate_json_payload(COMMON_SCHEMAS["connect_to_tool"])
def connect_to_tool():
    data = validated_json()
    tool_name = data.get('tool_name')
    url= data.get('url')
    username = data.get('username')
    password = data.get('password')
    database = data.get('database') or load_temp_config("active_tool_database", data.get('source_id'))
    session_id = data.get('source_id')
    payload={"url":url,"username":username,"password":password,"session_id":session_id} 
    if database:
        payload["database"] = database
    if url and username and password:
        if tools(tool_name,"connect",payload) is True:
            return jsonify({'status': 'success', 'message': 'Connected!'}), 200
        else:
            return jsonify({'status': 'error', 'message': 'Not connected!'}), 200
    else:
        return jsonify({'status': 'error', 'message': 'Not connected!'}), 400

@app.route('/disconnect_tool', methods=['POST'])
@validate_json_payload(COMMON_SCHEMAS["disconnect_tool"])
def disconnect_tool():
    data = validated_json()
    session_id = data.get('source_id')
    tool_name = data.get('tool_name')
    payload={"session_id":session_id}
    if session_id:
        if tools(tool_name,"disconnect",payload) is True:
            return jsonify({'status': 'success', 'message': 'Disconnected!'}), 200
        else:
            return jsonify({'status': 'error', 'message': 'Disconnecting failed!'}), 200
    else:
        return jsonify({'status': 'error', 'message': 'Disconnecting failed!'}), 400

@app.route('/upload_batch_files', methods=['POST'])
def upload_batch_files():
    if 'file' not in request.files:
        return jsonify({"message": "No file part in the request"}), 400
    try:
        form_data = validate_payload({"session_id": request.form.get("session_id")}, COMMON_SCHEMAS["upload_batch_files"])
        safe_files = validate_uploaded_files(
            request.files.getlist('file'),
            allowed_extensions={"csv", "json", "parquet", "xlsx"},
            max_files=25,
        )
    except PayloadValidationError as exc:
        return _validation_error_response(exc)

    session_id = form_data["session_id"]
    upload_folder = ensure_artifact_dir("uploads", session_id)

    # Create info file
    create_file(upload_folder, "info", "txt", "This directory is used for temporary uploads.")
    # Save session path in config (assuming you have this function)
    save_temp_config("files_storage_path", upload_folder, session_id)

    for file, _filename, _ext in safe_files:
        saved_path = save_uploaded_file(file, upload_folder, filename_prefix=session_id, session_id=session_id)
        if not saved_path:
            return jsonify({"message": "Failed to save file"}), 500
        register_artifact(saved_path, "upload", session_id=session_id, filename=os.path.basename(saved_path))

    return jsonify({"message": "success!"}), 200

@app.route('/live_batch_files', methods=['POST'])
@validate_json_payload(COMMON_SCHEMAS["live_batch_files"])
def live_batch_files():
    data = validated_json()
    print("1:",data)
    action_id = data.get('id')
    session_id = data.get('session_id')
    if not action_id or not session_id:
        return jsonify({'results': None, 'message': 'Missing action_id or session_id'}), 400

    # -----------------------------
    # SEARCH HDFS / HYBRID
    # -----------------------------
    if action_id == "search":
        value = data.get("value", {})
        storage_ip = load_temp_config("active_storage_address",session_id)
        payload = {
            "id": "search",
            "keyword": value.get("keyword", ""),
            "date": value.get("date") or None,
            "offset": value.get("offset", 0),
            "limit": value.get("limit", 50),
            "search_column": value.get("search_column", "transactionid"), #falback to 'transaction id'
            "hybrid": value.get("hybrid", False),
            "strict": value.get("strict_mood", False),
            "storage": storage_ip,
            "session_id": data.get("session_id"),
        }
        # delegate working logic to batch_data_manager
        result = batch_data_manager(payload)
        if result is None:
            return jsonify({
                "results": 0,
                "has_more": False,
                "offset": 0,
                "limit": 0,
                "message": "No results!"
            }), 200            
        # main.py handles returning the JSON
        return jsonify({
            "results": result.get("results") or [],
            "has_more": result.get("has_more") or False,
            "offset": result.get("offset") or 0,
            "limit": result.get("limit") or 0,
            "message": result.get("message", "")
        }), 200


    # -----------------------------
    # CREATE DATAFRAME / LOAD FILES
    # -----------------------------
    if action_id == "create_DF":
        print("data",data)
        print("kindkindkind:", data.get("kind", ""))
        return create_dataframe_response(data, session_id)
    # -----------------------------
    # START SESSION
    # -----------------------------
    elif action_id == "stream":
        values=data.get("value") or {}    
        #Create the session instance
        payload = {"id": "create_session", "session_id": session_id}
        session = batch_data_manager(payload)
        #Start the session
        if session is True:
            values["id"]="start_session"
            payload=values
            stream = batch_data_manager(payload)
            if stream is not None:
                print("stream:",stream)
                return jsonify({'results': stream, 'message': 'success!'}), 200
            else:
                return jsonify({'results': stream, 'message': 'failed!'}), 400
        else:
            return jsonify({'results': session, 'message': 'failed!'}), 400

    # -----------------------------
    # END SESSION
    # -----------------------------
    elif action_id == "end_session":
        payload = {"id": "end_session", "session_id": session_id}
        result = batch_data_manager(payload)
        return jsonify(result), 200

    # -----------------------------
    # INVALID ACTION
    # -----------------------------
    else:
        return jsonify({'results': None, 'error': f'Invalid action: {action_id}'}), 400

@app.route('/graph_link', methods=['POST'])
@validate_json_payload(COMMON_SCHEMAS["graph_link"])
def graph_link():
    data = validated_json()
    id = data.get('id')
    print(1)
    if id == "link":
        print(2)
        session_id = data.get('source_id')
        session_info = _session_store.get(session_id)  # returns None if not found
        str_report_status = globals.str_report_status_registry.get(str(session_id), {})
        if session_info or str_report_status:
            print(3)
            return jsonify({'message': 'success!'}), 200  # Background fetch or STR status is ready
        else:
            print(4)
            return jsonify({'message': 'failed!'}), 200  # No background fetch
    else:
        print(5)
        return {'results': "No action!", "error": "Invalid Request"}, 400

@app.route('/get_graph', methods=['POST'])
@validate_json_payload(COMMON_SCHEMAS["get_graph"])
def get_graph():
    print("fetch_graph_called")
    data = validated_json()
    id = data.get('id')
    source_id = data.get('source_id','')
    #session_id = data.get('source_id')
    # Take everything after the first underscore
    #session_suffix = session_id.split('_', 1)[1] if session_id and '_' in session_id else None
    # if id == "status":
    #     try:
    #         informationfile=fetch_graph(id,"overview",source_id,"","json");
    #         if informationfile is not None:
    #             return jsonify({'results': informationfile, 'message': 'success!'}), 200
    #         else:
    #             return jsonify({'results': "", 'message': 'failed!'}), 200
    #     except Exception as e:
    #         return jsonify({'exception': str(e), 'message': 'failed!'}), 200
    if id == "relationship":
        try:
            print("id:",id)
            graph = fetch_graph(id,"generate",data["source_id"],data["relationship"],"html") #Static limit is 100000

            # if fetch_graph returned a tuple (Flask Response), return it directly
            if isinstance(graph, tuple):
                return graph
            # otherwise add file info
            graph["file"] = "graphs_template"
            return jsonify({'results': graph, 'message': 'success!'}), 200

        except Exception as e:
            print(e)
            return jsonify({'exception': str(e), 'message': 'failed!'}), 500

    return jsonify({'results': "", 'message': 'failed!'}), 200


if __name__ == "__main__":
    #socketio.run(app, host="0.0.0.0", port=8000, debug=True)
    port = int(os.getenv("PORT", "8100"))
    eventlet.wsgi.server(eventlet.listen(('0.0.0.0', port)), app)
