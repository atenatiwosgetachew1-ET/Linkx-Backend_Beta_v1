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
from batch_manager.processing.merger import merge_pandas_and_save, merge_spark_and_save
from batch_manager.processing.rules_validator import validate_rules_json
from batch_manager.processing.rules_compiler import generate_python_rule, normalize_rule_key
from batch_manager.analyzing.LA_graphs_script import fetch_graph
from batch_manager.analyzing.analyzer import analyzer
from logger import log_writer,log_stream_background
from io_sockets import register_socket_handlers
from api.STR_link_analysis import STR_link_analysis_api
import globals #Globally used by multible pages (functions and variables) #Contains the front end url



app = Flask(__name__)
allowed_origins = os.getenv("LINKX_CORS_ORIGINS", "*")
cors_origins = "*" if allowed_origins == "*" else [origin.strip() for origin in allowed_origins.split(",") if origin.strip()]
CORS(app, origins=cors_origins)  # Allow frontend
app.secret_key = os.getenv("LINKX_FLASK_SECRET_KEY", "dev-only-change-me")
socketio = SocketIO(app, cors_allowed_origins=cors_origins, async_mode="eventlet") #Socket listners are found inside 'logger.py' page
# Register socket
register_socket_handlers(socketio)
# Register external API blueprint
app.register_blueprint(STR_link_analysis_api, url_prefix="/api")


def _is_spark_df(df):
    return "pyspark.sql.dataframe.DataFrame" in str(type(df))


def _dataframe_info_from_df(df, session_id):
    if df is None:
        return None

    path_to_save = "public/temp_dfParts/"
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
def init():
    print("Initializing ....")
    data = request.get_json()
    # Check if the config file already exists
    old_session = data.get('existing_session')
    file_path = f'public/temp_config/{old_session}_temp_config.json'    # Check if file exists
    if os.path.isfile(file_path):
        # if the File exists just return
        configs=load_temp_config("data",old_session)
        return jsonify({'results': old_session, 'configurations': configs, 'message': 'success!'}), 200
    try:
        #create new session instances (called when the page initalizes load first  time)
        #Preparing new session
        now = datetime.now()
        max_value = 1000000
        min_value = 0
        session_id = random.randint(min_value, max_value - 1)
        config_folder = "public/temp_config/"
        configs = get_default_session_config(session_id)
        # Create info file
        create_file(config_folder, f"{session_id}_temp_config", "json", configs)
        print("config_folder:",config_folder)
        stored_new_configs=load_temp_config("data",session_id)
        return jsonify({'results': session_id, 'configurations': stored_new_configs, 'message': 'success!'}), 200
    except Exception as e:
        print(e)
        return jsonify({'results': str(e), 'message': 'failed!'}), 200

@app.route('/configuration', methods=['POST'])
def configuration():
    data = {}
    files = {}
    if request.is_json:
        data = request.get_json()
    else:
        data = request.form.to_dict() #Passed datas
        files = request.files.to_dict()  #Uploaded files -> FileStorage object
        files = {key: file for key, file in files.items() if file and file.filename}
        # If any fields are JSON-encoded strings, try parsing
        for key, value in data.items():
            try:
                import json
                data[key] = json.loads(value)
            except (ValueError, TypeError):
                pass

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
            for key, file in files.items():
                print(f"Uploaded file: {key} -> {file.filename}")
                if not file.filename or not file.filename.lower().endswith(".json"):
                    return jsonify({'results': "Rule upload must be a JSON file.", 'message': 'failed!'}), 400
                #Check uploading folder exists
                upload_dir = os.path.join("public","temp_uploads")
                os.makedirs(upload_dir, exist_ok=True)
                #save upload into Temp folder
                filename = secure_filename(file.filename)
                file_path = os.path.join(upload_dir, f"{session_id}_{filename}")
                file.save(file_path)
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
                        rules_dir = os.path.join(
                            os.path.dirname(os.path.abspath(__file__)),
                            "public",
                            "temp_rules",
                            str(session_id),
                        )
                        os.makedirs(rules_dir, exist_ok=True)
                        output_py = os.path.join(rules_dir, f"{rule_file_name}.py")
                        generate_python_rule(rule_json, output_py)
                        py_compile.compile(output_py, doraise=True)

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

        session_rules_dir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "public",
            "temp_rules",
            str(session_id),
        )
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
def init_source():
    print("Initializing source window....")
    data = request.get_json()
    # Check if the config file already exists
    active_session = data.get('session_id')
    window_id = data.get('window_id')
    config_folder = "public/temp_config"
    file_path = f'{config_folder}/{window_id}_{active_session}_temp_config.json'    # Check if file exists
    if os.path.isfile(file_path):
        # if the File exists just return        
        return jsonify({'message': 'success!'}), 200
    try:#if the configuration file of that specific window doesn't exist, then check for the initial configuration file and do a duplication
        file_path = f'{config_folder}/{active_session}_temp_config.json'    # Check if file exists
        if os.path.isfile(file_path):#create a duplication of the configuration file as a duplication that represent the specific window id             
            # duplicate the file
            # duplicated file with window_id prefix
            duplicated_file = os.path.join(
                config_folder, f"{window_id}_{active_session}_temp_config.json"
            )
            shutil.copyfile(file_path, duplicated_file)
            return jsonify({'message': 'success!'}), 200
        else:
            return jsonify({'results': "Base session config not found", 'message': 'failed!'}), 404
    except Exception as e:
        print(e)
        return jsonify({'results': str(e), 'message': 'failed!'}), 200

@app.route('/connect_to_source', methods=['POST'])
def connect_to_source():
    data = request.get_json() or {}
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
def disconnect_source():
    data = request.get_json()
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
    else:
        return jsonify({'status': 'error', 'message': 'Disconnecting failed!'}), 400

@app.route('/connect_to_tool', methods=['POST'])
def connect_to_tool():
    data = request.get_json()
    tool_name = data.get('tool_name')
    url= data.get('url')
    username = data.get('username')
    password = data.get('password')
    session_id = data.get('source_id')
    payload={"url":url,"username":username,"password":password,"session_id":session_id} 
    if url and username and password:
        if tools(tool_name,"connect",payload) is True:
            return jsonify({'status': 'success', 'message': 'Connected!'}), 200
        else:
            return jsonify({'status': 'error', 'message': 'Not connected!'}), 200
    else:
        return jsonify({'status': 'error', 'message': 'Not connected!'}), 400

@app.route('/disconnect_tool', methods=['POST'])
def disconnect_tool():
    data = request.get_json()
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
    files = request.files.getlist('file')
    session_id = request.form.get("session_id")
    upload_folder = "public/temp_uploads/"

    # Create info file
    create_file(upload_folder, "info", "txt", "This directory is used for temporary uploads.")
    # Save session path in config (assuming you have this function)
    save_temp_config("files_storage_path", upload_folder, session_id)

    for file in files:
        if file.filename == '':
            return jsonify({"message": "No file selected"}), 400
        ext = file.filename.rsplit('.', 1)[1].lower()
        allowed_ext = {"csv", "json", "parquet", "xlsx"}
        if ext not in allowed_ext:
            return jsonify({"message": f"Unsupported file type: .{ext}"}), 400
        saved_path = save_uploaded_file(file, upload_folder, filename_prefix=session_id, session_id=session_id)
        if not saved_path:
            return jsonify({"message": "Failed to save file"}), 500

    return jsonify({"message": "success!"}), 200

@app.route('/live_batch_files', methods=['POST'])
def live_batch_files():
    data = request.get_json()
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
            "date": value.get("date", datetime.today().date().isoformat()),
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
        values=data.get("value")    
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
def graph_link():
    data = request.get_json()
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
def get_graph():
    print("fetch_graph_called")
    if request.is_json: #If Json is sent
        data = request.get_json()   
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

    else: #If form is sent              
        return jsonify({'results': "", 'message': 'failed!'}), 200


if __name__ == "__main__":
    #socketio.run(app, host="0.0.0.0", port=8000, debug=True)
    eventlet.wsgi.server(eventlet.listen(('0.0.0.0', 8000)), app)
