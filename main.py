import eventlet
import eventlet.wsgi
eventlet.monkey_patch()

from flask import Flask, request, jsonify, session, render_template
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

from globals import create_file,save_uploaded_file,save_temp_config,load_temp_config,_session_store
from connection_utils import kafka_broker, HDFSstorage, tools

from batch_manager.batch_data_manager import batch_data_manager
from batch_manager.utils.schema_utils import align_schemas
from batch_manager.processing.merger import merge_pandas_and_save, merge_spark_and_save
from batch_manager.processing.rules_validator import validate_rules_json
from batch_manager.processing.rules_compiler import generate_python_rule
from batch_manager.analyzing.LA_graphs_script import fetch_graph
from batch_manager.analyzing.analyzer import analyzer
from logger import log_writer,log_stream_background
from io_sockets import register_socket_handlers
from api.Ext_APIs import ext_api
import globals #Globally used by multible pages (functions and variables) #Contains the front end url



app = Flask(__name__)
CORS(app)  # Allow frontend
app.secret_key = 'datascience'
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="eventlet") #Socket listners are found inside 'logger.py' page
# Register socket
register_socket_handlers(socketio)
# Register external API blueprint
app.register_blueprint(ext_api, url_prefix="/api")

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
        configs={
            "session_id":session_id,
            "user_id":"Unknown",
            "kafka_addresses":[],
            "REST APIs":[],
            "active_kafka_adress":"",            
            "active_REST_API":"",
            "storage_addresses":["172.20.137.129"],
            "storage_path":"user/bank/cleaned_partitioned",
            "storage_databases":["bankdb","bank_db"],
            "storage_tables":["individual_transactions","entity_transactions"],
            "active_storage_address":"172.20.137.129",
            "active_storage_database":"bankdb",
            "active_storage_tables":["individual_transactions","entity_transactions"],
            "hadoop_rcp_port":"9870",
            "hadoop_web_port":"",
            "spark_port":"4040",
            "thrift_port":"9083",
            "hive_port":"10000",
            "api_port":"5000",
            "search_api_endpoint_es_fuzzy":"api/search/individual",
            "search_api_endpoint_es_strict":"api/search/ui",
            "search_api_endpoint_hive_fuzzy":"api/search/individual",
            "search_api_endpoint_hive_strict":"api/search/ui",
            "search_columns_strict":["transactionid","businessmobileno","accountno","benaccountno","bentelno","transactiondate","transactiontime"],
            "search_columns_fuzzy":["entity_name","involver_name","othername","accownername","benfullname","branchname","benbranchname","city","bencity","country","bencountry","transactiontype","amountinbirr","balanceheld"],
            "fetch_columns":["TRANSACTIONID","BRANCHNAME","TRANSACTIONDATE","TRANSACTIONTIME","TRANSACTIONTYPE","AMOUNTINBIRR","ACCOWNERNAME","BUSINESSMOBILENO","ACCOUNTNO","BALANCEHELD","BENFULLNAME","BENACCOUNTNO","BENTELNO"],
            "date_column": "transactiondate",
            "dataframes_limit":1000000,
            "tools":["neo4j"],
            "active_tool":"neo4j",
            "active_tool_protocol":"neo4j://172.21.22.88",    
            "active_tool_username":"neo4j",    
            "active_tool_password":"1430fmgat762190",    
            "active_tool_database":"",
            "active_tool_tables":[],    
            "tool_protocol_port":"7687",
            "tool_web_port":"7473",
            "rule_names":["bank transactions","social media (tweeter)"],
            "rule_file_names": ["bank_transactions_rules","social_media_(tweeter)_rules"],
            "active_rule":["bank transactions"],
            "automation":'true',
            "remote":'false',
        }
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
                #Check uploading folder exists
                upload_dir = os.path.join("public","temp_uploads")
                os.makedirs(upload_dir, exist_ok=True)
                #save upload into Temp folder
                filename = secure_filename(file.filename)
                file_path = os.path.join(upload_dir, filename)
                file.save(file_path)
                #Validate rule (the uploaded rule)
                try:
                    rule_json = validate_rules_json(file_path)
                    if rule_json:
                        print("The rule is valid:", filename)
                        file_name = filename.lower().replace(".json", "")
                        rule_name = data.get("rule_name", "").strip() or file_name

                        # Save Python version of rule
                        rules_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "public", "temp_rules")
                        os.makedirs(rules_dir, exist_ok=True)
                        output_py = os.path.join(rules_dir, f"{rule_name}.py")
                        generate_python_rule(rule_json, output_py)

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
                        if file_name not in config_dict["rule_file_names"]:
                            config_dict["rule_file_names"].append(file_name)

                        # Activate the new rule
                        config_dict["active_rule"] = [rule_name]

                        # Merge back into configuration
                        save_temp_config("all", config_dict, session_id)

                        return jsonify({'results': "", 'message': 'success!'}), 200
                    else:
                        print("The rule is invalid")
                        return jsonify({'results': "Invalid rule file.", 'message': 'failed!'}), 200
                except Exception as e:
                    print(f"Failed to upload rule: {e}")
                    return jsonify({'results': str(e), 'message': 'failed!'}), 200
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
            return jsonify({'results': str(e), 'message': 'failed!'}), 200
    except Exception as e:
        print(e)
        return jsonify({'results': str(e), 'message': 'failed!'}), 200

@app.route('/connect_to_source', methods=['POST'])
def connect_to_source():
    data = request.get_json()
    broker = data.get('broker')
    source_url = data.get('hdfs') #passed hdfs_ip:port
    session_id = data.get('session_id')
    if not source_url: #if hdfs url is not passed, get from configuration file
        hdfs = load_temp_config("active_storage_address",session_id)
        hdfs_port = load_temp_config("hadoop_rcp_port",session_id)
        source_url=f"{hdfs}:{hdfs_port}"
    if broker:
        #checking for broker
        if kafka_broker("check",broker,session_id) is True:
            #checking HDFS or do something
            pass
        else:
            return jsonify({'status': 'error', 'message': 'Connection failed!'}), 200
    if source_url: #if there is hdfs ip from both cases
        if ":" in source_url: #check the port exists
            source_port = source_url.split(":", 1)[1]
            if source_port != "9870": #if the port is not as expected, return
                return jsonify({'status': 'Warning', 'message': 'Connection failed! No storage found.'}), 200
        else:#if the port is not stated
            hdfs_port = load_temp_config("hadoop_rcp_port",session_id)
            source_url= f"{source_url}:{hdfs_port}"  #use the configuration port

        #continue to connect
        if HDFSstorage("check",source_url,session_id) is True:
            return jsonify({'status': 'success', 'message': 'Connection established!'}), 200
        else:
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
        files = data.get('value', [])
        date = data.get('date',None)
        kind = data.get("kind", "")
        df_type = data.get("type")
        use_spark = True if kind.lower() == "spark" else False

        # -----------------------------
        # 1) Load all files
        # -----------------------------
        dfs = []

        if kind == "files":  # individual file iteration
            for f in files:
                payload = {
                    "id": "load_sourceData",
                    "session_id": session_id,
                    "path": f,
                    "use_spark": use_spark,
                    "type": df_type,
                    "kind": kind
                }
                print("create_df_payload",payload)
                try:
                    df = batch_data_manager(payload)

                    # Handle all valid cases
                    if df is None:
                        print("batch_data_manager returned None, skipping file:", f)
                        continue
                    elif isinstance(df, dict) and "df" in df:
                        dfs.append(df["df"])
                    elif hasattr(df, "columns"):  # pandas or Spark DF
                        dfs.append(df)
                    elif isinstance(df, pd.DataFrame) or 'pyspark.sql.dataframe.DataFrame' in str(type(df)):
                        dfs.append(df)
                    else:
                        print("Skipping invalid object returned by batch_data_manager:", df)
                        continue
                except Exception as e:
                    print(f"Error loading file {f}: {e}")
                    continue
        else:  # HDFS / bulk
            print("files:",files)
            payload = {
                "id": "load_sourceData",
                "session_id": session_id,
                "files": files, #files contain identities like [Elastic/hive and strict/fuzzy] 
                "date": date,
                "use_spark": use_spark,
                "type": df_type,
                "kind": kind
            }
            print("create_df_payload:",payload)
            df = batch_data_manager(payload)
            print("dfsLen",len(df))
            if df is None:
                print(0)
                return {"results": "", "message": "Failed to load DF"}, 400
            elif isinstance(df, list):
                print(1)
                for d in df:
                    if hasattr(d, "columns"):
                        dfs.append(d)
                    else:
                        print(10)
            elif isinstance(df, dict) and "df" in df:
                print(2)
                dfs.append(df["df"])
            elif hasattr(df, "columns"):
                print(3)
                dfs.append(df)
            elif isinstance(df, pd.DataFrame) or 'pyspark.sql.dataframe.DataFrame' in str(type(df)):
                print(4)
                dfs.append(df)
            else:
                print("here:", df)
                return {"results": "", "message": "No valid DF returned from batch_data_manager"}, 400


        if not dfs:
            return {"results": "", "message": "No valid dataframes loaded"}, 400

        # -----------------------------
        # Align & merge DataFrames by type
        # -----------------------------
        try:
            # 1) Collect all columns for alignment
            all_columns = set()
            for df in dfs:
                all_columns.update(df.columns)
            all_columns = list(all_columns)

            # Align schemas
            aligned = [align_schemas(df, all_columns) for df in dfs]

            # 2) Separate by type
            pandas_dfs = [df for df in aligned if isinstance(df, pd.DataFrame)]
            spark_dfs = [df for df in aligned if 'pyspark.sql.dataframe.DataFrame' in str(type(df))]

            path_to_save = "public/temp_dfParts/"
            merged_pandas = None
            merged_spark = None

            # First Remove all files and subdirectories inside the directory
            for filename in os.listdir(path_to_save):
                file_path = os.path.join(path_to_save, filename)
                try:
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.unlink(file_path)  # Remove the file or link
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)  # Remove the directory and its contents
                except Exception as e:
                    print(f'Failed to delete {file_path}. Reason: {e}')

            # Merge pandas DataFrames (once)
            if pandas_dfs:
                merged_pandas = merge_pandas_and_save(pandas_dfs, path_to_save, session_id)
                if merged_pandas is None:
                    return jsonify({"results": "", "message": "Failed to merge pandas DFs!"}), 400
                num_rows_pandas = len(merged_pandas)
                columns_pandas = list(merged_pandas.columns)
            else:
                num_rows_pandas = 0
                columns_pandas = []

            # Merge Spark DataFrames (once)
            if spark_dfs:
                merged_spark = merge_spark_and_save(spark_dfs, path_to_save, session_id)
                if merged_spark is None:
                    return jsonify({"results": "", "message": "Failed to merge Spark DFs!"}), 400
                num_rows_spark = merged_spark.count()
                columns_spark = merged_spark.columns
            else:
                num_rows_spark = 0
                columns_spark = []

            # 3) Combine results (optional)
            final_columns = list(set(columns_pandas + list(columns_spark)))
            total_rows = num_rows_pandas + num_rows_spark  # approximate total rows

            # 4) Config / metadata
            storage_url = load_temp_config("active_storage_address", session_id)
            broker_url = load_temp_config("active_kafka_adress", session_id)
            actions = ["Store data", "Source / Target Relationship", "Link Analysis"]
            rules = load_temp_config("rule_names", session_id)
            tool = load_temp_config("active_tool", session_id)

            return jsonify({
                "results": {
                    "columns": final_columns,
                    "num_columns": len(final_columns),
                    "num_rows": total_rows,
                    "storage_url": storage_url,
                    "broker_url": broker_url,
                    "tool": tool,
                    "actions": actions,
                    "rules": rules
                },
                "message": "success!"
            }), 200

        except Exception as e:
            app.logger.exception("create_DF failed")
            return jsonify({
                "results": None,
                "message": str(e)
            }), 500
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
        if session_info:
            print(3)
            return jsonify({'message': 'success!'}), 200  # Background fetch is running
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
