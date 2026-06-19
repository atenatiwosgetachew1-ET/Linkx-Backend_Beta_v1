from batch_manager.processing.file_source_loader import load_file
from batch_manager.processing.api_source_loader import load_api
from batch_manager.processing.realtime_source_loader import load_kafka_batch_messages
from batch_manager.processing.hdfs_source_loader import load_source
from batch_manager.processing.merger import merge_pandas_and_save, merge_spark_and_save
from batch_manager.processing.session_manager import create_session,start_session,end_session

from batch_manager.utils.spark_utils import get_spark_session
from batch_manager.utils.hdfs_utils import stream_hdfs_metadata,load_hdfs_files

from batch_manager.utils.hive_utils import run_hive_query, hive_keyword_search, load_hive_rows
from batch_manager.utils.elastic_utils import es_keyword_search
from py4j.java_gateway import java_import
import os, pickle
from datetime import datetime, timedelta
from globals import load_temp_config, save_temp_config
from batch_manager.utils.artifact_utils import ensure_artifact_dir
import re
from werkzeug.utils import secure_filename

def _is_kafka_batch_topic(topic):
    topic_name = str(topic or "")
    return topic_name.endswith(".batches") or topic_name.endswith("batches")


def _host_without_port(value):
    value = str(value or "").replace("http://", "").replace("https://", "").replace("hdfs://", "")
    return value.split(":", 1)[0]


def _join_url(base_url, endpoint):
    return f"{str(base_url).rstrip('/')}/{str(endpoint).strip('/')}"


def _storage_host(session_id, fallback=None):
    return load_temp_config("active_storage_host", session_id) or _host_without_port(fallback or load_temp_config("active_storage_address", session_id))


def _storage_hdfs_uri(session_id, fallback=None):
    configured = load_temp_config("storage_hdfs_uri", session_id)
    if configured:
        return configured
    host = _storage_host(session_id, fallback)
    if not host:
        return fallback
    rpc_port = load_temp_config("hdfs_rpc_port", session_id) or load_temp_config("hadoop_rcp_port", session_id) or "8020"
    return f"hdfs://{host}:{rpc_port}"


def _hive_metastore_uri(session_id, fallback=None):
    configured = load_temp_config("hive_metastore_uri", session_id)
    if configured:
        return configured
    host = load_temp_config("hive_server_host", session_id) or _storage_host(session_id, fallback)
    thrift_port = load_temp_config("thrift_port", session_id) or "9083"
    return f"thrift://{host}:{thrift_port}" if host else None


def _elastic_api_url(session_id, endpoint, fallback_storage=None):
    base_url = load_temp_config("elastic_api_base_url", session_id)
    if not base_url:
        host = _storage_host(session_id, fallback_storage)
        api_port = load_temp_config("api_port", session_id) or "5000"
        base_url = f"http://{host}:{api_port}" if host else ""
    return _join_url(base_url, endpoint) if base_url else ""


def _truthy_config(value):
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "on"}



def batch_data_manager(payload):
    action_id = payload.get("id")
    session_id = payload.get("session_id")
    storage_ip = load_temp_config("active_storage_address", session_id)
    storage_hdfs_uri = _storage_hdfs_uri(session_id, storage_ip)
    hive_metastore_uri = _hive_metastore_uri(session_id, storage_ip)
    # -----------------------------
    # SESSION MANAGEMENT
    # -----------------------------
    if action_id == "create_session":
        return create_session(payload)
    if action_id == "start_session":
        session_id=payload["session_id"]
        active_source_type = load_temp_config("active_source_type", session_id)
        active_topic = load_temp_config("active_kafka_topic", session_id)
        active_source_mode = payload.get("source_mode") or load_temp_config("active_source_mode", session_id)
        dataframe_actions = {"Store data", "Source / Target Relationship", "Link Analysis"}
        active_source_is_realtime = (
            active_source_type in {"broker", "kafka", "api"}
            and active_source_mode != "batch"
            and not _is_kafka_batch_topic(active_topic)
        )
        explicit_realtime = payload.get("source_mode") == "realtime" or payload.get("listen_realtime") is True
        explicit_batch = payload.get("source_mode") == "batch"
        dataframe_ready = load_temp_config("dataframe_ready", session_id) is True
        active_dataframe_kind = load_temp_config("active_dataframe_kind", session_id)
        live_address_dataframe = active_dataframe_kind == "address" and active_source_type in {"broker", "kafka", "api"}
        use_existing_dataframe = payload.get("use_dataframe") is True and not active_source_is_realtime
        dataframe_analysis = (
            payload.get("action") in dataframe_actions
            and dataframe_ready
            and not explicit_realtime
            and not active_source_is_realtime
            and (explicit_batch or use_existing_dataframe or not live_address_dataframe)
        )
        should_listen_realtime = (
            active_source_is_realtime
            and not explicit_batch
            and not dataframe_analysis
        )
        print(
            "[batch_router]",
            {
                "session_id": session_id,
                "action": payload.get("action"),
                "payload_source_mode": payload.get("source_mode"),
                "active_source_type": active_source_type,
                "active_source_mode": active_source_mode,
                "active_topic": active_topic,
                "dataframe_ready": dataframe_ready,
                "active_dataframe_kind": active_dataframe_kind,
                "explicit_batch": explicit_batch,
                "explicit_realtime": explicit_realtime,
                "active_source_is_realtime": active_source_is_realtime,
                "dataframe_analysis": dataframe_analysis,
                "should_listen_realtime": should_listen_realtime,
            },
        )
        if explicit_realtime and not active_source_is_realtime:
            return {
                "status": "failed",
                "message": "Realtime source is not connected for this session. Reconnect the source before streaming.",
                "detail": {
                    "active_source_type": active_source_type,
                    "active_source_mode": active_source_mode,
                    "active_topic": active_topic,
                },
            }
        if should_listen_realtime:
            payload["id"] = "realtime_data"
            payload["type"] = "listen"
            payload["source_type"] = "kafka" if active_source_type == "broker" else active_source_type
            payload["broker_url"] = load_temp_config("active_kafka_adress", session_id)
            payload["topic"] = active_topic
            payload["api_url"] = load_temp_config("active_REST_API", session_id)
            payload["api_poll_interval"] = payload.get("api_poll_interval", 5)
        else:
            payload["id"]="batch_data"
            payload["type"]="new"
            #Loading the merged parquet files onto spark
            directory = os.path.join(ensure_artifact_dir("dfparts"), "merged_dfpart_" + session_id) #Pass only the directory (loads all the files inside it)
            #dataframe=load_file(directory,session_id,use_spark=True)
            #print(dataframe)
            payload["dataframe_dir"]=directory
        spark_port = load_temp_config("spark_port", session_id)
        active_tool = load_temp_config("active_tool",session_id)
        tool_credentials = load_temp_config("tool_credentials",session_id)
        payload["spark_conf"] = {
            "storage_ip": storage_ip,
            "spark_port": spark_port
        }
        payload["tool"] = active_tool
        payload["tool_credentials"] = tool_credentials
        return start_session(payload)
    if action_id == "end_session":
        print("2:",action_id)
        payload = {"session_id": session_id}
        return end_session(payload)


    # -----------------------------
    # LOAD FILE/SOURCE (Called before the merge codition (from main.py))
    # -----------------------------
    # 3 layer separation, 1 layer conversion happens here
    #       file                   Keyword
    #        |                        |
    #        |                 --------------
    #        |                |              |
    #        |             es result     hive result
    #        |                |              |
    #        ---------------------------------
    #                       |
    #                 Spark Dataframe
    #                       |
    #               Temporary parquet (files)
    
    if action_id == "load_sourceData": #Is ussually called for Dataframe creation 
        print("kind:",payload["kind"])
        if payload["type"] == "array" and payload["kind"] == "files": #Only for uploaded files
            use_spark = payload.get("use_spark", False)
            print("Now loading files", payload["path"], use_spark)
            # File info is a dict
            file_info = payload["path"]
            session_id = payload["session_id"]
            try:
                file_info = payload["path"]
                session_id = payload["session_id"]

                # CASE 1: Processed folder with parquet parts (file_info is dict)
                if isinstance(file_info, dict):
                    folder_path = os.path.join(ensure_artifact_dir("dfparts"), f"merged_dfpart_{session_id}")
                    df = load_file(folder_path, session_id, use_spark=True)
                    return df

                # CASE 2: Raw uploaded file (file_info is string) (Freash meat) --------------------------------------
                elif isinstance(file_info, str):
                    filename = file_info
                    # Sanitize incoming filename to match how uploads are saved
                    try:
                        safe_name = secure_filename(filename)
                    except Exception:
                        safe_name = filename

                    local_path = os.path.join(ensure_artifact_dir("uploads", session_id), f"{session_id}_{safe_name}")
                    # If exact path doesn't exist, try to find a matching file
                    if not os.path.exists(local_path):
                        uploads_dir = ensure_artifact_dir("uploads", session_id)
                        candidates = []
                        def normalize_name(name):
                            # keep only alphanumeric and dot, convert to lower
                            return re.sub(r"[^0-9a-zA-Z.]", "", name).lower()

                        target_norm = normalize_name(safe_name)
                        for f in os.listdir(uploads_dir):
                            if not f.startswith(f"{session_id}_"):
                                continue
                            # strip session prefix
                            suffix = f[len(session_id) + 1:]
                            if normalize_name(suffix) == target_norm or target_norm in normalize_name(suffix):
                                candidates.append(f)

                        if candidates:
                            chosen = candidates[0]
                            local_path = os.path.join(uploads_dir, chosen)
                            print("Fallback: matched upload file:", local_path)
                        else:
                            print("No matching uploaded file found for:", local_path)

                    df = load_file(local_path, session_id, use_spark)
                    return df

                else:
                    raise ValueError("Invalid file_info format received")

            except Exception as e:
                print("Error in load_file handler:", e)
                return None
        elif payload["kind"] == "address" and payload["type"] in {"api", "broker", "kafka"}:
            use_spark = payload.get("use_spark", False)
            session_id = payload["session_id"]
            source_type = payload["type"]
            if source_type == "api" and payload.get("topic"):
                source_type = "broker"
            try:
                if source_type == "api":
                    url = payload.get("files") or payload.get("address")
                    if url:
                        save_temp_config("active_REST_API", url, session_id)
                        save_temp_config("active_source_type", "api", session_id)
                        save_temp_config("active_source_mode", payload.get("source_mode") or "batch", session_id)
                    df = load_api(url, session_id, use_spark=use_spark)
                else:
                    broker_url = payload.get("broker") or payload.get("broker_url") or payload.get("address") or payload.get("files")
                    if isinstance(broker_url, (list, tuple)):
                        broker_url = broker_url[0] if broker_url else None
                    topic = payload.get("topic")
                    if broker_url:
                        save_temp_config("active_kafka_adress", broker_url, session_id)
                        save_temp_config("active_source_type", "broker", session_id)
                    if topic:
                        save_temp_config("active_kafka_topic", topic, session_id)
                    if not broker_url:
                        raise ValueError("Missing Kafka broker address")
                    if not topic:
                        raise ValueError("Missing Kafka topic")
                    is_batch_topic = _is_kafka_batch_topic(topic)
                    save_temp_config("active_source_mode", "batch" if is_batch_topic else "realtime", session_id)
                    max_messages = payload.get("max_messages") or payload.get("limit") or (200 if is_batch_topic else 1)
                    max_rows = payload.get("max_rows") or payload.get("row_limit") or (1000 if is_batch_topic else None)
                    from_beginning = bool(payload.get("from_beginning", False))
                    df = load_kafka_batch_messages(
                        broker_url,
                        topic,
                        session_id,
                        use_spark=use_spark,
                        max_messages=max_messages,
                        max_rows=max_rows,
                        from_beginning=from_beginning,
                    )
                print("df:", df)
                return df
            except Exception as e:
                print("Error in address source handler:", e)
                return None
        
        elif payload["type"] == "array" and payload["kind"] == "hybrid": # Works with along side # CASE 2 (Fresh meat)-- #For files and keyword search
            print("Now loading hdfs files")
            files = payload.get("files") or payload.get("value") or []
            date = payload.get("date",None)    
            # ---------------------------------------------------------------- Mutual payloads
            storage_address = storage_ip       
            if ":" in storage_address: #check the port exists
                storage_address = storage_ip.split(":", 1)[0] #get only the ip
            api_search_endpoint = "" #To be updated below
            API_URL = ""
            api_port = load_temp_config("api_port",session_id)   
            fetch_columns = load_temp_config("fetch_columns", session_id)   
            date_column = load_temp_config("date_column", session_id)   
            hive_search_endpoint_strict = load_temp_config("search_api_endpoint_hive_strict", session_id)   
            hive_search_endpoint_fuzzy = load_temp_config("search_api_endpoint_hive_fuzzy", session_id)   
            es_search_endpoint_strict = load_temp_config("search_api_endpoint_es_strict", session_id)   
            es_search_endpoint_fuzzy = load_temp_config("search_api_endpoint_es_fuzzy", session_id)   
            dfs = []
            large_search_backend = str(load_temp_config("large_search_backend", session_id) or "hive").strip().lower()
            elastic_scroll_enabled = _truthy_config(load_temp_config("elastic_scroll_enabled", session_id))
            use_elastic_for_large_search = large_search_backend in {"elastic", "elastic_scroll", "scroll"} or elastic_scroll_enabled
            print("large_search_backend:", large_search_backend, "elastic_scroll_enabled:", elastic_scroll_enabled)
            # ---------------------------------------------------------------- Categorize datas with identity (elastic,hive)
            hdfs_categories = []
            elastic_categories = []
            hive_categories = []
            for file in files:
                print("trying file type:",file['type'])           
                if file['type'] == 'raw':
                    hdfs_categories.append(file) 
                elif file['type'] == 'elastic':
                    elastic_categories.append(file)
                elif file['type'] == 'hive':
                    if use_elastic_for_large_search:
                        elastic_file = dict(file)
                        elastic_file["type"] = "elastic"
                        elastic_file["large_result_backend"] = "elastic_scroll"
                        elastic_categories.append(elastic_file)
                    else:
                        hive_categories.append(file) 
                else:
                    print("Error on file type:",file['type'])           
            # ---------------------------------------------------------------- Raw HDFS files
            if len(hdfs_categories) > 0: #Consists an elastic datas
                spark_port = load_temp_config("spark_port", session_id)
                spark = get_spark_session(storage_ip, spark_port, hdfs_uri=storage_hdfs_uri)            
                print("Consists hdfs file values",hdfs_categories)                               
                try:
                    df = load_hdfs_files(spark,hdfs_categories)
                    if df is not None:
                        print("collecting dfss")
                        dfs.extend(df)
                except Exception as e:
                    print(f"Error during hdfs raw file fetch: {e}")                    
            # ---------------------------------------------------------------- Elastic DFs (default limit 100,000)
            if len(elastic_categories) > 0: #Consists an elastic datas
                print("Consists elastic values",elastic_categories)               
                for file in elastic_categories:      
                    id = "fetch"
                    search_column = file.get('column')    
                    keyword = file.get('keyword')                         
                    strict_mood = file.get('strict')
                    print("search_column:",search_column)
                    if file.get('strict', False): #if data is from a stict search
                        print("strictttt")
                        endpoint = es_search_endpoint_strict
                    else:
                        print("not strictttt")
                        endpoint = es_search_endpoint_fuzzy   
                    #trigger a fetching logic (call a function that returns the df)                                 
                    API_URL = _elastic_api_url(session_id, endpoint, storage_address)
                    try:
                        fetch_limit = None
                        if file.get("large_result_backend") == "elastic_scroll":
                            fetch_limit = load_temp_config("elastic_scroll_limit", session_id) or load_temp_config("dataframes_limit", session_id)
                        df = es_keyword_search(id, API_URL, keyword, search_column, strict_mood, date_column, date, fetch_columns, limit=fetch_limit)
                        if df is not None:
                            dfs.append(df)
                    except Exception as e:
                        print(f"Error during es fetch: {e}")
            # ---------------------------------------------------------------- Hive DFs (Results above the limit 100,000)
            if len(hive_categories) > 0: #Consists an hive datas 
                hive_port = load_temp_config("hive_port", session_id)
                spark_port = load_temp_config("spark_port", session_id)
                thrift_port = load_temp_config("thrift_port",session_id)
                spark = get_spark_session(storage_ip, spark_port, thrift_port, hdfs_uri=storage_hdfs_uri, hive_metastore_uri=hive_metastore_uri)            
                storage_database= load_temp_config("active_storage_database",session_id)
                storage_tables = load_temp_config("active_storage_tables",session_id) or []
                limit = load_temp_config("dataframes_limit",session_id)
                tables = [f"{storage_database}.{t}" for t in storage_tables]
                print("Consists Hive values")               
                for file in hive_categories:     
                    search_column = file.get('column')    
                    keyword = file.get('keyword')                         
                    try:
                        if search_column:
                            search_columns = [{"field": search_column}]
                        else:
                            configured = load_temp_config("search_columns_strict" if file.get('strict', False) else "search_columns_fuzzy", session_id) or []
                            search_columns = [{"field": col} for col in configured]
                        df = load_hive_rows(
                            storage_address,
                            hive_port,
                            spark,
                            search_columns,
                            tables,
                            [keyword],
                            date=date,
                            limit=limit,
                        )
                        if df is not None:
                            dfs.append(df)
                    except Exception as e:
                        print(f"Error during hive fetch: {e}")

            #---------------------------------------------------------------------- Returning collective Dataframes                        
            return dfs
        else: 
            print("here1:",payload["type"],payload["kind"])
            return False

    # -----------------------------
    # MERGE DATAFRAMES
    # -----------------------------
    if action_id == "merge":
        print("dataframe to merge")
        dfs = payload.get("dfs", [])
        use_spark = payload.get("use_spark", False)
        kind = payload.get("kind")
        path = ensure_artifact_dir("dfparts")
        if use_spark or kind == "hdfs":
            return merge_spark_and_save(dfs,path,session_id)
        else:
            return merge_pandas_and_save(dfs,path,session_id)
  

    # -----------------------------
    # SEARCH (HDFS / HIVE) (2 layer searching)
    # -----------------------------
    if action_id == "search":
        keyword = payload.get("keyword", "")
        date = payload.get("date")
        offset = payload.get("offset", 0)
        limit = payload.get("limit", 50)
        hybrid = payload.get("hybrid")
        strict = payload.get("strict")
        storage_ip = payload.get("storage") 
        search_columns_elastic = payload.get("search_column") #Single column, or configured list for fuzzy search
        search_columns_hive = "" #Multi columns
        date_column = load_temp_config("date_column",session_id)  
        if hybrid and not strict and search_columns_elastic in (None, "", "transactionid"):
            search_columns_elastic = load_temp_config("search_columns_fuzzy", session_id) or search_columns_elastic
        #-----------------------------------------------------------------------
        if hybrid:#Elastic search -> hive search if it exceeds 100000 results    
            storage_address = storage_ip       
            if ":" in storage_address: #check the port exists
                storage_address = storage_ip.split(":", 1)[0] #get only the ip  
            api_port = load_temp_config("api_port",session_id)     
            api_search_endpoint = ""    
            #--------------------------------------------------------------------------            
            if strict: #Strict condition
                api_search_endpoint = load_temp_config("search_api_endpoint_es_strict",session_id)  
                search_columns_hive = load_temp_config("search_columns_strict",session_id)
            else: #Fuzzy condition
                api_search_endpoint = load_temp_config("search_api_endpoint_es_fuzzy",session_id) 
                search_columns_hive = load_temp_config("search_columns_fuzzy",session_id) 
            #--------------------------------------------------------------------------            
            API_URL = _elastic_api_url(session_id, api_search_endpoint, storage_address)
            #--------------------------------------------------------------------------            
            #Hive payloads
            storage_database= load_temp_config("active_storage_database",session_id)
            storage_tables = load_temp_config("active_storage_tables",session_id)
            hive_port = load_temp_config("hive_port",session_id)
            tables = []
            for t in storage_tables:
                tables.append(f"{storage_database}.{t}")
            hive_payload=[storage_address, keyword, search_columns_hive, strict, hive_port, tables, date_column, date] #For further hive searchings
            #--------------------------------------------------------------------------            
            #print("search params:","keyword:",keyword,"date:",date,"offset:",offset,"limit:",limit,"hybrid:",hybrid,"strict:",strict,"api_search_endpoint:",api_search_endpoint,"search_columns_elastic:",search_columns_elastic,"search_columns_hive:",search_columns_hive)
            response = es_keyword_search(action_id, API_URL, keyword, search_columns_elastic, strict, date_column, date, limit=limit, offset=offset) #Overrides to hive (Result out of bound)        
            print("es_response:",response)
        else:#Staric Row files search
            normalized_keyword = str(keyword or "").strip()
            if normalized_keyword and not re.search(r"[A-Za-z0-9]", normalized_keyword):
                return {
                    "results": [],
                    "has_more": False,
                    "offset": offset,
                    "limit": limit,
                    "message": "Raw file search keyword is too broad. Use letters, numbers, or leave it empty to list files.",
                }
            storage_path = load_temp_config("storage_path",session_id)
            base_path = f"/{storage_path}"
            response = stream_hdfs_metadata(storage_ip, base_path, keyword, date, offset, limit)

        return response


    # -----------------------------
    # INVALID ACTION
    # -----------------------------
    return {"error": f"Invalid action: {action_id}"}
