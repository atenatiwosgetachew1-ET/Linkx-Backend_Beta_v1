from batch_manager.processing.file_source_loader import load_file
from batch_manager.processing.hdfs_source_loader import load_source
from batch_manager.processing.merger import merge_pandas_and_save, merge_spark_and_save
from batch_manager.processing.session_manager import create_session,start_session,end_session

from batch_manager.utils.spark_utils import get_spark_session
from batch_manager.utils.hdfs_utils import stream_hdfs_metadata,load_hdfs_files

from batch_manager.utils.hive_utils import run_hive_query, hive_keyword_search
from batch_manager.utils.elastic_utils import es_keyword_search
from py4j.java_gateway import java_import
import os, pickle
from datetime import datetime, timedelta
from globals import load_temp_config
import re
from werkzeug.utils import secure_filename

def batch_data_manager(payload):
    action_id = payload.get("id")
    session_id = payload.get("session_id")
    storage_ip = load_temp_config("active_storage_address", session_id)
    # -----------------------------
    # SESSION MANAGEMENT
    # -----------------------------
    if action_id == "create_session":
        return create_session(payload)
    if action_id == "start_session":
        session_id=payload["session_id"]
        payload["id"]="batch_data"
        payload["type"]="new"
        #Loading the merged parquet files onto spark
        directory ="public/temp_dfParts/merged_dfpart_"+session_id+"/" #Pass only the directory (loads all the files inside it)
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
                    folder_path = f"public/temp_dfParts/merged_dfpart_{session_id}/"
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

                    local_path = f"public/temp_uploads/{session_id}_{safe_name}"
                    # If exact path doesn't exist, try to find a matching file
                    if not os.path.exists(local_path):
                        uploads_dir = "public/temp_uploads"
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

        elif payload["type"] == "array" and payload["kind"] == "hybrid": # Works with along side # CASE 2 (Fresh meat)-- #For files and keyword search
            print("Now loading hdfs files")
            files = payload["files"]
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
            # ---------------------------------------------------------------- Categorize datas with identity (elastic,hive)
            hdfs_categories = []
            elastic_categories = []
            hive_categories = []
            for file in files:
                if file['type'] == 'raw':
                    hdfs_categories.append(file) 
                elif file['type'] == 'elastic':
                    elastic_categories.append(file)
                elif file['type'] == 'hive':
                    hive_categories.append(file)            
            # ---------------------------------------------------------------- Raw HDFS files
            if len(hdfs_categories) > 0: #Consists an elastic datas
                spark_port = load_temp_config("spark_port", session_id)
                spark = get_spark_session(storage_ip, spark_port)            
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
                    API_URL = f"http://{storage_address}:{api_port}/{endpoint}"
                    try:
                        df = es_keyword_search(id, API_URL, keyword, search_column, strict_mood, date_column, date, fetch_columns)
                        if df is not None:
                            dfs.append(df)
                    except Exception as e:
                        print(f"Error during es fetch: {e}")
            # ---------------------------------------------------------------- Hive DFs (Results above the limit 100,000)
            hive_port = load_temp_config("hive_port", session_id)
            spark_port = load_temp_config("spark_port", session_id)
            thrift_port = load_temp_config("thrift_port",session_id)
            spark = get_spark_session(storage_ip, spark_port, thrift_port)            
            storage_database= load_temp_config("active_storage_database",session_id)
            storage_tables = load_temp_config("active_storage_tables",session_id)
            limit = load_temp_config("dataframes_limit",session_id)
            tables = []
            for t in storage_tables:
                tables.append(f"{storage_database}.{t}")       
            if len(hive_categories) > 0: #Consists an hive datas 
                print("Consists Hive values")               
                for file in hive_categories:     
                    id = "fetch"
                    search_column = file.get('column')    
                    keyword = file.get('keyword')                         
                    strict_mood = file.get('strict')              
                    if file.get('strict', False): #if data is from a stict search
                        endpoint = hive_search_endpoint_strict
                    else:
                        endpoint = hive_search_endpoint_fuzzy
                    #trigger a fetching logic (call a function that returns the df)         
                    API_URL = f"http://{storage_address}:{api_port}/{endpoint}"
                    try:
                        df = hive_keyword_search(id, API_URL, keyword, search_column, strict_mood, date_column, spark, date, fetch_columns)
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
        path = "public/temp_dfParts/"  # keep as local path (relative or absolute)
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
        search_columns_elastic = payload.get("search_column") #Single column
        search_columns_hive = "" #Multi columns
        date_column = load_temp_config("date_column",session_id)  
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
            API_URL = f"http://{storage_address}:{api_port}/{api_search_endpoint}"
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
            response = es_keyword_search(action_id, API_URL, keyword, search_columns_elastic, strict, date_column, date) #Overrides to hive (Result out of bound)        
            print("es_response:",response)
        else:#Staric Row files search
            storage_path = load_temp_config("storage_path",session_id)
            base_path = f"/{storage_path}"
            response = stream_hdfs_metadata(storage_ip, base_path, keyword, date, offset, limit)

        return response


    # -----------------------------
    # INVALID ACTION
    # -----------------------------
    return {"error": f"Invalid action: {action_id}"}
