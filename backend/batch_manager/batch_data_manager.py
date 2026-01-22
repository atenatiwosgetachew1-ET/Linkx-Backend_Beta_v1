from batch_manager.processing.file_source_loader import load_file
from batch_manager.processing.hdfs_source_loader import load_source
from batch_manager.processing.merger import merge_pandas_and_save, merge_spark_and_save
from batch_manager.processing.session_manager import create_session,start_session,end_session

from batch_manager.utils.spark_utils import get_spark_session
from batch_manager.utils.hdfs_utils import stream_hdfs_metadata

from batch_manager.utils.hive_utils import run_hive_query, hive_dynamic_search, hive_keyword_search_count
from batch_manager.utils.elastic_utils import es_keyword_search_count
from py4j.java_gateway import java_import
import os, pickle
from datetime import datetime, timedelta
from globals import load_temp_config
import re

def batch_data_manager(payload):
    action_id = payload.get("id")
    session_id = payload.get("session_id")

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
        storage_ip = load_temp_config("active_storage_address", session_id)
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
        if payload["type"] == "array" and payload["kind"] == "files":

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
                    local_path = f"public/temp_uploads/{session_id}_{filename}"
                    df = load_file(local_path, session_id, use_spark)
                    return df

                else:
                    raise ValueError("Invalid file_info format received")

            except Exception as e:
                print("Error in load_file handler:", e)
                return None

        elif payload["type"] == "array" and payload["kind"] == "hdfs": # Works with along side # CASE 2 (Fresh meat)--
            print("Now loading hdfs files")
            files = payload["files"]
            date = payload.get("date",None)
            fetch_columns = load_temp_config("fetch_columns", session_id)
            storage_ip = load_temp_config("active_storage_address", session_id)
            spark_port = load_temp_config("spark_port", session_id)
            hive_port = load_temp_config("hive_port", session_id)
            thrift_port = load_temp_config("thrift_port",session_id)
            spark = get_spark_session(storage_ip, spark_port, thrift_port)
            storage_address = storage_ip       
            if ":" in storage_address: #check the port exists
                storage_address = storage_ip.split(":", 1)[0] #get only the ip
            #payload["spark"] = spark
            #Elastic payloads
            api_port = load_temp_config("api_port",session_id)         
            api_search_endpoint = load_temp_config("search_api_endpoint",session_id)         
            API_URL = f"http://{storage_address}:{api_port}/{api_search_endpoint}"
            #Hive payloads
            storage_database= load_temp_config("active_storage_database",session_id)
            storage_tables = load_temp_config("active_storage_tables",session_id)
            hive_port = load_temp_config("hive_port",session_id)
            tables = []    
            for t in storage_tables:
                tables.append(f"{storage_database}.{t}")
            #Mutual payloads
            search_columns_list = load_temp_config("search_columns", session_id)
            limit = load_temp_config("dataframes_limit",session_id)
            if search_columns_list:
                search_columns = [{"field": field_name, "value": ""} for field_name in search_columns_list]

            dataframe_payload={"session_id":session_id,"storage_address":storage_address,"hive_port":hive_port,"tables":tables,"dataframes_limit":limit,"date":date,"API_URL":API_URL,"files":files,"fetch_columns":fetch_columns,"search_columns":search_columns,"spark":spark}
            df = load_source(dataframe_payload)
            print("dataframe returned")
            return df
        else: 
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
    # ANALYZE DATAFRAME
    # -----------------------------
    if action_id == "analyze":
        df = payload.get("df")
        return analyze_data(df)

    # -----------------------------
    # SEARCH (HDFS / HIVE) (2 layer searching)
    # -----------------------------
    if action_id == "search":
        keyword = payload.get("keyword", "")
        date = payload.get("date")
        offset = payload.get("offset", 0)
        limit = payload.get("limit", 50)
        storage_ip = payload.get("storage")        
        # Load the dynamic list of columns to search on from the configuration file
        search_columns_list = load_temp_config("search_columns", session_id)
        if search_columns_list:
            search_columns = [{"field": field_name, "value": keyword} for field_name in search_columns_list]

        if payload.get("hybrid"):#Elastic search -> hive search if it exceeds 100000 results  
            # First Elastic then hive through it
            storage_address = storage_ip       
            if ":" in storage_address: #check the port exists
                storage_address = storage_ip.split(":", 1)[0] #get only the ip
            #Elastic payloads
            api_port = load_temp_config("api_port",session_id)         
            api_search_endpoint = load_temp_config("search_api_endpoint",session_id)         
            API_URL = f"http://{storage_address}:{api_port}/{api_search_endpoint}"
            #Hive payloads
            storage_database= load_temp_config("active_storage_database",session_id)
            storage_tables = load_temp_config("active_storage_tables",session_id)
            hive_port = load_temp_config("hive_port",session_id)
            tables = []    
            for t in storage_tables:
                tables.append(f"{storage_database}.{t}")
            hive_payload=[storage_address, keyword, search_columns, hive_port, tables, date] #For further hive searchings
            response = es_keyword_search_count(API_URL, keyword, search_columns, hive_payload, date)
        else:#Staric Row files search
            storage_path = load_temp_config("storage_path",session_id)
            base_path = f"/{storage_path}"
            response = stream_hdfs_metadata(storage_ip, base_path, keyword, date, offset, limit)

        return response


    # -----------------------------
    # INVALID ACTION
    # -----------------------------
    return {"error": f"Invalid action: {action_id}"}
