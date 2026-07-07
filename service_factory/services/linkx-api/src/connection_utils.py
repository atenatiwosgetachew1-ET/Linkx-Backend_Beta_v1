# connection_utils.py
from flask import Flask, session
import os
import requests
from kafka import KafkaConsumer
from hdfs import InsecureClient
import json
from globals import create_file,save_temp_config,load_temp_config,sockets_registry
from batch_manager.utils.neo4j_utils import create_neo4j_driver, neo4j_database_name, redacted_neo4j_credentials



#global_broker=None
tool_driver_registry = {}

def rest_api(id, api_url, session_id):
    if id == "check":
        try:
            headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
            response = requests.get(api_url, headers=headers, timeout=5)

            if response.ok:
                data = response.json()  # could still raise exception if not JSON
                save_temp_config("active_REST_API", api_url, session_id)
                return True
            else:
                print(f"API not reachable! Status code: {response.status_code}")
                return False
        except Exception as e:
            print(f"[REST API Error] {e}")
            return False

    elif id == "disconnect":
        # Clear the stored API instead of saving it again
        save_temp_config("active_REST_API", "", session_id)
        return True

    else:
        raise ValueError(f"Unknown id value: {id}")

def kafka_broker(id,broker_url,session_id, topic=None):
    global global_broker
    if id == "check":
        try:
            consumer = KafkaConsumer(bootstrap_servers=[broker_url], request_timeout_ms=3000)
            topics = consumer.topics()
            if topic and topic not in topics:
                consumer.close()
                return False
            consumer.close()
            #print("kafka found")
            global_broker=broker_url
            save_temp_config("global_broker/API",broker_url,session_id)
            save_temp_config("active_kafka_adress",broker_url,session_id)
            if topic:
                save_temp_config("active_kafka_topic",topic,session_id)
            return True
        except Exception as e:
            #print(f"[Broker Error] {e}")
            #print("kafka not found")
            return False
    if id == "disconnect":
        return True
def HDFSstorage(id, webhdfs_url,session_id):
    if id == "check":
        raw_url = str(webhdfs_url or "").strip()
        address = raw_url if raw_url.startswith(("http://", "https://")) else "http://" + raw_url
        # print(f"Connecting to WebHDFS at: {address}")
        try:
            hdfs_user = load_temp_config("storage_hdfs_user", session_id) or os.getenv("LINKX_STORAGE_HDFS_USER", "link")
            client = InsecureClient(address, user=hdfs_user)
            #print("Attempting to list root directory...")
            items = client.status('/')   # only checks root metadata
            # print(f"Items in root: {items}")
            # print("hdfs found")
            storage = raw_url.replace("http://", "").replace("https://", "")
            storage_host = storage.split(":", 1)[0]
            webhdfs_port = storage.split(":", 1)[1] if ":" in storage else os.getenv("LINKX_STORAGE_WEBHDFS_PORT", "9870")
            hdfs_rpc_port = os.getenv("LINKX_HDFS_RPC_PORT", os.getenv("LINKX_HADOOP_RCP_PORT", "8020"))
            save_temp_config("storage_hdfs_user", hdfs_user, session_id)
            save_temp_config("active_storage_address", storage, session_id)
            save_temp_config("active_storage_host", storage_host, session_id)
            save_temp_config("storage_webhdfs_url", f"http://{storage_host}:{webhdfs_port}", session_id)
            if not load_temp_config("storage_hdfs_uri", session_id):
                save_temp_config("storage_hdfs_uri", f"hdfs://{storage_host}:{hdfs_rpc_port}", session_id)
            return True
        except Exception as e:
            print(f"Error: {e}")
            # print("hdfs not found")
            return False
    if id == "disconnect":
        try:
            #storage=None
            #save_temp_config("global_storage","",session_id)
            return True
        except Exception as e:            
            return False
def tools(id,action,payload):
    global tool_driver_registry
    if id == "neo4j":
        if action == "connect":
            credentials=payload
            url=credentials["url"]
            username=credentials["username"]
            password=credentials["password"]
            session_id=credentials["session_id"]
            print("creds_to_connect:", {**redacted_neo4j_credentials(credentials), "session_id": session_id})
            try:
                # response=[]
                neo4j_driver=create_neo4j_driver(credentials)
                query = "RETURN 1 AS ok"  # Sample query to test credentials/database
                with neo4j_driver.session() as session:
                    try:
                        result = session.run(query)
                        response={"state":"connected","result":neo4j_driver}
                        save_temp_config("tool", id,session_id)
                        save_temp_config("tool_credentials", credentials,session_id)
                        tool_driver_registry[session_id]=neo4j_driver
                        # Reset static graph info for all sockets of this session
                        for sid, entry in list(sockets_registry.items()):
                            status = entry.get("status")
                            if not status:
                                continue

                            status["static_infos"] = None
                            status["sent_static"] = False
                        return True
                    except Exception as e:
                        print(str(e))
                        # response={"state":"failed","message":"Entered wrong credentials!"}
                        return False
            except Exception as e:
                print(str(e))
                #response={"state":"failed","message":"URI scheme not supported!"}
                return False
        if action == "disconnect":  
            session_id=payload["session_id"]                  
            tool_driver_registry[payload["session_id"]]=None
            save_temp_config("tool_credentials", None,session_id)
            return True
        if action == "check":
            session_id = payload["session_id"]
            print("session_id:",session_id)
            creds = load_temp_config("tool_credentials", session_id)
            print("creds:", redacted_neo4j_credentials(creds) if isinstance(creds, dict) else creds)
            if not creds:
                return False
            url = creds["url"]
            username = creds["username"]
            password = creds["password"]
            try:
                neo4j_driver=create_neo4j_driver(creds)
                query = "RETURN 1 AS ok"  # Sample query to test credentials/database
                with neo4j_driver.session() as session:
                    try:
                        result = session.run(query)
                        tool_driver_registry[session_id]=neo4j_driver
                        return neo4j_driver
                    except Exception as e:
                        # response={"state":"failed","message":"Entered wrong credentials!"}
                        return False
            except Exception as e:
                print(e)
                #response={"state":"failed","message":"URI scheme not supported!"}
                return False