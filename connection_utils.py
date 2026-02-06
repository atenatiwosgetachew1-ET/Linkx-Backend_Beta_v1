# connection_utils.py
from flask import Flask, session

from kafka import KafkaConsumer
from hdfs import InsecureClient
from neo4j import GraphDatabase

from globals import create_file,save_temp_config,load_temp_config,sockets_registry



global_broker=None
tool_driver_registry = {}


def kafka_broker(id,broker_url,session_id):
    global global_broker
    if id == "check":
        try:
            consumer = KafkaConsumer(bootstrap_servers=[broker_url], request_timeout_ms=3000)
            consumer.topics()
            consumer.close()
            #print("kafka found")
            global_broker=broker_url
            save_temp_config("global_broker/API",broker_url,session_id)
            return True
        except Exception as e:
            #print(f"[Broker Error] {e}")
            #print("kafka not found")
            return False
    if id == "disconnect":
        return True
def HDFSstorage(id, webhdfs_url,session_id):
    if id == "check":
        address="http://"+webhdfs_url
        # print(f"Connecting to WebHDFS at: {address}")
        try:
            client = InsecureClient(address)
            #print("Attempting to list root directory...")
            items = list(client.status('/'))   # only checks root metadata
            # print(f"Items in root: {items}")
            # print("hdfs found")
            storage=webhdfs_url
            save_temp_config("active_storage_address",storage,session_id)
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
            print("creds_to_connect:",credentials)
            try:
                # response=[]
                neo4j_driver=GraphDatabase.driver(url, auth=(username,password))
                query = "MATCH (n) RETURN n LIMIT 1"  # Sample query to test
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
            save_temp_config("tool_credentials", "",session_id)
            return True
        if action == "check":
            session_id = payload["session_id"]
            print("session_id:",session_id)
            creds = load_temp_config("tool_credentials", session_id)
            print("creds:",creds)
            if not creds:
                return False
            url = creds["url"]
            username = creds["username"]
            password = creds["password"]
            try:
                neo4j_driver=GraphDatabase.driver(url, auth=(username,password))
                query = "MATCH (n) RETURN n LIMIT 1"  # Sample query to test
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