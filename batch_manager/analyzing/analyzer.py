from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
from kafka.admin import KafkaAdminClient
from neo4j import GraphDatabase
import threading
import re
from datetime import datetime
import time
import uuid
import os
import importlib.util
from connection_utils import tools
from logger import log_writer
from batch_manager.analyzing.LA_rules_script import batch_graph_analysis_posts,batch_graph_analysis_transactions
from batch_manager.processing.file_source_loader import load_file
from globals import load_temp_config,_session_store

#Commit Check

global_iteration_thread = None
iteration_thread_registry = {}

def json_serializer(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()  # Convert datetime to ISO string
    raise TypeError(f"Type {type(obj)} not serializable")

def neo4j_row_data_adjuster(row_dict):
    # Time adjustment
    try:
        if 'TRANSACTIONDATE' in row_dict and 'TRANSACTIONTIME' in row_dict:
            date_obj = datetime.strptime(row_dict['TRANSACTIONDATE'], "%m/%d/%Y")
            time_obj = datetime.strptime(row_dict['TRANSACTIONTIME'], "%I:%M:%S %p")
            row_dict['TRANSACTIONDATE'] = date_obj.date().isoformat()
            row_dict['TRANSACTIONTIME'] = time_obj.time().isoformat()
    except Exception as e:
        pass
    return row_dict

def make_write_partition(neo4j_conf, batch_size=500):

    def write_partition(rows):
        driver = GraphDatabase.driver(
            neo4j_conf["url"],
            auth=(neo4j_conf["username"], neo4j_conf["password"])
        )

        with driver.session() as session:
            batch = []
            for row in rows:
                record = {k: ("" if v is None else v) for k, v in row.asDict().items()}
                record.setdefault("NodeId", str(uuid.uuid4()))
                batch.append(record)

                if len(batch) >= batch_size:
                    session.run("""
                        UNWIND $rows AS row
                        MERGE (n:Entity { NodeId: row.NodeId })
                        SET n += row
                    """, rows=batch)
                    batch.clear()

            if batch:
                session.run("""
                    UNWIND $rows AS row
                    MERGE (n:Entity { NodeId: row.NodeId })
                    SET n += row
                """, rows=batch)

        driver.close()

    return write_partition

def set_session_status(driver, session_id, status, rule=None):
    with driver.session() as session:
        session.run("""
            MERGE (s:Session {id: $id})
            SET s.status = $status,
                s.rule = coalesce($rule, s.rule),
                s.updated_at = datetime()
        """, id=session_id, status=status, rule=rule)

def check_rule_status(rule_key, rule_path):
    if not os.path.exists(rule_path):
        return None, "No rule file found."
    spec = importlib.util.spec_from_file_location(rule_key, rule_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    if hasattr(module, 'main'):
        return module, True
    else:
        return None, "No main() function found inside the rule file."


def neo4j_row_data_injector(payload, batch_size=500):
    tool_credentials = payload.get("neo4j_conf")
    df = payload.get("dataframe")
    log_file = payload.get("log_file")
    action = payload.get("action")
    stop_event = payload.get("stop_event")
    session_id = payload.get("session_id")
    print("neo4j_row_data_injector_session_id:",session_id)
    if not tool_credentials or df is None:
        log_writer(log_file, f"{datetime.now()} [Error] - Missing Neo4j credentials or dataframe")
        return

    if stop_event and stop_event.is_set():
        log_writer(log_file, f"[{datetime.now()}] [Stop] Stop signal received — terminating injector")
        return

    driver = GraphDatabase.driver(
        tool_credentials["url"],
        auth=(tool_credentials["username"], tool_credentials["password"])
    )
    set_session_status(driver, session_id, "INGESTING")
    try:
        log_writer(log_file, f"[{datetime.now()}] [Info] - Injection started for action '{action}'")

        if action == "Store data":
            log_writer(log_file, f"[{datetime.now()}] [Info] - Storing nodes in batches of {batch_size}")

            # Collect rows safely
            rows = [r.asDict() for r in df.toLocalIterator()]
            total_rows = len(rows)
            log_writer(log_file, f"[{datetime.now()}] [Info] - {total_rows} rows collected")

            with driver.session() as session:
                session.run("""
                    CREATE CONSTRAINT IF NOT EXISTS FOR (n:Entity) REQUIRE n.NodeId IS UNIQUE
                """)

                for i in range(0, total_rows, batch_size):
                    if stop_event and stop_event.is_set():
                        log_writer(log_file, f"[{datetime.now()}] [STOP] Node insertion cancelled")
                        break

                    batch = rows[i:i + batch_size]
                    for r in batch:
                        r.setdefault("NodeId", str(uuid.uuid4()))

                    session.run("""
                        UNWIND $rows AS row
                        MERGE (n:Entity { NodeId: row.NodeId })
                        SET n += row
                    """, rows=batch)

                    log_writer(
                        log_file,
                        f"[{datetime.now()}] [Info] - Inserted batch {i//batch_size + 1} ({len(batch)} rows)"
                    )
            set_session_status(driver, session_id, "READY_FOR_ANALYSIS")
            log_writer(log_file, f"[{datetime.now()}] [Info] - Node insertion completed successfully")
        if action == "Source / Target Relationship":
            source_col = payload.get("source")
            target_col = payload.get("target")
            relationship_type = payload.get("relationship")

            # Sanitize the relationship label
            relationship_type = re.sub(r'[^a-zA-Z0-9_]', '_', relationship_type.strip())

            log_writer(log_file, f"[{datetime.now()}] [Info] - Creating relationships")

            def sanitize_props(d):
                return {k.replace(" ", "_").replace(".", "_"): v for k, v in d.items() if v is not None}

            rows = (
                df.where(f"{source_col} IS NOT NULL AND {target_col} IS NOT NULL")
                .dropDuplicates([source_col, target_col])
                .toLocalIterator()
            )

            rels = []
            for r in rows:
                if r[source_col] == r[target_col]:
                    continue
                row_dict = sanitize_props(r.asDict(recursive=True))
                rels.append({
                    "source": r[source_col],
                    "target": r[target_col],
                    "props": row_dict
                })

            total_rels = len(rels)
            log_writer(log_file, f"[{datetime.now()}] [Info] - {total_rels} relationships collected")

            if session_id in _session_store:
                _session_store[session_id]["primary_rel_type"] = relationship_type

            with driver.session() as session:
                # Create indexes
                session.run(f"CREATE INDEX IF NOT EXISTS FOR (n:Entity) ON (n.{source_col})")
                session.run(f"CREATE INDEX IF NOT EXISTS FOR (n:Entity) ON (n.{target_col})")

                # Delete relationships first
                session.run("""
                    MATCH ()-[r]->()
                    WHERE r.session_id = $session_id AND type(r) = $relationship_type
                    DELETE r
                """, session_id=session_id, relationship_type=relationship_type)

                # Delete orphaned nodes next
                session.run("""
                    MATCH (n:Entity)
                    WHERE n.session_id = $session_id AND n.rel_type = $relationship_type
                    AND NOT (n)--()
                    DELETE n
                """, session_id=session_id, relationship_type=relationship_type)


                for i in range(0, total_rels, batch_size):
                    if stop_event and stop_event.is_set():
                        log_writer(log_file, f"[{datetime.now()}] [STOP] Relationship creation cancelled")
                        break

                    batch = rels[i:i + batch_size]

                    # Add session_id to each row
                    for r in batch:
                        r["session_id"] = session_id

                    # Extract the first row source for the batch (single source node)
                    batch_source = batch[0]["source"]
                    batch_source_props = batch[0]["props"]

                    # Only merge one source node, targets are separate
                    # Assume 'batch_source' is the source value for this batch
                    # and 'batch_source_props' is sanitized props for the source node

                    session.run(f"""
                        MERGE (a:Entity {{ 
                            {source_col}: $source, 
                            session_id: $session_id, 
                            rel_type: $relationship_type 
                        }})
                        ON CREATE SET a += $source_props
                        WITH a
                        UNWIND $rows AS row
                            CREATE (b:Entity {{ {target_col}: row.target, session_id: row.session_id, rel_type: $relationship_type }})
                            SET b += row.props
                            CREATE (a)-[rel:{relationship_type} {{ session_id: row.session_id }}]->(b)
                            SET rel.bgcolor = '#750b8c',
                                rel.textcolor = '#ffffff'
                    """, source=batch_source, source_props=batch_source_props, session_id=session_id, relationship_type=relationship_type, rows=batch)



                    log_writer(
                        log_file,
                        f"[{datetime.now()}] [Info] - Inserted relationship batch {i//batch_size + 1} ({len(batch)} rels)"
                    )





        if action == "Link Analysis":
            rule = payload.get("rule")
            rule_key = str(rule).strip().lower().replace(' ', '_') if rule else ""
            log_writer(log_file, f"[{datetime.now()}] [Info] - Preparing Link Analysis data")

            # Collect rows from dataframe
            rows = [r.asDict() for r in df.toLocalIterator()]
            total_rows = len(rows)
            log_writer(log_file, f"[{datetime.now()}] [Info] - {total_rows} rows collected for Link Analysis")

            if total_rows == 0:
                log_writer(log_file, f"[{datetime.now()}] [Info] - No rows to process for Link Analysis")
                return

            # Delete existing nodes for this session
            with driver.session() as session:
                session.run("""
                    MATCH (n)
                    WHERE n.batch_id STARTS WITH $session_id
                    DETACH DELETE n
                """, session_id=session_id)
                log_writer(log_file, f"[{datetime.now()}] [Info] - Existing Link Analysis nodes for session '{session_id}' deleted")

            # Prepare clean rows
            clean_rows = []
            for r in rows:
                row = {k: ("" if v is None else v) for k, v in r.items()}
                row.setdefault("NodeId", str(uuid.uuid4()))
                clean_rows.append(row)
            # Run rule-specific analysis on whatever was injected
            # ------------------------------------------------------------------------------------------------------------- Identifing rules to label nodes with 1
            #Stored/uploaded rules
            rules=load_temp_config("rule_file_names",session_id)        
            # Decide node label
            rule_name_first_part = rule_key.split('_')[0]
            node_label = f"{rule_name_first_part}_{session_id}"
            #Linking the node_label to the session
            if session_id in _session_store:
                _session_store[session_id]["node_label"] = node_label
            # Run rule-specific analysis on whatever was injected
            # ------------------------------------------------------------------------------------------------------------- Identifing rules to analyse with 2
            # Path to the directory containing rule files
            rules_dir = 'public/temp_rules/'
            rule_filename = f"{rule_key}_rules.py"
            rule_path = os.path.join(rules_dir, rule_filename)
            module, rule_status = check_rule_status(rule_key, rule_path) #Check the rule
            print(f"Loading rule from {rule_path}")  # debug
            # Insert nodes in batches; stop if stop_event is set
            early_analysis_ran = False

            with driver.session() as session:
                for i in range(0, len(clean_rows), batch_size):
                    if stop_event and stop_event.is_set():
                        log_writer(log_file, f"[{datetime.now()}] [STOP] Insertion stopped at batch {i//batch_size + 1}")
                        break

                    batch_number = i // batch_size + 1
                    batch = clean_rows[i:i + batch_size]

                    for row in batch:
                        row["batch_id"] = f"{session_id}_{batch_number}"
                        row["nodes_label"] = node_label

                    session.run("""
                        UNWIND $rows AS row
                        MERGE (n:Transactions { NodeId: row.NodeId })
                        SET n += row
                    """, rows=batch)
                    
                    # EARLY ANALYSIS (ONCE, AFTER FIRST BATCH)
                    if batch_number == 1 and module and not early_analysis_ran:
                        log_writer(
                            log_file,
                            f"[{datetime.now()}] [Info] Running Quick analysis ..."
                        )
                        try:
                            module.main(driver, session_id, node_label, log_file)
                            early_analysis_ran = True
                        except Exception as e:
                            log_writer(
                                log_file,
                                f"[{datetime.now()}] [Warning] Early analysis failed: {e}"
                            )                                        
                    log_writer(
                        log_file,
                        f"[{datetime.now()}] [Info] Inserted batch {batch_number} ({len(batch)} rows)"
                    )
            # ---------- FULL-GRAPH RECOMPUTATION ALWAYS RUNS ----------
            log_writer(log_file, f"[{datetime.now()}] [Info] - Starting full-graph recomputation for rule '{rule}'")

            set_session_status(driver, session_id, "READY_FOR_ANALYSIS")
            with driver.session() as session:
                locked = session.run("""
                    MATCH (s:Session {id:$id})
                    WHERE s.status IN ['READY_FOR_ANALYSIS']
                    SET s.status = 'ANALYZING'
                    RETURN s
                """, id=session_id).single()

                if not locked:
                    log_writer(log_file, f"[{datetime.now()}] [Info] - Analysis already running, skipping")
                    return
            if module:
                module.main(driver, session_id, node_label, log_file)
            else:
                print(rule_status)
                log_writer(log_file, f"[{datetime.now()}] [Warning] - {rule_status}")


            set_session_status(driver, session_id, "ANALYZED")
            log_writer(log_file, f"[{datetime.now()}] [Info] - Full-graph recomputation finished for rule '{rule}'")
    finally:
        driver.close()
        log_writer(log_file, f"[{datetime.now()}] [Info] - Injection finished for action '{action}'")


# ----------------- avoiding fragments if session stops unexpectedly -----------------
def recover_pending_sessions(driver):
    with driver.session() as session:
        result = session.run("""
            MATCH (s:Session)
            WHERE s.status IN ['INGESTING', 'READY_FOR_ANALYSIS' ,'ANALYZING']
            RETURN s.id AS session_id, s.rule AS rule
        """)
        return [(r["session_id"], r["rule"]) for r in result]


# ----------------- Analyzer -----------------

def analyzer(payload):
    print("analyzer called")
    session_id = payload.get("session_id")
    stop_event = payload.get("stop_event")
    dataframe_dir = payload.get("dataframe_dir")
    spark_conf = payload.get("spark_conf")

    # Determine if we should use Spark
    use_spark = True  # set True if you want Spark reading

    # Load the DataFrame (handles local/Windows paths safely)
    df = load_file(dataframe_dir, session_id, use_spark=use_spark)
    if df is None:
        print(f"[{session_id}] Failed to load DataFrame from: {dataframe_dir}")
        return

    print(f"[{session_id}] DataFrame loaded successfully: {df}")

    # ---------- Batch Data Processing ----------
    if payload.get("id") == "batch_data" and payload.get("type") == "new":
        print(1)
        if stop_event and stop_event.is_set():
            print(0)
            print(f"[{session_id}] Analyzer aborted early due to stop signal.")
            return

        if payload.get("tool") == "neo4j":
            print(2)
            driver = tools("neo4j", "check", {"session_id": session_id})
            pending = recover_pending_sessions(driver)
            for sid, rule in pending:
                if sid == session_id:
                    continue  # DO NOT analyze the active session

                log_writer(payload.get("log_file"), f"[Recovery] Analyzing session {sid}")
                set_session_status(driver, sid, "READY_FOR_ANALYSIS")

                if rule in ["bank", "bank transactions", "transactions"]:
                    batch_graph_analysis_transactions(driver, payload.get("log_file"))
                elif rule in ["social", "social media", "posts", "tweets"]:
                    batch_graph_analysis_posts(driver, payload.get("log_file"))

                set_session_status(driver, sid, "ANALYZED")

            if not driver:
                print(f"[{session_id}] Neo4j driver not found!")
                return

            try:
                params = {
                    "neo4j_conf": payload.get("tool_credentials"),
                    "id": payload.get("id"),
                    "session_id": session_id,
                    "dataframe": df,
                    "action": payload.get("action"),   # Store data / Source / Target / Link Analysis
                    "rule": payload.get("rule"),      # Social media / Bank Transactions
                    "source": payload.get("source"),
                    "target": payload.get("target"),
                    "relationship": payload.get("relationship"),
                    "log_file": payload.get("log_file"),
                    "stop_event": stop_event
                }
                neo4j_row_data_injector(params)
                print(f"[{session_id}] Batch analysis completed successfully.")
            except Exception as e:
                print(f"[{session_id}] Batch analysis failed: {e}")
                log_writer(payload.get("log_file"), f"[Error] Analyzing session {session_id} failed {e}")



    print(f"[{session_id}] Analyzer finished")
