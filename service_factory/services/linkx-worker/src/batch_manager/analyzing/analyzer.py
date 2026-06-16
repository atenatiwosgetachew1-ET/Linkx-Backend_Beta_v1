from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
from kafka.admin import KafkaAdminClient
import re
import json
from datetime import datetime
import time
import uuid
import os
import importlib.util
from connection_utils import tools
from logger import log_writer
from batch_manager.processing.file_source_loader import load_file
from batch_manager.utils.neo4j_utils import create_neo4j_driver
from batch_manager.utils.artifact_utils import ensure_artifact_dir
from batch_manager.processing.realtime_source_loader import records_to_dataframe, iter_kafka_messages, iter_api_messages
from globals import load_temp_config,_session_store


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
        driver = create_neo4j_driver(neo4j_conf)

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

def set_session_status(driver, session_id, status, rule=None, run_id=None):
    with driver.session() as session:
        session.run("""
            MERGE (s:Session {id: $id})
            SET s.status = $status,
                s.rule = coalesce($rule, s.rule),
                s.run_id = coalesce($run_id, s.run_id),
                s.updated_at = datetime()
        """, id=session_id, status=status, rule=rule, run_id=run_id)

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


def rule_to_node_label(rule, session_id):
    rule_key = str(rule or "").strip().lower().replace(' ', '_')
    return f"{rule_key}_{session_id}"


def run_incremental_rule(module, driver, session_id, node_label, batch_id, log_file):
    if not module:
        return None
    if hasattr(module, "incremental"):
        return module.incremental(driver, session_id, node_label, batch_id, log_file)
    if hasattr(module, "incremental_graph_analysis_transactions"):
        return module.incremental_graph_analysis_transactions(driver, session_id, node_label, batch_id, log_file)
    return None


def _spark_or_pandas_row_dict(row):
    if hasattr(row, "asDict"):
        return row.asDict(recursive=True)
    if isinstance(row, dict):
        return row
    return dict(row)


def _neo4j_property_value(value):
    if value is None:
        return ""
    if isinstance(value, float) and value != value:
        return ""
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (dict, list, tuple, set)):
        return json.dumps(value, default=str, sort_keys=True)
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _clean_neo4j_props(row):
    return {key: _neo4j_property_value(value) for key, value in row.items()}


def _relationship_node_props(row):
    props = dict(row)
    props.pop("NodeId", None)
    return props


def _iter_dataframe_row_batches(df, batch_size, transform=None):
    if hasattr(df, "to_dict"):
        iterator = df.to_dict(orient="records")
    else:
        iterator = df.toLocalIterator()

    batch = []
    for row in iterator:
        record = _spark_or_pandas_row_dict(row)
        if transform:
            record = transform(record)
        batch.append(record)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def neo4j_row_data_injector(payload, batch_size=500):
    tool_credentials = payload.get("neo4j_conf")
    df = payload.get("dataframe")
    log_file = payload.get("log_file")
    action = payload.get("action")
    stop_event = payload.get("stop_event")
    session_id = payload.get("session_id")
    run_id = payload.get("run_id")
    print("neo4j_row_data_injector_session_id:",session_id)
    if not tool_credentials or df is None:
        log_writer(log_file, f"{datetime.now()} [Error] - Missing Neo4j credentials or dataframe")
        return

    if stop_event and stop_event.is_set():
        log_writer(log_file, f"[{datetime.now()}] [Stop] Stop signal received — terminating injector")
        return

    driver = create_neo4j_driver(tool_credentials)
    set_session_status(driver, session_id, "INGESTING", run_id=run_id)
    try:
        log_writer(log_file, f"[{datetime.now()}] [Info] - Injection started for action '{action}'")

        if action == "Store data":
            log_writer(log_file, f"[{datetime.now()}] [Info] - Storing nodes in Neo4j batches of {batch_size}")

            def prepare_store_row(row):
                clean = _clean_neo4j_props(row)
                clean.setdefault("NodeId", str(uuid.uuid4()))
                clean["session_id"] = session_id
                clean["run_id"] = run_id
                return clean

            total_rows = 0
            with driver.session() as session:
                session.run("""
                    CREATE CONSTRAINT IF NOT EXISTS FOR (n:Entity) REQUIRE n.NodeId IS UNIQUE
                """)

                for batch_number, batch in enumerate(
                    _iter_dataframe_row_batches(df, batch_size, transform=prepare_store_row),
                    start=1,
                ):
                    if stop_event and stop_event.is_set():
                        log_writer(log_file, f"[{datetime.now()}] [STOP] Node insertion cancelled")
                        break

                    session.run("""
                        UNWIND $rows AS row
                        MERGE (n:Entity { NodeId: row.NodeId })
                        ON CREATE SET n.node_identity = 'Entity Node'
                        SET n += row
                    """, rows=batch)
                    total_rows += len(batch)

                    log_writer(
                        log_file,
                        f"[{datetime.now()}] [Info] - Inserted Neo4j batch {batch_number} ({len(batch)} rows)"
                    )
            set_session_status(driver, session_id, "READY_FOR_ANALYSIS", run_id=run_id)
            log_writer(log_file, f"[{datetime.now()}] [Info] - Node insertion completed successfully ({total_rows} rows)")
        if action == "Source / Target Relationship":
            source_col = payload.get("source")
            target_col = payload.get("target")
            relationship_type = payload.get("relationship")

            if not source_col or not target_col or not relationship_type:
                log_writer(log_file, f"[{datetime.now()}] [Error] - Source, target, and relationship are required")
                return

            # Sanitize the relationship label
            relationship_type = re.sub(r'[^a-zA-Z0-9_]', '_', relationship_type.strip())

            log_writer(log_file, f"[{datetime.now()}] [Info] - Creating weighted relationships")

            def sanitize_props(d):
                return {k.replace(" ", "_").replace(".", "_"): v for k, v in d.items() if v is not None}

            # Collect rows (NO dropDuplicates -> we need frequency). This path must
            # support both pandas and Spark dataframes, so filtering is done in Python.
            from collections import defaultdict

            rel_counter = defaultdict(lambda: {
                "source": None,
                "target": None,
                "props": None,
                "weight": 0
            })

            for row_batch in _iter_dataframe_row_batches(df, batch_size):
                for raw_row in row_batch:
                    source_value = _neo4j_property_value(raw_row.get(source_col))
                    target_value = _neo4j_property_value(raw_row.get(target_col))
                    if source_value == "" or target_value == "" or source_value == target_value:
                        continue

                    row_dict = _relationship_node_props(_clean_neo4j_props(sanitize_props(raw_row)))
                    key = (source_value, target_value)

                    if rel_counter[key]["weight"] == 0:
                        rel_counter[key]["source"] = source_value
                        rel_counter[key]["target"] = target_value
                        rel_counter[key]["props"] = row_dict

                    rel_counter[key]["weight"] += 1

            rels = []
            for v in rel_counter.values():
                rels.append({
                    "source": v["source"],
                    "target": v["target"],
                    "props": v["props"],
                    "weight": v["weight"]
                })

            total_rels = len(rels)
            log_writer(log_file, f"[{datetime.now()}] [Info] - {total_rels} weighted relationships collected")

            if session_id in _session_store:
                _session_store[session_id]["primary_rel_type"] = relationship_type

            with driver.session() as session:
                # Indexes
                session.run(f"CREATE INDEX IF NOT EXISTS FOR (n:Entity) ON (n.`{source_col}`)")
                session.run(f"CREATE INDEX IF NOT EXISTS FOR (n:Entity) ON (n.`{target_col}`)")

                # Delete old relationships
                session.run("""
                    MATCH ()-[r]->()
                    WHERE r.run_id = $run_id AND type(r) = $relationship_type
                    DELETE r
                """, run_id=run_id, relationship_type=relationship_type)

                # Delete orphan nodes
                session.run("""
                    MATCH (n:Entity)
                    WHERE n.run_id = $run_id AND n.rel_type = $relationship_type
                    AND NOT (n)--()
                    DELETE n
                """, run_id=run_id, relationship_type=relationship_type)

                # -------- BATCH INSERT --------
                from collections import defaultdict

                # ---- GROUP BY SOURCE FIRST ----
                grouped_by_source = defaultdict(list)

                for r in rels:
                    grouped_by_source[r["source"]].append(r)

                # ---- PROCESS EACH SOURCE SEPARATELY ----
                for source_value, source_rows in grouped_by_source.items():

                    for i in range(0, len(source_rows), batch_size):

                        if stop_event and stop_event.is_set():
                            log_writer(log_file, f"[{datetime.now()}] [STOP] Relationship creation cancelled")
                            break

                        batch = source_rows[i:i + batch_size]

                        for r in batch:
                            r["session_id"] = session_id
                            r["run_id"] = run_id

                        batch_source_props = batch[0]["props"]

                        session.run(f"""
                            MERGE (a:Entity {{ 
                                `{source_col}`: $source,
                                node_identity: 'Source Node',
                                session_id: $session_id,
                                run_id: $run_id,
                                rel_type: $relationship_type
                            }})
                            ON CREATE SET a += $source_props
                            WITH a
                            UNWIND $rows AS row
                                MERGE (b:Entity {{
                                    `{target_col}`: row.target,
                                    node_identity: 'Target Node',
                                    session_id: row.session_id,
                                    run_id: row.run_id,
                                    rel_type: $relationship_type
                                }})
                                SET b += row.props
                                CREATE (a)-[rel:{relationship_type} {{
                                    session_id: row.session_id,
                                    run_id: row.run_id,
                                    weight: row.weight
                                }}]->(b)
                                SET rel.bgcolor = '#750b8c',
                                    rel.textcolor = '#ffffff'
                        """,
                        source=source_value,
                        source_props=batch_source_props,
                        session_id=session_id,
                        run_id=run_id,
                        relationship_type=relationship_type,
                        rows=batch)

                        log_writer(
                            log_file,
                            f"[{datetime.now()}] [Info] - Inserted weighted relationship batch "
                            f"{i//batch_size + 1} for source '{source_value}' ({len(batch)} rels)"
                        )
        if action == "Link Analysis":
            rule = payload.get("rule")
            rule_key = str(rule).strip().lower().replace(' ', '_') if rule else ""
            log_writer(log_file, f"[{datetime.now()}] [Info] - Preparing Link Analysis data in Neo4j batches of {batch_size}")

            total_rows = 0
            batches_inserted = 0

            # Run rule-specific analysis on whatever was injected
            # ------------------------------------------------------------------------------------------------------------- Identifing rules to label nodes with 1
            #Stored/uploaded rules
            rules=load_temp_config("rule_file_names",session_id)        
            # Decide node label
            # rule_name_first_part = rule_key.split('_')[0]
            # node_label = f"{rule_name_first_part}_{session_id}"
            node_label = f"{rule_key}_{session_id}"
            #Linking the node_label to the session
            if session_id in _session_store:
                _session_store[session_id]["node_label"] = node_label
            # Run rule-specific analysis on whatever was injected
            # ------------------------------------------------------------------------------------------------------------- Identifing rules to analyse with 2
            # Path to the directory containing rule files
            rules_dir = ensure_artifact_dir("rules")
            rule_filename = f"{rule_key}_rules.py"
            session_rule_path = os.path.join(rules_dir, str(session_id), rule_filename)
            default_rule_path = os.path.join(rules_dir, rule_filename)
            legacy_default_rule_path = os.path.join("public", "temp_rules", rule_filename)
            rule_path = (
                session_rule_path
                if os.path.exists(session_rule_path)
                else default_rule_path
                if os.path.exists(default_rule_path)
                else legacy_default_rule_path
            )
            module, rule_status = check_rule_status(rule_key, rule_path) #Check the rule
            print(f"Loading rule from {rule_path}")  # debug
            # Insert nodes in batches; run cheap incremental rules after every batch.

            def prepare_link_row(row):
                clean = _clean_neo4j_props(row)
                clean.setdefault("NodeId", str(uuid.uuid4()))
                return clean

            with driver.session() as session:
                for batch_number, batch in enumerate(
                    _iter_dataframe_row_batches(df, batch_size, transform=prepare_link_row),
                    start=1,
                ):
                    if stop_event and stop_event.is_set():
                        log_writer(log_file, f"[{datetime.now()}] [STOP] Insertion stopped at batch {batch_number}")
                        break

                    batch_id = f"{session_id}_{batch_number}"
                    for row in batch:
                        row["session_id"] = session_id
                        row["run_id"] = run_id
                        row["batch_id"] = batch_id
                        row["nodes_label"] = node_label

                    query = f"""
                        UNWIND $rows AS row
                        MERGE (n:`{node_label}` {{ NodeId: row.NodeId }})
                        ON CREATE SET n.node_identity = 'Entity Node'
                        SET n += row
                        """
                    session.run(query, rows=batch)
                    total_rows += len(batch)
                    batches_inserted = batch_number

                    if module:
                        try:
                            live_counts = run_incremental_rule(module, driver, session_id, node_label, batch_id, log_file)
                            if live_counts is not None:
                                if session_id in _session_store:
                                    _session_store[session_id]["live_analysis"] = {
                                        "batch_id": batch_id,
                                        "batch_number": batch_number,
                                        "total_batches": None,
                                        "flags": live_counts,
                                        "provisional": True,
                                    }
                                log_writer(
                                    log_file,
                                    f"[{datetime.now()}] [Info] Live analysis batch {batch_number} flags: {live_counts}"
                                )
                        except Exception as e:
                            log_writer(
                                log_file,
                                f"[{datetime.now()}] [Warning] Incremental analysis failed for batch {batch_number}: {e}"
                            )

                    log_writer(
                        log_file,
                        f"[{datetime.now()}] [Info] Inserted Neo4j batch {batch_number} ({len(batch)} rows)"
                    )

            if total_rows == 0:
                log_writer(log_file, f"[{datetime.now()}] [Info] - No rows to process for Link Analysis")
                return
            # ---------- FULL-GRAPH RECOMPUTATION ALWAYS RUNS ----------
            log_writer(log_file, f"[{datetime.now()}] [Info] - Starting full-graph recomputation for rule '{rule}'")

            set_session_status(driver, session_id, "READY_FOR_ANALYSIS", run_id=run_id)
            with driver.session() as session:
                locked = session.run("""
                    MATCH (s:Session {id:$id})
                    WHERE s.status IN ['READY_FOR_ANALYSIS']
                      AND ($run_id IS NULL OR s.run_id = $run_id)
                    SET s.status = 'ANALYZING'
                    RETURN s
                """, id=session_id, run_id=run_id).single()

                if not locked:
                    log_writer(log_file, f"[{datetime.now()}] [Info] - Analysis already running, skipping")
                    return
            if module:
                final_counts = module.main(driver, session_id, node_label, log_file)
                if session_id in _session_store and final_counts is not None:
                    _session_store[session_id]["live_analysis"] = {
                        "batch_id": None,
                        "batch_number": None,
                        "total_batches": batches_inserted,
                        "flags": final_counts,
                        "provisional": False,
                    }
            else:
                print(rule_status)
                log_writer(log_file, f"[{datetime.now()}] [Warning] - {rule_status}")


            set_session_status(driver, session_id, "ANALYZED", run_id=run_id)
            log_writer(log_file, f"[{datetime.now()}] [Info] - Full-graph recomputation finished for rule '{rule}'")
    finally:
        driver.close()
        log_writer(log_file, f"[{datetime.now()}] [Info] - Injection finished for action '{action}'")


def _dataframe_to_row_dicts(df):
    if df is None:
        return []
    if hasattr(df, "to_dict"):
        return df.to_dict(orient="records")
    if hasattr(df, "toLocalIterator"):
        return [row.asDict(recursive=True) for row in df.toLocalIterator()]
    return []


def _load_rule_module(rule, session_id):
    rule_key = str(rule).strip().lower().replace(' ', '_') if rule else ""
    rules_dir = ensure_artifact_dir("rules")
    rule_filename = f"{rule_key}_rules.py"
    session_rule_path = os.path.join(rules_dir, str(session_id), rule_filename)
    default_rule_path = os.path.join(rules_dir, rule_filename)
    legacy_default_rule_path = os.path.join("public", "temp_rules", rule_filename)
    rule_path = (
        session_rule_path
        if os.path.exists(session_rule_path)
        else default_rule_path
        if os.path.exists(default_rule_path)
        else legacy_default_rule_path
    )
    module, rule_status = check_rule_status(rule_key, rule_path)
    return rule_key, module, rule_status


def realtime_neo4j_message_ingest(payload, df, batch_number):
    session_id = payload.get("session_id")
    run_id = payload.get("run_id")
    log_file = payload.get("log_file")
    action = payload.get("action") or "Link Analysis"
    rule = payload.get("rule")
    stop_event = payload.get("stop_event")
    tool_credentials = payload.get("tool_credentials")
    rows = _dataframe_to_row_dicts(df)

    if not rows:
        log_writer(log_file, f"[{datetime.now()}] [Info] - Realtime message normalized to 0 rows; skipping")
        return
    if not tool_credentials:
        log_writer(log_file, f"[{datetime.now()}] [Error] - Missing Neo4j credentials for realtime ingestion")
        return
    if stop_event and stop_event.is_set():
        return

    driver = create_neo4j_driver(tool_credentials)
    batch_id = f"{session_id}_rt_{batch_number}"
    try:
        set_session_status(driver, session_id, "INGESTING", rule=rule, run_id=run_id)
        clean_rows = []
        for row in rows:
            clean = _clean_neo4j_props(row)
            clean.setdefault("NodeId", str(uuid.uuid4()))
            clean["session_id"] = session_id
            clean["run_id"] = run_id
            clean["batch_id"] = batch_id
            clean_rows.append(clean)

        if action == "Source / Target Relationship":
            source_col = payload.get("source")
            target_col = payload.get("target")
            relationship_type = payload.get("relationship") or "HAS_RELATIONSHIP"
            relationship_type = re.sub(r'[^a-zA-Z0-9_]', '_', relationship_type.strip())
            relationship_rows = [
                {
                    "source": row.get(source_col),
                    "target": row.get(target_col),
                    "props": _relationship_node_props(row),
                }
                for row in clean_rows
                if source_col and target_col and row.get(source_col) and row.get(target_col)
            ]
            if relationship_rows:
                with driver.session() as session:
                    session.run(f"""
                        UNWIND $rows AS row
                        MERGE (a:Entity {{`{source_col}`: row.source, session_id: $session_id, run_id: $run_id, node_identity: 'Source Node'}})
                        SET a += row.props
                        MERGE (b:Entity {{`{target_col}`: row.target, session_id: $session_id, run_id: $run_id, node_identity: 'Target Node'}})
                        SET b += row.props
                        CREATE (a)-[rel:{relationship_type} {{session_id: $session_id, run_id: $run_id, batch_id: $batch_id, weight: 1}}]->(b)
                    """, rows=relationship_rows, session_id=session_id, run_id=run_id, batch_id=batch_id)
            log_writer(log_file, f"[{datetime.now()}] [Info] - Realtime relationship batch {batch_id} ingested ({len(relationship_rows)} rows)")
            return

        if action == "Store data":
            with driver.session() as session:
                session.run("""
                    UNWIND $rows AS row
                    MERGE (n:Entity { NodeId: row.NodeId })
                    ON CREATE SET n.node_identity = 'Entity Node'
                    SET n += row
                """, rows=clean_rows)
            log_writer(log_file, f"[{datetime.now()}] [Info] - Realtime storage batch {batch_id} ingested ({len(clean_rows)} rows)")
            return

        rule_key, module, rule_status = _load_rule_module(rule, session_id)
        node_label = f"{rule_key}_{session_id}"
        for row in clean_rows:
            row["nodes_label"] = node_label

        if session_id in _session_store:
            _session_store[session_id]["node_label"] = node_label

        with driver.session() as session:
            query = f"""
                UNWIND $rows AS row
                MERGE (n:`{node_label}` {{ NodeId: row.NodeId }})
                ON CREATE SET n.node_identity = 'Entity Node'
                SET n += row
            """
            session.run(query, rows=clean_rows)

        if module:
            live_counts = run_incremental_rule(module, driver, session_id, node_label, batch_id, log_file)
            if session_id in _session_store and live_counts is not None:
                _session_store[session_id]["live_analysis"] = {
                    "batch_id": batch_id,
                    "batch_number": batch_number,
                    "total_batches": None,
                    "flags": live_counts,
                    "provisional": True,
                }
            log_writer(log_file, f"[{datetime.now()}] [Info] - Realtime incremental analysis for {batch_id}: {live_counts}")
        else:
            log_writer(log_file, f"[{datetime.now()}] [Warning] - {rule_status}")
    finally:
        driver.close()


def realtime_analyzer(payload):
    session_id = payload.get("session_id")
    source_type = payload.get("source_type")
    stop_event = payload.get("stop_event")
    log_file = payload.get("log_file")
    log_writer(log_file, f"[{datetime.now()}] [Info] - Realtime listener starting for {source_type}")

    if payload.get("tool") != "neo4j":
        log_writer(log_file, f"[{datetime.now()}] [Error] - Realtime ingestion currently requires Neo4j tool integration")
        return

    if not payload.get("tool_credentials"):
        log_writer(log_file, f"[{datetime.now()}] [Error] - Neo4j credentials not found")
        return

    if source_type == "kafka":
        broker_url = payload.get("broker_url")
        topic = payload.get("topic")
        if not broker_url or not topic:
            log_writer(log_file, f"[{datetime.now()}] [Error] - Missing Kafka broker or topic for realtime listener")
            return

        def message_iterator():
            return iter_kafka_messages(broker_url, topic, stop_event=stop_event)

    elif source_type == "api":
        api_url = payload.get("api_url")
        if not api_url:
            log_writer(log_file, f"[{datetime.now()}] [Error] - Missing API URL for realtime listener")
            return

        def message_iterator():
            return iter_api_messages(api_url, stop_event=stop_event, interval_seconds=payload.get("api_poll_interval", 5))

    else:
        log_writer(log_file, f"[{datetime.now()}] [Error] - Unsupported realtime source type: {source_type}")
        return

    batch_number = 0
    try:
        while not (stop_event and stop_event.is_set()):
            try:
                for message in message_iterator():
                    if stop_event and stop_event.is_set():
                        break
                    batch_number += 1
                    try:
                        df = records_to_dataframe(message, capitalized_keys_only=(source_type == "kafka"))
                        realtime_neo4j_message_ingest(payload, df, batch_number)
                    except Exception as e:
                        log_writer(log_file, f"[{datetime.now()}] [Error] - Realtime message processing failed: {e}")

                if stop_event and stop_event.is_set():
                    break

                log_writer(log_file, f"[{datetime.now()}] [Warning] - Realtime {source_type} listener ended; reconnecting")
            except Exception as e:
                if stop_event and stop_event.is_set():
                    break
                log_writer(log_file, f"[{datetime.now()}] [Warning] - Realtime {source_type} listener error; reconnecting: {e}")

            if stop_event and stop_event.wait(2):
                break
    finally:
        log_writer(log_file, f"[{datetime.now()}] [Info] - Realtime listener stopped for {source_type}")


# ----------------- Analyzer -----------------

def analyzer(payload):
    print("analyzer called")
    if payload.get("id") == "realtime_data":
        realtime_analyzer(payload)
        return True
    session_id = payload.get("session_id")
    stop_event = payload.get("stop_event")
    dataframe_dir = payload.get("dataframe_dir")
    spark_conf = payload.get("spark_conf")

    # Spark is opt-in; API nodes in the split deployment do not require Java.
    use_spark = bool(payload.get("use_spark", False))

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
            if not driver:
                print(f"[{session_id}] Neo4j driver not found!")
                log_writer(payload.get("log_file"), f"[{datetime.now()}] [Error] - Neo4j driver not found")
                return False

            driver.close()

            try:
                params = {
                    "neo4j_conf": payload.get("tool_credentials"),
                    "id": payload.get("id"),
                    "session_id": session_id,
                    "run_id": payload.get("run_id"),
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
                return True
            except Exception as e:
                print(f"[{session_id}] Batch analysis failed: {e}")
                log_writer(payload.get("log_file"), f"[Error] Analyzing session {session_id} failed {e}")
                return False



    print(f"[{session_id}] Analyzer finished")
