from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
from kafka.admin import KafkaAdminClient
from neo4j import GraphDatabase
import threading
import json
from datetime import datetime
import time
import uuid
import globals

from globals import create_file, save_temp_config, load_temp_config
from connection_utils import tools
from logger import log_writer
from LA_rules_script import batch_graph_analysis_posts,batch_graph_analysis_transactions

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

def neo4j_row_data_injector(payload, batch_size=500):
    driver = payload.get("driver")
    df = payload.get("dataframe")
    log_file = payload.get("log_file")
    action = payload.get("action")
    stop_event = payload.get("stop_event")

    if not driver or df is None:
        log_writer(log_file, f"{datetime.now()} [Error] - Missing driver or dataframe")
        return

    if payload.get("stop_event") and payload["stop_event"].is_set():
        driver.close()
        log_writer(log_file, f"[{datetime.now()}] [Stop signal received — terminating.]")
        return

    log_writer(log_file, f"[{datetime.now()}] [Info] - Injection started for action '{action}'")

    if action == "Store data":
        # Convert Spark rows to Python dicts
        batch_rows = [row.asDict() for row in df.rdd.toLocalIterator()]

        if not batch_rows:
            log_writer(log_file, f"{datetime.now()} [Info] - No rows to store.")
            return

        clean_rows = []
        for r in batch_rows:
            # Replace None values with empty string (Neo4j can't MERGE null)
            row = {k: ("" if v is None else v) for k, v in r.items()}

            # Assign unique ID to each row/node
            row["NodeId"] = str(uuid.uuid4())

            clean_rows.append(row)

        # Batch insert function
        def chunk_list(lst, size):
            for i in range(0, len(lst), size):
                yield lst[i:i + size]

        total = len(clean_rows)
        inserted = 0

        with driver.session() as session:
            for batch in chunk_list(clean_rows, batch_size):
                if stop_event.is_set():
                    driver.close()
                    log_writer(log_file, f"[{datetime.now()}] [STOP] Cancelled during batch insert.")
                    break
                try:
                    session.run("""
                        UNWIND $rows AS row
                        CREATE (n:Entity)
                        SET n = row
                    """, rows=batch)

                    inserted += len(batch)
                    if inserted==total:
                        log_writer(log_file,
                            f"{datetime.now()} [Info] - All ({total}) nodes are Stored."
                        )
                    else:
                        log_writer(log_file,
                            f"{datetime.now()} [Info] - Stored {inserted}/{total} nodes"
                        )
                except Exception as e:
                    log_writer(log_file,
                        f"{datetime.now()} [Error] - Failed node batch insert: {e}"
                    )

        log_writer(log_file,
            f"{datetime.now()} [Info] Completed storing {inserted} nodes."
        )
    elif action == "Source / Target Relationship":
        source_col = payload.get("source")
        target_col = payload.get("target")
        relationship_type = payload.get("relationship")

        # Convert Spark DataFrame to list of dicts
        batch_rows = [row.asDict() for row in df.rdd.toLocalIterator()]

        # Remove self-loops
        batch_rows = [r for r in batch_rows if r[source_col] != r[target_col]]
        total_rows = len(batch_rows)

        if total_rows == 0:
            log_writer(log_file, f"[{datetime.now()}] [Info] - No valid rows to insert (all self-loops removed).")
            return

        log_writer(log_file, f"[{datetime.now()}] [Info] - Total rows to insert: {total_rows}")

        def chunks(lst, size):
            for i in range(0, len(lst), size):
                yield lst[i:i + size]

        processed_rows = 0

        # Create indexes (run only once but safe to call)
        try:
            with driver.session() as session:
                session.run(f"CREATE INDEX IF NOT EXISTS FOR (n:Entity) ON (n.{source_col})")
                session.run(f"CREATE INDEX IF NOT EXISTS FOR (n:Entity) ON (n.{target_col})")
        except Exception:
            pass

        with driver.session() as session:
            for batch_idx, batch in enumerate(chunks(batch_rows, batch_size), start=1):
                if stop_event.is_set():
                    driver.close()
                    log_writer(log_file, f"[{datetime.now()}] [STOP] Relationship injection cancelled.")
                    break
                batch_params = [{"source": r[source_col], "target": r[target_col]} for r in batch]
                cypher_query = f"""
                UNWIND $rows AS row
                MERGE (a:Entity {{ {source_col}: row.source }})
                MERGE (b:Entity {{ {target_col}: row.target }})
                WITH a, b
                MATCH (x:Entity {{ {source_col}: a.{source_col} }}),
                      (y:Entity {{ {target_col}: b.{target_col} }})
                MERGE (x)-[:{relationship_type} {{ bgcolor: '#750b8c', color: '#FFF' }}]->(y)
                """

                try:
                    session.execute_write(lambda tx: tx.run(cypher_query, rows=batch_params))
                    processed_rows += len(batch)
                    log_writer(
                        log_file,
                        f"[{datetime.now()}] [Info] - Batch {batch_idx} inserted ({len(batch)} rows). Total processed: {processed_rows}/{total_rows}"
                    )
                    if total_rows==processed_rows:
                        log_writer(log_file, f"[{datetime.now()}] [Info] - Relationship injection completed. Total rows: {processed_rows}/{total_rows}")
                except Exception as e:
                    log_writer(log_file, f"[{datetime.now()}] [Error] - Failed to insert batch {batch_idx}: {e}")

    elif action == "Link Analysis":
        rule = payload.get("rule")
        rule_key = str(rule).strip().lower() if rule else ""
        # Convert Spark DF safely
        try:
            rows = df.toPandas().to_dict("records")
        except Exception:
            try:
                rows = [r.asDict() for r in df.rdd.toLocalIterator()]
            except Exception as e:
                log_writer(log_file, f"{datetime.now()} [Error] - Failed to collect rows: {e}")
                return

        total_rows = len(rows)
        log_writer(log_file, f"[{datetime.now()}] [Info] - Received {total_rows} rows for Link Analysis")

        if total_rows == 0:
            log_writer(log_file, f"[{datetime.now()}] [Warning] - No new rows received. Running analysis on existing Neo4j data")
        else:
            log_writer(log_file, f"[{datetime.now()}] [Info] - Storing and preparing a total of {total_rows} rows for analysis ...")

            # Deduplicate + normalize rows
            clean_rows = []
            inserted = 0
            for r in rows:
                # Replace None with empty string
                row = {k: ("" if v is None else v) for k, v in r.items()}
                # Assign unique NodeId if missing
                if "NodeId" not in row or not row["NodeId"]:
                    row["NodeId"] = str(uuid.uuid4())
                clean_rows.append(row)

            # Determine Neo4j label based on rule
            if rule_key in ["bank", "bank transactions", "transactions"]:
                node_label = "Transactions"
            elif rule_key in ["social", "social media", "posts", "tweets"]:
                node_label = "Tweet"
            else:
                node_label = "Entity"  # fallback

            # Batch insert with deduplication on NodeId
            def chunk_list(lst, size):
                for i in range(0, len(lst), size):
                    yield lst[i:i + size]

            with driver.session() as session:
                for batch_idx, batch in enumerate(chunk_list(clean_rows, batch_size), start=1):
                    if stop_event.is_set():
                        driver.close()
                        log_writer(log_file, f"[{datetime.now()}] [STOP] Link analysis cancelled.")
                        break
                    try:
                        session.run(f"""
                            UNWIND $rows AS row
                            MERGE (n:{node_label} {{NodeId: row.NodeId}})
                            SET n += row
                        """, rows=batch)
                        inserted += len(batch)
                    except Exception as e:
                        log_writer(log_file,
                            f"[{datetime.now()}] [Error] - Failed batch insert {batch_idx}: {e}"
                        )

            log_writer(log_file,
                f"[{datetime.now()}] [Info] - Now running analysis..."
            )

        # ---------------------------
        # Run the corresponding graph analysis
        # ---------------------------
        if rule_key in ["bank", "bank transactions", "transactions"]:
            log_writer(log_file, f"[{datetime.now()}] [Info] - Analyzing Bank transactions for flags ...")
            batch_graph_analysis_transactions(driver, log_file)

        elif rule_key in ["social", "social media", "posts", "tweets"]:
            log_writer(log_file, f"[{datetime.now()}] [Info] - Analyzing Social media activities for flags ...")
            batch_graph_analysis_posts(driver, log_file)

        else:
            log_writer(log_file, f"[{datetime.now()}] [Error] - Unknown rule '{rule}' for Link Analysis")




# ----------------- Analyzer -----------------

def analyzer(param):  # Only called with a thread
    payload = param
    session_id = payload.get("session_id")
    stop_event = payload.get("stop_event")

    if payload.get("id") == "batch_data" and payload.get("type") == "new":
        if stop_event and stop_event.is_set():
            print(f"[{session_id}] Analyzer aborted early due to stop signal.")
            return

        if payload.get("tool") == "Neo4j":
            driver = tools(payload["tool"].lower(), "check", {"session_id": session_id})
            if driver:
                try:
                    params = {
                        "driver": driver,
                        "id": payload["id"],
                        "session_id": session_id,
                        "dataframe": payload["SparkDataFrame"],
                        "action": payload["action"],   # Store data / Source / Target / Link Analysis
                        "rule": payload.get("rule"),  # Social media / Bank Transactions
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
            else:
                print(f"[{session_id}] Neo4j driver not found!")

    elif payload.get("id") == "fetch_data" and payload.get("type") == "relationships":
        driver = payload.get("driver")
        stop_event = payload.get("stop_event")
        session_id = payload.get("session_id")
        socketio = payload.get("socketio")
        last_hash = None

        def calc_hash(data):
            import hashlib, json
            return hashlib.md5(json.dumps(data, sort_keys=True).encode()).hexdigest()

        while not stop_event.is_set():
            try:
                with driver.session() as session:                    
                    result = session.run("""
                                MATCH ()-[r]->()
                                WITH type(r) AS rel_type, collect(r)[0] AS sample
                                RETURN rel_type AS relationship_type,
                                       elementId(sample) AS id,
                                       sample {.*} AS relProps
                            """)
                    relationships = []
                    seen_ids = set()
                    for record in result:
                        rel_id = record["id"]
                        if rel_id in seen_ids:
                            continue  # skip duplicates
                        seen_ids.add(rel_id)

                        rel_type = record["relationship_type"]
                        rel_props = record["relProps"]
                        caption = f"{rel_id} {rel_type} {rel_props.get('color', '#000')}"

                        relationships.append({
                            "id": rel_id,
                            "type": rel_type,
                            "color": rel_props.get("color", "#000"),
                            "bgcolor": rel_props.get("bgcolor", "#CCC"),
                            "caption": caption
                        })



                # dedupe using hash
                new_hash = calc_hash(relationships)
                if new_hash != last_hash:
                    last_hash = new_hash
                    # PUSH to socket listener
                    socketio.emit(
                        "relationships",
                        { "data": relationships, "session_id": session_id }
                    )

            except Exception as e:
                print("Neo4j stream error:", str(e))

            # sleep small — server controls frequency, not frontend
            time.sleep(1)  # adjust as needed
