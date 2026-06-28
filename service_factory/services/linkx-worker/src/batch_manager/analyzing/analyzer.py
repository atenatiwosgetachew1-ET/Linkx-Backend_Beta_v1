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
from logger import log_writer
from batch_manager.processing.file_source_loader import load_file
from batch_manager.utils.neo4j_utils import Neo4jCredentialConfigError, create_neo4j_driver, load_session_neo4j_credentials, redacted_neo4j_credentials
from batch_manager.utils.neo4j_cleanup import clean_existing_session
from batch_manager.utils.artifact_utils import ensure_artifact_dir
from batch_manager.processing.realtime_source_loader import records_to_dataframe, iter_kafka_messages, iter_api_messages
from batch_manager.processing.rules_compiler import normalize_rule_key
from batch_manager.analyzing import LA_rules_script
from globals import load_temp_config,_session_store

try:
    from service_orchestration import enqueue_cleanup_run
except Exception:
    enqueue_cleanup_run = None


global_iteration_thread = None
iteration_thread_registry = {}

def json_serializer(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()  # Convert datetime to ISO string
    raise TypeError(f"Type {type(obj)} not serializable")

def _is_retryable_neo4j_error(exc):
    code = getattr(exc, "code", None) or getattr(exc, "neo4j_code", None)
    text = f"{code or ''} {exc}"
    retry_markers = (
        "Neo.TransientError",
        "DeadlockDetected",
        "LockClientStopped",
        "DatabaseUnavailable",
        "NotALeader",
        "ForsetiClient",
    )
    return any(marker in text for marker in retry_markers)


def _neo4j_retry_delay(attempt):
    return min(10.0, 0.75 * (2 ** max(0, attempt - 1)))


def _stop_aware_sleep(stop_event, seconds):
    deadline = time.time() + max(0.0, float(seconds or 0))
    while time.time() < deadline:
        if stop_event and stop_event.is_set():
            return True
        time.sleep(min(0.25, deadline - time.time()))
    return bool(stop_event and stop_event.is_set())


def _cleanup_retry_run(params, exc):
    run_id = params.get("run_id")
    session_id = params.get("session_id")
    credentials = params.get("neo4j_conf")
    if not run_id or not credentials:
        return None
    driver = None
    try:
        driver = create_neo4j_driver(credentials)
        return clean_existing_session(driver, session_id, run_id=run_id)
    except Exception as cleanup_exc:
        return {"status": "cleanup_failed", "error": str(cleanup_exc), "trigger_error": str(exc)}
    finally:
        if driver:
            driver.close()


def _neo4j_inject_with_retry(params, max_attempts=4):
    log_file = params.get("log_file")
    session_id = params.get("session_id")
    stop_event = params.get("stop_event")
    attempt = 1
    while True:
        if stop_event and stop_event.is_set():
            return False
        try:
            neo4j_row_data_injector(params)
            return True
        except Exception as exc:
            if not _is_retryable_neo4j_error(exc) or attempt >= max_attempts:
                raise
            delay = _neo4j_retry_delay(attempt)
            cleanup_result = _cleanup_retry_run(params, exc)
            message = (
                f"[{datetime.now()}] [Warning] - Neo4j transient write error for session {session_id}; "
                f"cleaned partial run result={cleanup_result}; "
                f"retrying attempt {attempt + 1}/{max_attempts} after {delay:.1f}s: {exc}"
            )
            print(message)
            if log_file:
                log_writer(log_file, message)
            if _stop_aware_sleep(stop_event, delay):
                return False
            attempt += 1


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

def _parent_session_id(session_id):
    raw = str(session_id or "")
    if "_" not in raw:
        return None
    _, parent = raw.split("_", 1)
    return parent or None


def _graph_metadata(session_id, run_id=None, batch_id=None):
    metadata = {
        "session_id": str(session_id or ""),
        "created_by": "linkx",
        "linkx_managed": True,
        "created_at": datetime.utcnow().isoformat(),
    }
    parent_session_id = _parent_session_id(session_id)
    if parent_session_id:
        metadata["parent_session_id"] = parent_session_id
    if run_id:
        metadata["run_id"] = str(run_id)
    if batch_id:
        metadata["batch_id"] = str(batch_id)
    return metadata

def _stamp_graph_ownership(driver, session_id, run_id=None, batch_id=None):
    if not driver or not session_id:
        return {"nodes": 0, "relationships": 0}
    parent_session_id = _parent_session_id(session_id)
    batch_prefix = f"{session_id}_"
    with driver.session() as session:
        node_record = session.run(
            """
            MATCH (n)
            WHERE n.session_id = $session_id
               OR ($run_id IS NOT NULL AND n.run_id = $run_id)
               OR ($batch_id IS NOT NULL AND n.batch_id = $batch_id)
               OR coalesce(n.batch_id, '') STARTS WITH $batch_prefix
            SET n.created_by = coalesce(n.created_by, 'linkx'),
                n.linkx_managed = coalesce(n.linkx_managed, true),
                n.parent_session_id = coalesce(n.parent_session_id, $parent_session_id),
                n.run_id = coalesce(n.run_id, $run_id),
                n.ownership_stamped_at = datetime()
            RETURN count(n) AS count
            """,
            session_id=str(session_id),
            parent_session_id=parent_session_id,
            run_id=str(run_id) if run_id else None,
            batch_id=str(batch_id) if batch_id else None,
            batch_prefix=batch_prefix,
        ).single()
        rel_record = session.run(
            """
            MATCH ()-[r]->()
            WHERE r.session_id = $session_id
               OR ($run_id IS NOT NULL AND r.run_id = $run_id)
               OR ($batch_id IS NOT NULL AND r.batch_id = $batch_id)
               OR coalesce(r.batch_id, '') STARTS WITH $batch_prefix
            SET r.created_by = coalesce(r.created_by, 'linkx'),
                r.linkx_managed = coalesce(r.linkx_managed, true),
                r.parent_session_id = coalesce(r.parent_session_id, $parent_session_id),
                r.run_id = coalesce(r.run_id, $run_id),
                r.ownership_stamped_at = datetime()
            RETURN count(r) AS count
            """,
            session_id=str(session_id),
            parent_session_id=parent_session_id,
            run_id=str(run_id) if run_id else None,
            batch_id=str(batch_id) if batch_id else None,
            batch_prefix=batch_prefix,
        ).single()
    return {
        "nodes": int((node_record or {}).get("count") or 0),
        "relationships": int((rel_record or {}).get("count") or 0),
    }


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
                s.parent_session_id = coalesce($parent_session_id, s.parent_session_id),
                s.created_by = 'linkx',
                s.linkx_managed = true,
                s.updated_at = datetime()
        """, id=session_id, status=status, rule=rule, run_id=run_id, parent_session_id=_parent_session_id(session_id))

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


class _BuiltinRuleModule:
    def __init__(self, main_func, incremental_func):
        self._main_func = main_func
        self._incremental_func = incremental_func

    def main(self, driver, session_id, node_label, log_file):
        return self._main_func(driver, log_file, session_id=session_id, nodes_label=node_label)

    def incremental(self, driver, session_id, node_label, batch_id, log_file):
        return self._incremental_func(driver, session_id, node_label, batch_id, log_file)


def _builtin_rule_module(rule_key):
    mapping = {
        "bank_transactions": _BuiltinRuleModule(
            LA_rules_script.batch_graph_analysis_transactions,
            LA_rules_script.incremental_graph_analysis_transactions,
        ),
        "transactions": _BuiltinRuleModule(
            LA_rules_script.batch_graph_analysis_transactions,
            LA_rules_script.incremental_graph_analysis_transactions,
        ),
        "social_media_tweeter": _BuiltinRuleModule(
            LA_rules_script.batch_graph_analysis_posts,
            LA_rules_script.incremental_graph_analysis_posts,
        ),
        "social_media_(tweeter)": _BuiltinRuleModule(
            LA_rules_script.batch_graph_analysis_posts,
            LA_rules_script.incremental_graph_analysis_posts,
        ),
        "social_media": _BuiltinRuleModule(
            LA_rules_script.batch_graph_analysis_posts,
            LA_rules_script.incremental_graph_analysis_posts,
        ),
        "call_data_records": _BuiltinRuleModule(
            LA_rules_script.batch_graph_analysis_cdr,
            LA_rules_script.incremental_graph_analysis_cdr,
        ),
        "cdr": _BuiltinRuleModule(
            LA_rules_script.batch_graph_analysis_cdr,
            LA_rules_script.incremental_graph_analysis_cdr,
        ),
    }
    return mapping.get(rule_key)


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

    try:
        driver = create_neo4j_driver(tool_credentials)
    except Neo4jCredentialConfigError as exc:
        log_writer(log_file, f"[{datetime.now()}] [Error] - Invalid Neo4j credential configuration for graph write: {exc}")
        return
    set_session_status(driver, session_id, "INGESTING", run_id=run_id)
    try:
        log_writer(log_file, f"[{datetime.now()}] [Info] - Injection started for action '{action}'")

        if action == "Store data":
            log_writer(log_file, f"[{datetime.now()}] [Info] - Storing nodes in Neo4j batches of {batch_size}")

            def prepare_store_row(row):
                clean = _clean_neo4j_props(row)
                clean.setdefault("NodeId", str(uuid.uuid4()))
                clean.update(_graph_metadata(session_id, run_id=run_id))
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

            scanned_rows = 0
            skipped_blank = 0
            skipped_self = 0
            for row_batch in _iter_dataframe_row_batches(df, batch_size):
                for raw_row in row_batch:
                    scanned_rows += 1
                    source_value = _neo4j_property_value(raw_row.get(source_col))
                    target_value = _neo4j_property_value(raw_row.get(target_col))
                    if source_value == "" or target_value == "":
                        skipped_blank += 1
                        continue
                    if source_value == target_value:
                        skipped_self += 1
                        continue

                    row_dict = _relationship_node_props(_clean_neo4j_props(sanitize_props(raw_row)))
                    row_dict.update(_graph_metadata(session_id, run_id=run_id))
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
            log_writer(
                log_file,
                f"[{datetime.now()}] [Info] - Source/Target rows scanned={scanned_rows}, "
                f"unique_pairs={total_rels}, skipped_blank={skipped_blank}, skipped_self={skipped_self}"
            )
            log_writer(log_file, f"[{datetime.now()}] [Info] - {total_rels} weighted relationships collected")

            if session_id in _session_store:
                _session_store[session_id]["primary_rel_type"] = relationship_type

            with driver.session() as session:
                # Indexes
                session.run(f"CREATE INDEX IF NOT EXISTS FOR (n:Entity) ON (n.`{source_col}`)")
                session.run(f"CREATE INDEX IF NOT EXISTS FOR (n:Entity) ON (n.`{target_col}`)")
                try:
                    session.run(f"CREATE INDEX rel_{relationship_type}_session_id IF NOT EXISTS FOR ()-[r:{relationship_type}]-() ON (r.session_id)")
                    session.run(f"CREATE INDEX rel_{relationship_type}_run_id IF NOT EXISTS FOR ()-[r:{relationship_type}]-() ON (r.run_id)")
                except Exception as index_exc:
                    log_writer(log_file, f"[{datetime.now()}] [Warning] - Relationship index creation skipped: {index_exc}")

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
                for source_value in sorted(grouped_by_source.keys()):
                    source_rows = grouped_by_source[source_value]

                    for i in range(0, len(source_rows), batch_size):

                        if stop_event and stop_event.is_set():
                            log_writer(log_file, f"[{datetime.now()}] [STOP] Relationship creation cancelled")
                            break

                        batch = source_rows[i:i + batch_size]

                        for r in batch:
                            r.update(_graph_metadata(session_id, run_id=run_id))

                        batch_source_props = batch[0]["props"]

                        session.run(f"""
                            MERGE (a:Entity {{ 
                                `{source_col}`: $source,
                                node_identity: 'Source Node',
                                session_id: $session_id,
                                run_id: $run_id,
                                rel_type: $relationship_type
                            }})
                            SET a += $source_props
                            WITH a
                            UNWIND $rows AS row
                                MERGE (b:Entity {{
                                    `{target_col}`: row.target,
                                    node_identity: 'Target Node',
                                    session_id: row.session_id,
                                    run_id: row.run_id,
                                    parent_session_id: row.parent_session_id,
                                    created_by: 'linkx',
                                    linkx_managed: true,
                                    rel_type: $relationship_type
                                }})
                                SET b += row.props
                                CREATE (a)-[rel:{relationship_type} {{
                                    session_id: row.session_id,
                                    run_id: row.run_id,
                                    parent_session_id: row.parent_session_id,
                                    created_by: 'linkx',
                                    linkx_managed: true,
                                    weight: row.weight
                                }}]->(b)
                                SET rel.bgcolor = '#750b8c',
                                    rel.textcolor = '#ffffff'
                        """,
                        source=source_value,
                        source_props=batch_source_props,
                        session_id=session_id,
                        run_id=run_id,
                        parent_session_id=_parent_session_id(session_id),
                        relationship_type=relationship_type,
                        rows=batch)

                        log_writer(
                            log_file,
                            f"[{datetime.now()}] [Info] - Inserted weighted relationship batch "
                            f"{i//batch_size + 1} for source '{source_value}' ({len(batch)} rels)"
                        )
        if action == "Link Analysis":
            rule = payload.get("rule")
            rule_key, module, rule_status = _load_rule_module(rule, session_id)
            log_writer(log_file, f"[{datetime.now()}] [Info] - Preparing Link Analysis data in Neo4j batches of {batch_size}")
            if module:
                log_writer(log_file, f"[{datetime.now()}] [Info] - Link Analysis rule resolved: {rule_key} ({rule_status})")

            total_rows = 0
            batches_inserted = 0
            node_label = f"{rule_key}_{session_id}" if rule_key else f"link_analysis_{session_id}"
            if session_id in _session_store:
                _session_store[session_id]["node_label"] = node_label
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
                        row.update(_graph_metadata(session_id, run_id=run_id, batch_id=batch_id))
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
        try:
            ownership = _stamp_graph_ownership(driver, session_id, run_id=run_id)
            log_writer(log_file, f"[{datetime.now()}] [Info] - Ownership metadata stamped: {ownership}")
        except Exception as exc:
            log_writer(log_file, f"[{datetime.now()}] [Warning] - Ownership metadata stamp failed: {exc}")
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


def _parent_session_id(session_id):
    raw = str(session_id or "")
    if "_" not in raw:
        return None
    _, parent = raw.split("_", 1)
    return parent or None


def _first_config_value(value):
    if isinstance(value, (list, tuple)):
        return value[0] if value else None
    return value


def _resolve_rule_name(rule, session_id):
    if rule:
        return rule
    active_rule = _first_config_value(load_temp_config("active_rule", session_id))
    if active_rule:
        return active_rule
    rule_names = load_temp_config("rule_names", session_id)
    return _first_config_value(rule_names)


def _load_rule_module(rule, session_id):
    resolved_rule = _resolve_rule_name(rule, session_id)
    rule_key = normalize_rule_key(resolved_rule) if resolved_rule else ""
    if not rule_key:
        return "", None, "No active rule selected."

    rules_dir = ensure_artifact_dir("rules")
    rule_filename = f"{rule_key}_rules.py"
    candidate_paths = [
        os.path.join(rules_dir, str(session_id), rule_filename),
    ]
    parent_session_id = _parent_session_id(session_id)
    if parent_session_id:
        candidate_paths.append(os.path.join(rules_dir, parent_session_id, rule_filename))
    candidate_paths.extend([
        os.path.join(rules_dir, rule_filename),
        os.path.join("public", "temp_rules", rule_filename),
    ])

    for rule_path in candidate_paths:
        if os.path.exists(rule_path):
            module, rule_status = check_rule_status(rule_key, rule_path)
            return rule_key, module, rule_status

    builtin_module = _builtin_rule_module(rule_key)
    if builtin_module:
        return rule_key, builtin_module, "Using built-in rule module."

    return rule_key, None, f"No rule file found. Checked: {candidate_paths}"


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

    try:
        driver = create_neo4j_driver(tool_credentials)
    except Neo4jCredentialConfigError as exc:
        log_writer(log_file, f"[{datetime.now()}] [Error] - Invalid Neo4j realtime credential configuration: {exc}")
        return
    batch_id = f"{session_id}_rt_{batch_number}"
    try:
        set_session_status(driver, session_id, "INGESTING", rule=rule, run_id=run_id)
        clean_rows = []
        for row in rows:
            clean = _clean_neo4j_props(row)
            clean.setdefault("NodeId", str(uuid.uuid4()))
            clean.update(_graph_metadata(session_id, run_id=run_id, batch_id=batch_id))
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
                        CREATE (a)-[rel:{relationship_type} {{session_id: $session_id, run_id: $run_id, parent_session_id: $parent_session_id, batch_id: $batch_id, created_by: 'linkx', linkx_managed: true, weight: 1}}]->(b)
                    """, rows=relationship_rows, session_id=session_id, run_id=run_id, parent_session_id=_parent_session_id(session_id), batch_id=batch_id)
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
        try:
            ownership = _stamp_graph_ownership(driver, session_id, run_id=run_id, batch_id=batch_id)
            log_writer(log_file, f"[{datetime.now()}] [Info] - Realtime ownership metadata stamped: {ownership}")
        except Exception as exc:
            log_writer(log_file, f"[{datetime.now()}] [Warning] - Realtime ownership metadata stamp failed: {exc}")
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
    log_writer(log_file, f"[{datetime.now()}] [Info] - Realtime Neo4j credential source: {redacted_neo4j_credentials(payload.get('tool_credentials'))}")

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
    dataframe_id = payload.get("dataframe_id")
    expected_rows = payload.get("expected_dataframe_rows")
    spark_conf = payload.get("spark_conf")

    # Spark is opt-in; API nodes in the split deployment do not require Java.
    use_spark = bool(payload.get("use_spark", False))

    print(f"[{session_id}] Loading dataframe id={dataframe_id} path={dataframe_dir} use_spark={use_spark} expected_rows={expected_rows}")
    df = load_file(dataframe_dir, session_id, use_spark=use_spark)
    if df is None:
        print(f"[{session_id}] Failed to load DataFrame from: {dataframe_dir}")
        return False

    print(f"[{session_id}] DataFrame loaded successfully: {df}")
    actual_rows = None
    try:
        actual_rows = df.count() if hasattr(df, "count") and "pyspark" in str(type(df)).lower() else len(df)
    except Exception as exc:
        print(f"[{session_id}] Unable to count loaded DataFrame rows: {exc}")
    print(f"[{session_id}] Loaded dataframe rows actual={actual_rows} expected={expected_rows} id={dataframe_id} path={dataframe_dir}")
    try:
        expected_int = int(expected_rows) if expected_rows not in (None, "", "None") else None
    except (TypeError, ValueError):
        expected_int = None
    if expected_int is not None and actual_rows is not None and int(actual_rows) != expected_int:
        print(f"[{session_id}] DataFrame row count mismatch: actual={actual_rows} expected={expected_int}")
        return False

    # ---------- Batch Data Processing ----------
    if payload.get("id") == "batch_data" and payload.get("type") == "new":
        print(1)
        if stop_event and stop_event.is_set():
            print(0)
            print(f"[{session_id}] Analyzer aborted early due to stop signal.")
            return

        if payload.get("tool") == "neo4j":
            print(2)
            driver = None
            tool_credentials = payload.get("tool_credentials")
            if tool_credentials:
                try:
                    driver = create_neo4j_driver(tool_credentials)
                    with driver.session() as session:
                        session.run("RETURN 1 AS ok").consume()
                except Neo4jCredentialConfigError as exc:
                    log_writer(payload.get("log_file"), f"[{datetime.now()}] [Error] - Invalid Neo4j credential configuration: {exc}")
                    return False
                except Exception as exc:
                    print(f"[{session_id}] Payload Neo4j credential check failed creds={redacted_neo4j_credentials(tool_credentials)} error={exc}")
                    log_writer(payload.get("log_file"), f"[{datetime.now()}] [Error] - Neo4j connection verification failed: {exc}")
                    try:
                        if driver:
                            driver.close()
                    finally:
                        driver = None
                    return False
            else:
                try:
                    tool_credentials = load_session_neo4j_credentials(session_id, purpose="batch_analysis")
                    driver = create_neo4j_driver(tool_credentials)
                    with driver.session() as session:
                        session.run("RETURN 1 AS ok").consume()
                    payload["tool_credentials"] = tool_credentials
                except Neo4jCredentialConfigError as exc:
                    log_writer(payload.get("log_file"), f"[{datetime.now()}] [Error] - Invalid Neo4j credential configuration: {exc}")
                    return False
                except Exception as exc:
                    print(f"[{session_id}] Session Neo4j credential check failed error={exc}")
                    log_writer(payload.get("log_file"), f"[{datetime.now()}] [Error] - Neo4j connection verification failed: {exc}")
                    try:
                        if driver:
                            driver.close()
                    finally:
                        driver = None
                    return False
            if not driver:
                print(f"[{session_id}] Neo4j driver not found after credential resolution")
                log_writer(payload.get("log_file"), f"[{datetime.now()}] [Error] - Neo4j driver not found after credential resolution")
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
                if not _neo4j_inject_with_retry(params):
                    print(f"[{session_id}] Batch analysis cancelled before completion.")
                    return False
                print(f"[{session_id}] Batch analysis completed successfully.")
                return True
            except Exception as e:
                print(f"[{session_id}] Batch analysis failed: {e}")
                log_writer(payload.get("log_file"), f"[Error] Analyzing session {session_id} failed {e}")
                if enqueue_cleanup_run and payload.get("run_id"):
                    try:
                        cleanup_id = enqueue_cleanup_run(
                            "run",
                            session_id=session_id,
                            run_id=payload.get("run_id"),
                            reason="analysis_failed",
                            neo4j_credentials=payload.get("tool_credentials"),
                            payload={"event": "analysis_failed", "error": str(e)},
                        )
                        log_writer(payload.get("log_file"), f"[Info] Cleanup queued for failed run {payload.get('run_id')}: {cleanup_id}")
                    except Exception as cleanup_exc:
                        log_writer(payload.get("log_file"), f"[Warning] Failed to queue cleanup for failed run: {cleanup_exc}")
                return False



    print(f"[{session_id}] Analyzer finished")
