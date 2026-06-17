import os
import shutil

import pandas as pd
from flask import jsonify

from batch_manager.batch_data_manager import batch_data_manager
from batch_manager.processing.merger import merge_pandas_and_save, merge_spark_and_save
from batch_manager.utils.schema_utils import align_schemas
from batch_manager.utils.artifact_utils import ensure_artifact_dir, register_artifact_dir
from globals import load_temp_config, save_temp_config


def _is_spark_df(df):
    return "pyspark.sql.dataframe.DataFrame" in str(type(df))


def _append_dataframe(dfs, df, label="dataframe"):
    if df is None:
        print(f"{label} returned None, skipping")
        return
    if isinstance(df, dict) and "df" in df:
        dfs.append(df["df"])
    elif hasattr(df, "columns") or isinstance(df, pd.DataFrame) or _is_spark_df(df):
        dfs.append(df)
    else:
        print(f"Skipping invalid object returned for {label}:", df)


def load_dataframes_for_create_df(data, session_id):
    files = data.get("value", [])
    address_value = files if isinstance(files, str) else None
    date = data.get("date", None)
    kind = data.get("kind", "")
    if kind == "hdfs":
        kind = "hybrid"
    df_type = data.get("type")
    topic = data.get("topic") or data.get("kafka_topic")
    if kind == "address" and df_type == "api" and topic:
        df_type = "broker"
    use_spark = True if kind.lower() == "spark" else False
    dfs = []

    if (not files or files == []) and kind != "address":
        active_source_type = load_temp_config("active_source_type", session_id)
        if active_source_type in {"broker", "kafka", "api"}:
            kind = "address"
            df_type = "broker" if active_source_type in {"broker", "kafka"} else "api"
            address_value = (
                load_temp_config("active_kafka_adress", session_id)
                if df_type == "broker"
                else load_temp_config("active_REST_API", session_id)
            )
            topic = load_temp_config("active_kafka_topic", session_id) if df_type == "broker" else topic
            files = address_value or []

    if kind == "files":
        for file_value in files:
            payload = {
                "id": "load_sourceData",
                "session_id": session_id,
                "path": file_value,
                "use_spark": use_spark,
                "type": df_type,
                "kind": kind,
            }
            print("create_df_payload", payload)
            try:
                _append_dataframe(dfs, batch_data_manager(payload), f"file {file_value}")
            except Exception as e:
                print(f"Error loading file {file_value}: {e}")
        return dfs

    if kind == "address":
        payload = {
            "id": "load_sourceData",
            "session_id": session_id,
            "files": files,
            "address": data.get("address") or data.get("broker_url") or address_value,
            "broker": data.get("broker") or data.get("broker_url") or (address_value if df_type in {"broker", "kafka"} else None),
            "topic": topic,
            "max_messages": data.get("max_messages") or data.get("limit"),
            "from_beginning": data.get("from_beginning", False),
            "date": date,
            "use_spark": use_spark,
            "type": df_type,
            "kind": kind,
        }
        print("create_df_payload:", payload)
        try:
            _append_dataframe(dfs, batch_data_manager(payload), "address")
        except Exception as e:
            print(f"Error loading address response: {e}")
        return dfs

    payload = {
        "id": "load_sourceData",
        "session_id": session_id,
        "files": files,
        "date": date,
        "use_spark": use_spark,
        "type": df_type,
        "kind": kind,
    }
    print("create_df_payload:", payload)
    df = batch_data_manager(payload)
    if df is None:
        return []
    if isinstance(df, list):
        for item in df:
            _append_dataframe(dfs, item, "bulk item")
    else:
        _append_dataframe(dfs, df, "bulk dataframe")
    return dfs


def create_dataframe_response(data, session_id):
    dfs = load_dataframes_for_create_df(data, session_id)
    if not dfs:
        return jsonify({"results": "", "message": "No valid dataframes loaded"}), 400

    try:
        all_columns = set()
        for df in dfs:
            all_columns.update(df.columns)
        all_columns = list(all_columns)
        aligned = [align_schemas(df, all_columns) for df in dfs]

        pandas_dfs = [df for df in aligned if isinstance(df, pd.DataFrame)]
        spark_dfs = [df for df in aligned if _is_spark_df(df)]

        path_to_save = ensure_artifact_dir("dfparts")
        folder_name = f"merged_dfpart_{session_id}"
        target_folder = os.path.join(path_to_save, folder_name)
        if os.path.exists(target_folder):
            try:
                shutil.rmtree(target_folder)
                print("Deleted old folder:", target_folder)
            except Exception as e:
                print("Failed deleting session folder:", e)

        if pandas_dfs:
            merged_pandas = merge_pandas_and_save(pandas_dfs, path_to_save, session_id)
            if merged_pandas is None:
                return jsonify({"results": "", "message": "Failed to merge pandas DFs!"}), 400
            num_rows_pandas = len(merged_pandas)
            columns_pandas = list(merged_pandas.columns)
        else:
            num_rows_pandas = 0
            columns_pandas = []

        if spark_dfs:
            merged_spark = merge_spark_and_save(spark_dfs, path_to_save, session_id)
            if merged_spark is None:
                return jsonify({"results": "", "message": "Failed to merge Spark DFs!"}), 400
            num_rows_spark = merged_spark.count()
            columns_spark = merged_spark.columns
        else:
            num_rows_spark = 0
            columns_spark = []

        register_artifact_dir(target_folder, "dfpart", session_id=session_id, metadata={"kind": data.get("kind")})

        final_columns = list(set(columns_pandas + list(columns_spark)))
        total_rows = num_rows_pandas + num_rows_spark
        save_temp_config("dataframe_ready", True, session_id)
        save_temp_config("active_dataframe_kind", data.get("kind"), session_id)

        return jsonify({
            "results": {
                "columns": final_columns,
                "num_columns": len(final_columns),
                "num_rows": total_rows,
                "storage_url": load_temp_config("active_storage_address", session_id),
                "broker_url": load_temp_config("active_kafka_adress", session_id),
                "tool": load_temp_config("active_tool", session_id),
                "actions": ["Store data", "Source / Target Relationship", "Link Analysis"],
                "rules": load_temp_config("rule_names", session_id),
            },
            "message": "success",
        }), 200
    except Exception as e:
        print("create_DF failed:", e)
        return jsonify({"results": None, "message": str(e)}), 500
