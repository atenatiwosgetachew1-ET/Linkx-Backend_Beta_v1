import os
import shutil
import uuid

import pandas as pd
from flask import jsonify

from batch_manager.batch_data_manager import batch_data_manager
from batch_manager.processing.merger import merge_pandas_and_save, merge_spark_and_save
from batch_manager.utils.schema_utils import align_schemas
from batch_manager.utils.spark_utils import get_spark_session
from pyspark.sql.functions import col as spark_col
from batch_manager.utils.artifact_utils import ensure_artifact_dir, register_artifact_dir
from globals import load_temp_config, save_temp_config


def _is_spark_df(df):
    return "pyspark.sql.dataframe.DataFrame" in str(type(df))


def _append_dataframe(dfs, df, label="dataframe"):
    if isinstance(df, dict) and df.get("status") == "failed":
        raise RuntimeError(df.get("message") or f"{label} failed")
    if df is None:
        print(f"{label} returned None, skipping")
        return False
    if isinstance(df, dict) and "df" in df:
        dfs.append(df["df"])
        return True
    if hasattr(df, "columns") or isinstance(df, pd.DataFrame) or _is_spark_df(df):
        dfs.append(df)
        return True
    print(f"Skipping invalid object returned for {label}:", df)
    return False


def _source_manifest(data):
    files = data.get("value") or data.get("files") or []
    if isinstance(files, (str, dict)):
        files = [files]
    manifest = []
    for index, item in enumerate(files or []):
        if isinstance(item, dict):
            manifest.append({
                "index": index,
                "type": item.get("type"),
                "name": item.get("name") or item.get("filename") or item.get("path"),
                "path": item.get("path"),
                "column": item.get("column"),
                "keyword": item.get("keyword"),
                "strict": item.get("strict"),
                "size": item.get("size"),
            })
        else:
            manifest.append({"index": index, "type": data.get("kind"), "name": str(item)})
    return manifest


def _dataframe_id(data):
    value = data.get("dataframe_id") or data.get("job_id") or uuid.uuid4().hex
    return str(value).replace(os.sep, "_")


def _spark_as_string(df):
    return df.select([spark_col(name).cast("string").alias(name) for name in df.columns])


def _pandas_as_string(df):
    copy = df.copy()
    for name in copy.columns:
        copy[name] = copy[name].astype(str)
    return copy


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
    if isinstance(df, dict) and df.get("status") == "failed":
        return df
    if df is None:
        return []
    if isinstance(df, list):
        for item in df:
            _append_dataframe(dfs, item, "bulk item")
    else:
        _append_dataframe(dfs, df, "bulk dataframe")
    return dfs


def create_dataframe_result(data, session_id):
    dfs = load_dataframes_for_create_df(data, session_id)
    source_manifest = _source_manifest(data)
    if isinstance(dfs, dict) and dfs.get("status") == "failed":
        return {
            "results": {
                "source_manifest": source_manifest,
                "failed_sources": dfs.get("failed_sources", []),
                "loaded_sources": dfs.get("loaded_sources", []),
                "loaded_dataframes": dfs.get("loaded_dataframes", 0),
            },
            "message": dfs.get("message") or "Selected dataframe sources failed",
            "status": "failed",
        }, 400
    if not dfs:
        return {
            "results": {"source_manifest": source_manifest, "loaded_dataframes": 0},
            "message": "No valid dataframes loaded",
            "status": "failed",
        }, 400

    try:
        all_columns = set()
        for df in dfs:
            all_columns.update(df.columns)
        all_columns = list(all_columns)
        aligned = [align_schemas(df, all_columns) for df in dfs]

        pandas_dfs = [df for df in aligned if isinstance(df, pd.DataFrame)]
        spark_dfs = [df for df in aligned if _is_spark_df(df)]

        dataframe_id = _dataframe_id(data)
        path_to_save = ensure_artifact_dir("dfparts")
        folder_name = f"merged_dfpart_{session_id}_{dataframe_id}"
        target_folder = os.path.join(path_to_save, folder_name)
        if os.path.exists(target_folder):
            try:
                shutil.rmtree(target_folder)
                print("Deleted existing dataframe version folder:", target_folder)
            except Exception as e:
                print("Failed deleting dataframe version folder:", e)

        use_spark_output = bool(spark_dfs)
        if spark_dfs and pandas_dfs:
            spark = get_spark_session()
            spark_dfs = [_spark_as_string(df) for df in spark_dfs]
            for pdf in pandas_dfs:
                if not pdf.empty:
                    spark_dfs.append(spark.createDataFrame(_pandas_as_string(pdf)))
            pandas_dfs = []

        if spark_dfs:
            merged_spark = merge_spark_and_save(spark_dfs, path_to_save, session_id, folder_name=folder_name)
            if merged_spark is None:
                return {"results": "", "message": "Failed to merge Spark DFs!", "status": "failed"}, 400
            total_rows = merged_spark.count()
            final_columns = list(merged_spark.columns)
        elif pandas_dfs:
            merged_pandas = merge_pandas_and_save(pandas_dfs, path_to_save, session_id, folder_name=folder_name)
            if merged_pandas is None:
                return {"results": "", "message": "Failed to merge pandas DFs!", "status": "failed"}, 400
            total_rows = len(merged_pandas)
            final_columns = list(merged_pandas.columns)
        else:
            return {"results": "", "message": "No supported dataframe types loaded", "status": "failed"}, 400

        artifact_id = register_artifact_dir(
            target_folder,
            "dfpart",
            session_id=session_id,
            job_id=data.get("job_id"),
            metadata={
                "kind": data.get("kind"),
                "dataframe_id": dataframe_id,
                "row_count": total_rows,
                "columns": final_columns,
                "source_manifest": source_manifest,
                "loaded_dataframes": len(dfs),
                "use_spark": use_spark_output,
            },
        )

        save_temp_config("dataframe_ready", True, session_id)
        save_temp_config("active_dataframe_kind", data.get("kind"), session_id)
        save_temp_config("active_dataframe_id", dataframe_id, session_id)
        save_temp_config("active_dataframe_dir", target_folder, session_id)
        save_temp_config("active_dataframe_rows", total_rows, session_id)
        save_temp_config("active_dataframe_columns", final_columns, session_id)
        save_temp_config("active_dataframe_use_spark", use_spark_output, session_id)
        save_temp_config("active_dataframe_source_manifest", source_manifest, session_id)

        return {
            "results": {
                "dataframe_id": dataframe_id,
                "dataframe_dir": target_folder,
                "artifact_id": artifact_id,
                "columns": final_columns,
                "num_columns": len(final_columns),
                "num_rows": total_rows,
                "loaded_dataframes": len(dfs),
                "source_manifest": source_manifest,
                "storage_url": load_temp_config("active_storage_address", session_id),
                "broker_url": load_temp_config("active_kafka_adress", session_id),
                "tool": load_temp_config("active_tool", session_id),
                "actions": ["Store data", "Source / Target Relationship", "Link Analysis"],
                "rules": load_temp_config("rule_names", session_id),
            },
            "message": "success",
            "status": "success",
        }, 200
    except Exception as e:
        print("create_DF failed:", e)
        return {"results": None, "message": str(e), "status": "failed"}, 500


def create_dataframe_response(data, session_id):
    body, status = create_dataframe_result(data, session_id)
    response_body = dict(body)
    response_body.pop("status", None)
    return jsonify(response_body), status
