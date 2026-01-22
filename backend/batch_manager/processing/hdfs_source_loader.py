import re
from pyspark.sql import SparkSession
import pandas as pd
from batch_manager.utils.hdfs_utils import categorize_sources, load_hdfs_files
from batch_manager.utils.elastic_utils import load_elastic_rows
from batch_manager.utils.hive_utils import load_hive_rows
from batch_manager.utils.schema_utils import align_schemas

def load_source(payload):
    session_id = payload["session_id"]
    file_list = payload.get("files", [])
    search_columns = payload["search_columns"]
    date = payload.get("date",None)
    if not file_list:
        print("No files provided")
        return None

    # 1) Categorize sources
    file_results, keyword_results = categorize_sources(file_list)

    spark = payload["spark"]

    dfs = []
    all_columns = set()

    # 2) HDFS files
    if file_results:
        hdfs_files = [{"name": f["name"], "path": f["path"]} for f in file_results]
        hdfs_dfs, cols = load_hdfs_files(spark, hdfs_files)
        dfs.extend(hdfs_dfs)
        all_columns.update(cols)

    # 3) Elastic rows
    es_keywords = [k for k in keyword_results if k["type"] == "es"]
    if es_keywords:
        API_URL = payload["API_URL"]
        
        fetch_columns = payload["fetch_columns"]
        keys = [k["keyword"] for k in es_keywords]
        es_dfs, cols = load_elastic_rows(
            API_URL, keys, search_columns, fetch_columns
        )
        dfs.extend(es_dfs)
        all_columns.update(cols)

    # 4) Hive rows
    hive_keywords = [k for k in keyword_results if k["type"] == "hive"]
    if hive_keywords:
        storage_address = payload["storage_address"]
        hive_port = payload["hive_port"]
        tables= payload["tables"]
        limit = payload.get("dataframes_limit",1000000) 
        keys = [k["keyword"] for k in hive_keywords]
        hive_df, cols = load_hive_rows(storage_address, hive_port, spark, search_columns, tables, keys, date, limit)
        if hive_df is not None:     
            hive_df = hive_df.limit(limit)
            dfs.append(hive_df)
            all_columns.update(cols)


    # 5) Drop invalid / empty frames
    dfs = [df for df in dfs if df is not None or len(df) > 0]

    if not dfs:
        print("No data frames with real values generated")
        return None

    # 6) Align schemas
    all_columns = list(all_columns)
    aligned = [align_schemas(df, all_columns) for df in dfs]

    first_df = aligned[0]

    # 7) MERGE HERE (critical fix)
    if isinstance(first_df, pd.DataFrame):
        merged = pd.concat(aligned, ignore_index=True)
    else:
        merged = aligned[0]
        for df in aligned[1:]:
            merged = merged.unionByName(df)

    return merged
