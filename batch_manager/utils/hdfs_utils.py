import os
import pickle
import re
from batch_manager.utils.spark_utils import get_spark_session
from py4j.java_gateway import java_import
from globals import create_file
from datetime import datetime
import json

def list_files_recursively_hdfs(fs, path, keyword):
    """
    Keyword-based recursive HDFS listing using Hadoop listFiles.
    """
    results = []
    iterator = fs.listFiles(path, True)  # True → recursive
    while iterator.hasNext():
        file_status = iterator.next()
        path_str = file_status.getPath().toString()
        if keyword.lower() in path_str.lower():
            results.append(file_status)  # store FileStatus, not string
    return results


def load_hdfs_files(spark, hdfs_files):
    dfs = []
    all_cols = set()

    for item in hdfs_files:
        name = item["name"]
        path = item["path"]
        print("getting raw file:", item)
        if name.endswith(".csv"):
            df = spark.read.csv(path, header=True, inferSchema=True)
        elif name.endswith(".parquet"):
            df = spark.read.parquet(path)
        else:
            print(f"Unsupported HDFS file type: {name}")
            continue

        # Avoid duplicate column names
        rename_map = {c: c for c in df.columns}

        df = df.selectExpr(
            *[f"{old} as {new}" for old, new in rename_map.items()]
        )

        all_cols.update(rename_map.values())
        dfs.append(df)
        print("raw dfs:",dfs)
    return dfs

def categorize_sources(file_list):
    file_results = []
    keyword_results = []

    pattern_result = re.compile(r"Results found for '([^']+)'")
    for item in file_list:
        name = item["name"]
        match = pattern_result.match(name)

        if match:
            # It's a keyword result
            keyword = match.group(1)
            keyword_results.append({
                'keyword': keyword,
                'rows': item['size'],
                'type': 'es' if item['size'] < 100000 else 'hive'
            })
        else:
            # It's a file result
            file_results.append({
                'name': item['name'],
                'size': item['size'],
                'path': item['path']
            })

    return file_results, keyword_results    

def stream_hdfs_metadata(storage_ip, base_path, keyword="", date=None, offset=0, limit=50):
    spark = get_spark_session(hdfs_addr=storage_ip)
    sc = spark.sparkContext
    jvm = sc._jvm
    java_import(jvm, "org.apache.hadoop.fs.Path")

    fs = jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())

    # 🔹 Parse requested date (YYYY-MM-DD)
    requested_date = None
    if date:
        requested_date = datetime.strptime(date, "%Y-%m-%d").date()

    dirs_to_scan = ["individual", "entity"]
    results = []
    collected = 0
    lookahead = offset + limit + 1
    MAX_SCAN = lookahead + 1000

    keyword = keyword.lower()

    for sub in dirs_to_scan:
        stack = [jvm.Path(f"{base_path}/{sub}")]

        while stack:
            current = stack.pop()

            try:
                statuses = fs.listStatus(current)
            except Exception:
                continue

            for st in statuses:
                if st.isDirectory():
                    stack.append(st.getPath())
                    continue

                path_str = st.getPath().toString()

                # 🔹 Keyword filter
                if keyword and keyword not in path_str.lower():
                    continue

                # 🔹 Date filter (LAST MODIFIED)
                mod_time_ms = st.getModificationTime()
                mod_date = datetime.fromtimestamp(
                    mod_time_ms / 1000
                ).date()

                # Only files modified on requested date
                if requested_date and mod_date != requested_date:
                    continue

                collected += 1

                if collected > offset and len(results) < lookahead:
                    results.append({
                        "name": os.path.basename(path_str),
                        "path": path_str,
                        "size": round(st.getLen() / 1024, 2),
                        "date": mod_date.isoformat(),
                        "strict": "",
                        "type":"raw"
                    })

                if len(results) >= lookahead or collected >= MAX_SCAN:
                    break

            if len(results) >= lookahead or collected >= MAX_SCAN:
                break

        if len(results) >= lookahead or collected >= MAX_SCAN:
            break

    return {
        "results": results[:limit],
        "has_more": len(results) > limit,
        "offset": offset,
        "limit": limit,
        "date": requested_date.isoformat() if requested_date else None
    }