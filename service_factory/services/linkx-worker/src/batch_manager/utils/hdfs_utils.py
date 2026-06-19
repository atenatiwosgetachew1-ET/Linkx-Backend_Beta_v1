import os
import pickle
import re
from batch_manager.utils.spark_utils import get_spark_session
from py4j.java_gateway import java_import
from globals import create_file
from datetime import datetime
import json
from urllib.parse import quote
import requests

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
        ext = os.path.splitext(name)[1].lower()
        print("getting raw file:", item)
        if ext == ".csv":
            df = spark.read.csv(path, header=True, inferSchema=True)
        elif ext == ".parquet":
            df = spark.read.parquet(path)
        elif ext == ".json":
            df = spark.read.json(path)
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
    """List raw HDFS files through WebHDFS without starting Spark on the API server."""
    host = str(storage_ip or "").replace("http://", "").replace("https://", "").replace("hdfs://", "").strip("/")
    if not host:
        return {
            "results": [],
            "has_more": False,
            "offset": offset,
            "limit": limit,
            "date": date,
            "base_path": base_path,
            "storage": storage_ip,
            "errors": ["missing storage address"],
            "message": "Missing HDFS storage address.",
        }
    if ":" in host:
        host, webhdfs_port = host.split(":", 1)
    else:
        webhdfs_port = "9870"
    hdfs_uri_prefix = f"hdfs://{host}:8020"

    try:
        offset = max(0, int(offset or 0))
    except (TypeError, ValueError):
        offset = 0
    try:
        limit = max(1, int(limit or 50))
    except (TypeError, ValueError):
        limit = 50

    requested_date = None
    if date:
        try:
            requested_date = datetime.strptime(str(date), "%Y-%m-%d").date()
        except ValueError:
            return {
                "results": [],
                "has_more": False,
                "offset": offset,
                "limit": limit,
                "date": date,
                "base_path": base_path,
                "storage": storage_ip,
                "errors": ["date must be YYYY-MM-DD"],
                "message": "Invalid date format for raw file search.",
            }

    def normalize_path(value):
        value = "/" + str(value or "").strip("/")
        return value.replace("//", "/")

    def list_status(hdfs_path):
        encoded = quote(normalize_path(hdfs_path), safe="/")
        url = f"http://{host}:{webhdfs_port}/webhdfs/v1{encoded}?op=LISTSTATUS"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json().get("FileStatuses", {}).get("FileStatus", [])

    dirs_to_scan = ["individual", "entity"]
    keyword = str(keyword or "").lower()
    base_path = normalize_path(base_path)
    results = []
    errors = []
    collected = 0
    lookahead = offset + limit + 1
    max_scan = lookahead + 1000

    for sub in dirs_to_scan:
        stack = [normalize_path(f"{base_path}/{sub}")]
        while stack:
            current = stack.pop()
            try:
                statuses = list_status(current)
            except Exception as exc:
                if len(errors) < 5:
                    errors.append(f"{current}: {exc}")
                continue

            for status in statuses:
                suffix = status.get("pathSuffix") or ""
                child_path = normalize_path(f"{current}/{suffix}")
                if status.get("type") == "DIRECTORY":
                    stack.append(child_path)
                    continue

                path_for_filter = child_path.lower()
                if keyword and keyword not in path_for_filter:
                    continue

                mod_time_ms = status.get("modificationTime") or 0
                mod_date = datetime.fromtimestamp(mod_time_ms / 1000).date() if mod_time_ms else None
                if requested_date and mod_date != requested_date:
                    continue

                collected += 1
                if collected > offset and len(results) < lookahead:
                    hdfs_path = f"{hdfs_uri_prefix}{child_path}"
                    results.append({
                        "name": os.path.basename(child_path),
                        "path": hdfs_path,
                        "size": round((status.get("length") or 0) / 1024, 2),
                        "date": mod_date.isoformat() if mod_date else None,
                        "strict": "",
                        "type": "raw",
                    })

                if len(results) >= lookahead or collected >= max_scan:
                    break
            if len(results) >= lookahead or collected >= max_scan:
                break
        if len(results) >= lookahead or collected >= max_scan:
            break

    message = ""
    if not results and errors:
        message = "No files returned; WebHDFS listing failed for scanned paths."
    elif not results:
        message = "No files matched the raw search filters."

    return {
        "results": results[:limit],
        "has_more": len(results) > limit,
        "offset": offset,
        "limit": limit,
        "date": requested_date.isoformat() if requested_date else None,
        "base_path": base_path,
        "storage": host,
        "errors": errors,
        "message": message,
    }

