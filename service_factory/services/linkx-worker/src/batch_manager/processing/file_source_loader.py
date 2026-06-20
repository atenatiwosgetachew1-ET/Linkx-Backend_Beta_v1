from batch_manager.utils.pandas_utils import load_pandas_file
from batch_manager.utils.spark_utils import get_spark_session
import os
import time


def _resolve_dataframe_path(path, session_id=None, wait_seconds=10):
    raw_path = os.path.abspath(str(path))
    deadline = time.monotonic() + max(0, int(wait_seconds or 0))

    while True:
        if os.path.isdir(raw_path):
            basename = os.path.basename(raw_path.rstrip(os.sep))
            preferred = os.path.join(raw_path, f"{basename}.parquet")
            if os.path.exists(preferred):
                return preferred
            parquet_files = [
                os.path.join(raw_path, name)
                for name in os.listdir(raw_path)
                if name.lower().endswith(".parquet")
            ]
            if len(parquet_files) == 1:
                return parquet_files[0]
            return raw_path

        if not os.path.splitext(raw_path)[1]:
            basename = os.path.basename(raw_path.rstrip(os.sep))
            preferred = os.path.join(raw_path, f"{basename}.parquet")
            if os.path.exists(preferred):
                return preferred
            if session_id:
                session_file = os.path.join(raw_path, f"merged_dfpart_{session_id}.parquet")
                if os.path.exists(session_file):
                    return session_file

        if os.path.exists(raw_path) or time.monotonic() >= deadline:
            return raw_path
        time.sleep(0.25)


def load_file(path, session_id, use_spark=False):
    print("Now this is files loader")
    path = _resolve_dataframe_path(path, session_id=session_id)

    if use_spark:
        spark = get_spark_session()
        abs_path = os.path.abspath(path)
        spark_path = "file:///" + abs_path.replace("\\", "/")
        ext = os.path.splitext(path)[1].lower()
        print("spark_path:", spark_path)

        try:
            if os.path.isdir(path) or ext == ".parquet":
                return spark.read.parquet(spark_path)
            if ext == ".csv":
                return spark.read.csv(spark_path, header=True, inferSchema=True)
            if ext == ".json":
                return spark.read.json(spark_path)
            if ext in (".xlsx", ".xls"):
                print("Spark does not read Excel directly here; falling back to pandas")
            else:
                print(f"Unsupported Spark file format '{ext}'; falling back to pandas")
        except Exception as e:
            print(e)

    print("file path:", path)
    return load_pandas_file(path, session_id=session_id)
