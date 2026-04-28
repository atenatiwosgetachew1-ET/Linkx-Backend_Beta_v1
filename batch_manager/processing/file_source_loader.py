from batch_manager.utils.pandas_utils import load_pandas_file
from batch_manager.utils.spark_utils import get_spark_session
import os

def load_file(path, session_id, use_spark=False):
    print("Now this is files loader")

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
