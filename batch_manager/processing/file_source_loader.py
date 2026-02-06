from batch_manager.utils.pandas_utils import load_pandas_file
from batch_manager.utils.spark_utils import get_spark_session
import os

def load_file(path, session_id, use_spark=False):
    print("Now this is files loader")

    if use_spark:
        spark = get_spark_session()

        # Convert to absolute path
        abs_path = os.path.abspath(path)

        # Spark-compatible local path
        spark_path = "file:///" + abs_path.replace("\\", "/")
        print("spark_path:",spark_path)
        try:
            return spark.read.parquet(spark_path)
        except Exception as e:
            print(e)
            #return spark.read.csv(spark_path, header=True, inferSchema=True)
    print("file path:",path)
    return load_pandas_file(path)
