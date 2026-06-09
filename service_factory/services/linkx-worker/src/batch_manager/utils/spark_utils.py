from pyspark.sql import SparkSession
from threading import Lock

_spark = None
_spark_lock = Lock()

def get_spark_session(hdfs_addr=None, spark_port=9000, thrift_port=None, app_name="linkx_spark_session"):
    global _spark

    def normalize_hdfs_addr(addr):
        if not addr:
            return None
        addr = addr.replace("hdfs://", "").replace("http://", "").replace("https://", "")
        if ":9870" in addr:
            addr = addr.replace(":9870", f":{spark_port}")
        if ":" not in addr:
            addr = f"{addr}:{spark_port}"
        return f"hdfs://{addr}"

    def normalize_thrift_addr(addr):
        if not addr:
            return None
        addr = addr.replace("hdfs://", "").replace("http://", "").replace("https://", "")
        if ":9870" in addr and thrift_port:
            addr = addr.replace(":9870", f":{thrift_port}")
        if ":4040" in addr and thrift_port:
            addr = addr.replace(":4040", f":{thrift_port}")
        if ":" not in addr and thrift_port:
            addr = f"{addr}:{thrift_port}"
        return f"thrift://{addr}"

    hdfs_addr = normalize_hdfs_addr(hdfs_addr)

    with _spark_lock:  # ensure only one thread creates Spark
        if _spark:
            current_fs = _spark.sparkContext._jsc.hadoopConfiguration().get("fs.defaultFS")
            if hdfs_addr and current_fs != hdfs_addr:
                print(f"WARNING: Cannot restart Spark in multithreaded mode (current: {current_fs}, requested: {hdfs_addr})")
                # safer: raise error or ignore instead of stopping
        if _spark is None:
            builder = (
                SparkSession.builder
                .appName(app_name)
                .enableHiveSupport()
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.driver.memory", "8g")  # adjust memory
                .config("spark.executor.memory", "8g")
            )
            if hdfs_addr:
                builder = builder.config("spark.hadoop.fs.defaultFS", hdfs_addr)
            if thrift_port:
                thrift_addr = normalize_thrift_addr(hdfs_addr)
                builder = builder.config("spark.hadoop.hive.metastore.uris", thrift_addr)
            _spark = builder.getOrCreate()

    print("Connected to HDFS:", _spark.sparkContext._jsc.hadoopConfiguration().get("fs.defaultFS"))
    print("Hive Metastore:", _spark.sparkContext._jsc.hadoopConfiguration().get("hive.metastore.uris"))
    return _spark

def ensure_spark_df(spark, df):
    from pyspark.sql import DataFrame as SparkDF
    if not isinstance(df, SparkDF):
        return spark.createDataFrame(df)
    return df