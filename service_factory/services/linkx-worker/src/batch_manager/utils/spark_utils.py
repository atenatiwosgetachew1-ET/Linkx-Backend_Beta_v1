from pyspark.sql import SparkSession
from threading import Lock

_spark = None
_spark_lock = Lock()

def get_spark_session(
    hdfs_addr=None,
    spark_port=9000,
    thrift_port=None,
    app_name="linkx_spark_session",
    hdfs_uri=None,
    hdfs_rpc_port=None,
    hive_metastore_uri=None,
):
    global _spark

    def strip_scheme(addr):
        return str(addr or "").replace("hdfs://", "").replace("http://", "").replace("https://", "")

    def normalize_hdfs_addr(addr):
        if hdfs_uri:
            return hdfs_uri if str(hdfs_uri).startswith("hdfs://") else f"hdfs://{hdfs_uri}"
        if not addr:
            return None
        rpc_port = hdfs_rpc_port or "8020"
        addr = strip_scheme(addr)
        if ":9870" in addr:
            addr = addr.replace(":9870", f":{rpc_port}")
        elif ":" not in addr:
            addr = f"{addr}:{rpc_port}"
        return f"hdfs://{addr}"

    def normalize_thrift_addr(addr):
        if hive_metastore_uri:
            return hive_metastore_uri if str(hive_metastore_uri).startswith("thrift://") else f"thrift://{hive_metastore_uri}"
        if not addr or not thrift_port:
            return None
        host = strip_scheme(addr).split(":", 1)[0]
        return f"thrift://{host}:{thrift_port}"

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
            thrift_addr = normalize_thrift_addr(hdfs_addr)
            if thrift_addr:
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