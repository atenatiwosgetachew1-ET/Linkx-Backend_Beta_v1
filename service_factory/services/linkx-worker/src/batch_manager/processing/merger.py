try:
    from pyspark.sql import DataFrame as SparkDF
except ImportError:
    SparkDF = None
import pandas as pd
import os
from functools import reduce
import time

# ---------------------------------------------
# SPARK MERGE + SAVE WITH EXCEPTION HANDLING
# ---------------------------------------------
def merge_spark_and_save(dfs: list[SparkDF], base_path: str, session_id: str, folder_name: str | None = None):
    print("merge_spark_and_save:", session_id)
    try:
        # Empty list check
        if not dfs:
            print("[ERROR] merge_spark_and_save: no Spark DataFrames provided.")
            return None

        # Merge using unionByName
        try:
            merged = dfs[0]
            for df in dfs[1:]:
                merged = merged.unionByName(df, allowMissingColumns=True)
        except Exception as e:
            print("[ERROR] Failed to merge Spark DataFrames:", e)
            return None

        # Resolve output folder
        folder_name = folder_name or f"merged_dfpart_{session_id}"

        try:
            local_dir = os.path.abspath(os.path.join(base_path, folder_name))
            os.makedirs(local_dir, exist_ok=True)
        except Exception as e:
            print("[ERROR] Failed creating output directory:", e)
            return None

        # Convert to Spark-friendly file:/// URI
        spark_path = "file:///" + local_dir.replace("\\", "/")
        print("local_dir11:",local_dir)
        # Write to parquet
        try:
            merged.coalesce(1).write.mode("overwrite").parquet(spark_path)
        except Exception as e:
            print("[ERROR] Failed writing Spark DataFrame to parquet:", e)
            return None

        return merged

    except Exception as e:
        print("[UNEXPECTED ERROR] merge_spark_and_save:", e)
        return None


# ---------------------------------------------
# PANDAS MERGE + SAVE WITH EXCEPTION HANDLING
# ---------------------------------------------
def merge_pandas_and_save(dfs: list[pd.DataFrame], base_path: str, session_id: str, folder_name: str | None = None):
    """
    Merge pandas dataframes, harmonize dtypes, and save to parquet:
        merged_dfpart_<session_id>/<session_id>.parquet
    """
    started = time.monotonic()
    try:
        if not dfs:
            print("[ERROR] merge_pandas_and_save: no pandas DataFrames provided.", flush=True)
            return None

        shapes = [getattr(df, "shape", None) for df in dfs]
        print(f"[pandas_merge] start session={session_id} dfs={len(dfs)} shapes={shapes} base_path={base_path}", flush=True)

        try:
            merged = pd.concat(dfs, ignore_index=True)
            print(f"[pandas_merge] concat done session={session_id} shape={merged.shape}", flush=True)
        except Exception as e:
            print("[ERROR] Pandas concat failed:", e, flush=True)
            return None

        try:
            for col in merged.columns:
                if merged[col].dtype == "object" or merged[col].apply(lambda x: isinstance(x, (str, dict, list))).any():
                    merged[col] = merged[col].astype(str)
            print(f"[pandas_merge] dtype harmonized session={session_id}", flush=True)
        except Exception as e:
            print("[ERROR] Failed harmonizing dtypes:", e, flush=True)
            return None

        folder_name = folder_name or f"merged_dfpart_{session_id}"
        try:
            output_dir = os.path.join(base_path, folder_name)
            print(f"[pandas_merge] mkdir start output_dir={output_dir}", flush=True)
            os.makedirs(output_dir, exist_ok=True)
            print(f"[pandas_merge] mkdir done output_dir={output_dir}", flush=True)
        except Exception as e:
            print("[ERROR] Failed creating pandas output directory:", e, flush=True)
            return None

        output_path = os.path.join(output_dir, f"{folder_name}.parquet")
        try:
            print(f"[pandas_merge] parquet write start path={output_path}", flush=True)
            merged.to_parquet(output_path, index=False, engine="pyarrow", compression="snappy")
            size = os.path.getsize(output_path) if os.path.exists(output_path) else 0
            elapsed = round(time.monotonic() - started, 3)
            print(f"[pandas_merge] parquet write done path={output_path} bytes={size} elapsed={elapsed}s", flush=True)
        except Exception as e:
            print("[ERROR] Failed saving pandas parquet:", e, flush=True)
            return None
        return merged

    except Exception as e:
        print("[UNEXPECTED ERROR] merge_pandas_and_save:", e, flush=True)
        return None
