from pyspark.sql import DataFrame as SparkDF
import pandas as pd
import os
from functools import reduce

# ---------------------------------------------
# SPARK MERGE + SAVE WITH EXCEPTION HANDLING
# ---------------------------------------------
def merge_spark_and_save(dfs: list[SparkDF], base_path: str, session_id: str):
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
        folder_name = f"merged_dfpart_{session_id}"

        try:
            local_dir = os.path.abspath(os.path.join(base_path, folder_name))
            os.makedirs(local_dir, exist_ok=True)
        except Exception as e:
            print("[ERROR] Failed creating output directory:", e)
            return None

        # Convert to Spark-friendly file:/// URI
        spark_path = "file:///" + local_dir.replace("\\", "/")

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
def merge_pandas_and_save(dfs: list[pd.DataFrame], base_path: str, session_id: str):
    print("recieved_infos-merge_pandas",dfs,base_path,session_id)
    """
    Merge pandas dataframes, harmonize dtypes, and save to parquet:
        merged_dfpart_<session_id>/<session_id>.parquet
    """
    try:
        if not dfs:
            print("[ERROR] merge_pandas_and_save: no pandas DataFrames provided.")
            return None

        # Merge
        try:
            merged = pd.concat(dfs, ignore_index=True)
        except Exception as e:
            print("[ERROR] Pandas concat failed:", e)
            return None

        # Harmonize dtypes
        try:
            for col in merged.columns:
                if merged[col].dtype == "object" or merged[col].apply(lambda x: isinstance(x, (str, dict, list))).any():
                    merged[col] = merged[col].astype(str)
        except Exception as e:
            print("[ERROR] Failed harmonizing dtypes:", e)
            return None

        # Create folder
        folder_name = f"merged_dfpart_{session_id}"

        try:
            output_dir = os.path.join(base_path, folder_name)
            print("base_path:",base_path,",output_dir:",output_dir)
            os.makedirs(output_dir, exist_ok=True)
        except Exception as e:
            print("[ERROR] Failed creating pandas output directory:", e)
            return None

        # Output file path
        output_path = os.path.join(output_dir, f"{folder_name}.parquet")

        # Save to parquet
        try:
            merged.to_parquet(output_path, index=False)
        except Exception as e:
            print("[ERROR] Failed saving pandas parquet:", e)
            return None
        return merged

    except Exception as e:
        print("[UNEXPECTED ERROR] merge_pandas_and_save:", e)
        return None
