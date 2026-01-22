import pandas as pd
import json

def load_pandas_file(path): #Pandas is used for all raw files
    print("Now loading pandas file")
    if path.endswith(".csv"): #For CSV Files
        return pd.read_csv(path)
    elif path.endswith(".xlsx") or path.endswith(".xls"): #For EXCEL Files
        return pd.read_excel(path)
    elif path.endswith(".parquet"): #For PARQUET Files
        return pd.read_parquet(path)
    elif path.endswith(".json"): #For JSON Files
        # 1️⃣ Try standard JSON
        try:
            return pd.read_json(path)
        except Exception:
            pass
        # 2️⃣ Try JSON Lines (.jsonl or line-delimited)
        try:
            return pd.read_json(path, lines=True)
        except Exception:
            pass
        # 3️⃣ Try manual loading (list of JSON objects)
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = f.read().strip()
                # If multiple JSON objects appear on separate lines
                if "\n{" in data:
                    items = [json.loads(line) for line in data.splitlines() if line.strip()]
                    return pd.json_normalize(items)
                # Try normal JSON again but with python loader
                data = json.loads(data)
                return pd.json_normalize(data)
        except Exception as e:
            raise ValueError(f"Unsupported JSON format: {path} — {e}")
    else:
        raise ValueError(f"Unsupported format: {path}")


def harmonize_dtypes(df):
    """
    Fix mixed dtypes from your original logic.
    """
    return df.apply(lambda col: col.astype(str) if col.dtype == "object" else col)
