import pandas as pd
import json
import os
from batch_manager.utils.normalization_utils import normalize


def normalize_dataframe(df):
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].apply(lambda v: normalize("datetime", v))

        elif df[col].dtype == "object":
            # keep your old behavior: object -> string
            df[col] = df[col].apply(lambda v: normalize("string", v))

    return df
 
def load_pandas_file(path):
    print("Now loading pandas file")

    if path.endswith(".csv"):
        df = pd.read_csv(path)

    elif path.endswith(".xlsx") or path.endswith(".xls"):
        df = pd.read_excel(path)

    elif path.endswith(".parquet") or os.path.isdir(path):
        df = pd.read_parquet(path)

    elif path.endswith(".json"):
        try:
            df = pd.read_json(path)
        except Exception:
            try:
                df = pd.read_json(path, lines=True)
            except Exception:
                try:
                    with open(path, "r", encoding="utf-8") as f:
                        data = f.read().strip()

                    if "\n{" in data:
                        items = [json.loads(line) for line in data.splitlines() if line.strip()]
                        df = pd.json_normalize(items)
                    else:
                        data = json.loads(data)
                        df = pd.json_normalize(data)

                except Exception as e:
                    raise ValueError(f"Unsupported JSON format: {path} — {e}")

    else:
        raise ValueError(f"Unsupported format: {path}")

    df = normalize_dataframe(df)
    return df