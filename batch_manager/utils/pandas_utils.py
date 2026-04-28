import pandas as pd
import json
import os
import re
from batch_manager.utils.normalization_utils import normalize
from batch_manager.utils.notification_utils import add_notification


DEPRECATED_DATETIME_PATTERN = re.compile(
    r"^\s*\d{1,2}[/-]\d{1,2}[/-]\d{2,4}(?:\s+\d{1,2}:\d{2}(?::\d{2})?\s*(?:AM|PM|am|pm)?)?\s*$"
)


def _sample_deprecated_datetime_strings(series, limit=5):
    samples = []
    for value in series.dropna().astype(str).head(200):
        if DEPRECATED_DATETIME_PATTERN.match(value):
            samples.append(value)
            if len(samples) >= limit:
                break
    return samples


def normalize_dataframe(df, session_id=None):
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            count = int(df[col].notna().sum())
            df[col] = df[col].apply(lambda v: normalize("datetime", v))
            if count:
                add_notification(
                    session_id,
                    "info",
                    "DATETIME_NORMALIZED",
                    f"Deprecated datetime format detected in column '{col}'. Values were normalized to standard ISO format.",
                    source="file_loader",
                    details={"column": col, "count": count},
                )

        elif df[col].dtype == "object":
            samples = _sample_deprecated_datetime_strings(df[col])
            if samples:
                add_notification(
                    session_id,
                    "warning",
                    "DEPRECATED_DATETIME_FORMAT",
                    f"Deprecated datetime format detected in column '{col}'. Values will be normalized while processing continues.",
                    source="file_loader",
                    details={"column": col, "samples": samples},
                )
            # keep your old behavior: object -> string
            df[col] = df[col].apply(lambda v: normalize("string", v))

    return df

def load_pandas_file(path, session_id=None):
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

    df = normalize_dataframe(df, session_id=session_id)
    return df
