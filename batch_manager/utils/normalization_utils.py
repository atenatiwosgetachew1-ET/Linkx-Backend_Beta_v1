# batch_manager/utils/normalization_utils.py

from datetime import datetime, timezone
import pandas as pd


def normalize(type_, value):
    if type_ == "datetime":
        return normalize_datetime(value)

    if type_ == "string":
        return normalize_string(value)

    if type_ == "number":
        return normalize_number(value)

    if type_ == "boolean":
        return normalize_boolean(value)

    if type_ == "other":
        return value

    raise ValueError(f"Unsupported type: {type_}")


def normalize_datetime(value):
    if value is None or pd.isna(value):
        return None

    dt = None

    if isinstance(value, pd.Timestamp):
        dt = value.to_pydatetime()

    elif isinstance(value, datetime):
        dt = value

    elif isinstance(value, (int, float)):
        dt = datetime.fromtimestamp(
            value if value < 1e12 else value / 1000,
            tz=timezone.utc
        )

    elif isinstance(value, str):
        s = value.strip()

        if not s:
            return None

        if s.isdigit():
            num = int(s)
            dt = datetime.fromtimestamp(
                num if num < 1e12 else num / 1000,
                tz=timezone.utc
            )
        else:
            try:
                dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            except ValueError:
                return None

    elif isinstance(value, dict):
        try:
            dt = datetime(
                value["year"],
                value["month"],
                value["day"],
                value.get("hour", 0),
                value.get("minute", 0),
                value.get("second", 0),
                value.get("millisecond", 0) * 1000,
                tzinfo=timezone.utc
            )
        except Exception:
            return None

    else:
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)

    return dt.isoformat()


def normalize_string(value):
    if value is None or pd.isna(value):
        return None
    return str(value).strip()


def normalize_number(value):
    if value is None or pd.isna(value):
        return None
    try:
        return float(value)
    except Exception:
        return None


def normalize_boolean(value):
    if value is None or pd.isna(value):
        return None

    if isinstance(value, bool):
        return value

    s = str(value).strip().lower()

    if s in ("true", "1", "yes", "y"):
        return True
    if s in ("false", "0", "no", "n"):
        return False

    return None