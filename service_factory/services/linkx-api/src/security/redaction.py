SENSITIVE_KEY_PARTS = ("password", "secret", "token", "credential", "client_secret", "x-api-key", "authorization")
MAX_STRING_LOG_LENGTH = 512


def is_sensitive_key(key):
    key = str(key or "").lower()
    if key.endswith("_ref"):
        return False
    return any(part in key for part in SENSITIVE_KEY_PARTS)


def redact_value(value, key=None, *, max_depth=6):
    if is_sensitive_key(key):
        return "***"
    if max_depth <= 0:
        return "..."
    if isinstance(value, dict):
        return {str(k): redact_value(v, k, max_depth=max_depth - 1) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [redact_value(item, key, max_depth=max_depth - 1) for item in value[:100]]
    if isinstance(value, str):
        if len(value) > MAX_STRING_LOG_LENGTH:
            return value[:MAX_STRING_LOG_LENGTH] + "...<truncated>"
        return value
    return value


def public_error(exc=None, code="internal_error"):
    return {"error": code, "message": code}
