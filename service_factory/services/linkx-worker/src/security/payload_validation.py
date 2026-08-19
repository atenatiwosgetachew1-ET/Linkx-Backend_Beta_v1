import re
from functools import wraps

from flask import g, jsonify, request
from jsonschema import Draft202012Validator
from werkzeug.utils import secure_filename


CONTROL_CHAR_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
SENSITIVE_KEYS = {"password", "client_secret", "token", "x-api-key", "secret"}
DEFAULT_MAX_STRING_LENGTH = 4096


class PayloadValidationError(ValueError):
    def __init__(self, message, field=None):
        super().__init__(message)
        self.message = message
        self.field = field


def _is_sensitive_key(key):
    key = str(key or "").lower()
    return key in SENSITIVE_KEYS or key.endswith("_secret") or key.endswith("_password")


def sanitize_value(value, key=None, max_string_length=DEFAULT_MAX_STRING_LENGTH):
    if isinstance(value, str):
        if CONTROL_CHAR_RE.search(value):
            raise PayloadValidationError("unsafe_control_character", key)
        if len(value) > max_string_length:
            raise PayloadValidationError("string_too_long", key)
        return value if _is_sensitive_key(key) else value.strip()

    if isinstance(value, list):
        return [sanitize_value(item, key=key, max_string_length=max_string_length) for item in value]

    if isinstance(value, dict):
        sanitized = {}
        for child_key, child_value in value.items():
            safe_key = sanitize_value(str(child_key), max_string_length=256)
            sanitized[safe_key] = sanitize_value(
                child_value,
                key=safe_key,
                max_string_length=max_string_length,
            )
        return sanitized

    return value


def schema_error_message(error):
    field = ".".join(str(part) for part in error.absolute_path)
    if field:
        return f"{field}: {error.message}"
    return error.message


def validate_payload(payload, schema, max_string_length=DEFAULT_MAX_STRING_LENGTH):
    sanitized = sanitize_value(payload, max_string_length=max_string_length)
    validator = Draft202012Validator(schema)
    errors = sorted(validator.iter_errors(sanitized), key=lambda err: list(err.absolute_path))
    if errors:
        first = errors[0]
        raise PayloadValidationError(schema_error_message(first), ".".join(str(part) for part in first.absolute_path))
    return sanitized



def validate_uploaded_files(files, *, allowed_extensions, max_files=10, field="file"):
    if not files:
        raise PayloadValidationError("file_required", field)
    if len(files) > max_files:
        raise PayloadValidationError("too_many_files", field)

    safe_files = []
    allowed = {ext.lower().lstrip(".") for ext in allowed_extensions}
    for uploaded_file in files:
        filename = getattr(uploaded_file, "filename", "") or ""
        if not filename:
            raise PayloadValidationError("empty_filename", field)
        sanitize_value(filename, key=field, max_string_length=255)
        safe_name = secure_filename(filename)
        if not safe_name:
            raise PayloadValidationError("unsafe_filename", field)
        if "." not in safe_name:
            raise PayloadValidationError("missing_file_extension", field)
        ext = safe_name.rsplit(".", 1)[1].lower()
        if ext not in allowed:
            raise PayloadValidationError(f"unsupported_file_type:.{ext}", field)
        safe_files.append((uploaded_file, safe_name, ext))
    return safe_files

def validated_json():
    return getattr(g, "validated_json", None)


def validate_json_payload(
    schema,
    *,
    required=True,
    error_message="validation_error",
    max_string_length=DEFAULT_MAX_STRING_LENGTH,
    include_detail=True,
):
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            payload = request.get_json(silent=True)
            if payload is None:
                if required:
                    body = {"message": error_message}
                    if include_detail:
                        body["detail"] = "json_body_required"
                    return jsonify(body), 400
                payload = {}
            if not isinstance(payload, dict):
                body = {"message": error_message}
                if include_detail:
                    body["detail"] = "json_object_required"
                return jsonify(body), 400
            try:
                g.validated_json = validate_payload(payload, schema, max_string_length=max_string_length)
            except PayloadValidationError as exc:
                body = {"message": error_message}
                if include_detail:
                    body["detail"] = exc.message
                if include_detail and exc.field:
                    body["field"] = exc.field
                return jsonify(body), 400
            return fn(*args, **kwargs)
        return wrapper
    return decorator


COMMON_SCHEMAS = {
    "login": {
        "type": "object",
        "required": ["username", "password"],
        "additionalProperties": False,
        "properties": {
            "username": {"type": "string", "minLength": 1, "maxLength": 255},
            "password": {"type": "string", "minLength": 1, "maxLength": 1024},
        },
    },
    "service_token": {
        "type": "object",
        "required": ["client_id", "client_secret"],
        "additionalProperties": False,
        "properties": {
            "client_id": {"type": "string", "minLength": 1, "maxLength": 255, "pattern": "^[A-Za-z0-9_.:-]+$"},
            "client_secret": {"type": "string", "minLength": 1, "maxLength": 2048},
        },
    },
    "parent_token": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "username": {"type": "string", "minLength": 1, "maxLength": 255},
            "sub": {"type": "string", "minLength": 1, "maxLength": 255},
            "display_name": {"type": "string", "maxLength": 255},
            "name": {"type": "string", "maxLength": 255},
            "roles": {"type": ["array", "string"], "items": {"type": "string", "maxLength": 64}},
            "parent_roles": {"type": ["array", "string"], "items": {"type": "string", "maxLength": 64}},
        },
        "anyOf": [{"required": ["username"]}, {"required": ["sub"]}],
    },
    "verify": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "token": {"type": "string", "minLength": 1, "maxLength": 8192},
        },
    },
    "init": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "id": {"type": "string", "enum": ["init"]},
            "existing_session": {
                "anyOf": [
                    {"type": "string", "maxLength": 128, "pattern": "^[A-Za-z0-9_.:-]*$"},
                    {"type": "integer"},
                    {"type": "null"},
                ],
            },
            "socket_id": {
                "anyOf": [
                    {"type": "string", "maxLength": 128, "pattern": "^[A-Za-z0-9_.:-]*$"},
                    {"type": "null"},
                ],
            },
        },
    },
    "service_account_create": {
        "type": "object",
        "required": ["client_id", "client_secret"],
        "additionalProperties": False,
        "properties": {
            "client_id": {"type": "string", "minLength": 1, "maxLength": 255, "pattern": "^[A-Za-z0-9_.:-]+$"},
            "client_secret": {"type": "string", "minLength": 1, "maxLength": 2048},
            "display_name": {"type": "string", "maxLength": 255},
            "permissions": {"type": "array", "items": {"type": "string", "minLength": 1, "maxLength": 128, "pattern": "^[a-z]+:[a-z_]+$"}},
        },
    },
    "service_account_update": {
        "type": "object",
        "additionalProperties": False,
        "minProperties": 1,
        "properties": {
            "client_secret": {"type": "string", "minLength": 1, "maxLength": 2048},
            "display_name": {"type": ["string", "null"], "maxLength": 255},
            "permissions": {"type": "array", "items": {"type": "string", "minLength": 1, "maxLength": 128, "pattern": "^[a-z]+:[a-z_]+$"}},
            "is_active": {"type": "boolean"},
        },
    },
    "user_create": {
        "type": "object",
        "required": ["username", "password"],
        "additionalProperties": False,
        "properties": {
            "username": {"type": "string", "minLength": 1, "maxLength": 255, "pattern": "^[A-Za-z0-9_.@:-]+$"},
            "password": {"type": "string", "minLength": 1, "maxLength": 1024},
            "display_name": {"type": "string", "maxLength": 255},
            "roles": {"type": ["array", "string"], "items": {"type": "string", "maxLength": 64}},
            "is_active": {"type": "boolean"},
        },
    },
    "user_update": {
        "type": "object",
        "additionalProperties": False,
        "minProperties": 1,
        "properties": {
            "password": {"type": "string", "minLength": 1, "maxLength": 1024},
            "display_name": {"type": ["string", "null"], "maxLength": 255},
            "roles": {"type": ["array", "string"], "items": {"type": "string", "maxLength": 64}},
            "is_active": {"type": "boolean"},
        },
    },
    "str_link_analysis": {
        "type": "object",
        "required": ["entity", "type", "value"],
        "additionalProperties": False,
        "properties": {
            "entity": {"type": "string", "enum": ["bank"]},
            "type": {"type": "string", "enum": ["account_number"]},
            "value": {"type": "string", "minLength": 1, "maxLength": 255},
            "session_id": {"type": "string", "maxLength": 128, "pattern": "^[A-Za-z0-9_.:-]+$"},
            "str_id": {"type": "string", "maxLength": 128, "pattern": "^[A-Za-z0-9_.:-]+$"},
            "frontend_session_id": {"type": "string", "maxLength": 128, "pattern": "^[A-Za-z0-9_.:-]+$"},
            "date": {"type": ["string", "null"], "maxLength": 64},
        },
    },
    "configuration": {
        "type": "object",
        "required": ["id", "session_id"],
        "properties": {
            "id": {"type": "string", "enum": ["load", "save", "remove_rule", "reset", "load_default", "upload"]},
            "session_id": {"type": "string", "minLength": 1, "maxLength": 128, "pattern": "^[A-Za-z0-9_.:-]+$"},
            "rule_name": {"type": "string", "maxLength": 255},
            "active_rule": {"type": ["array", "string"], "items": {"type": "string", "maxLength": 255}},
        },
        "additionalProperties": True,
    },
    "init_source": {
        "type": "object",
        "required": ["id", "session_id", "window_id"],
        "additionalProperties": False,
        "properties": {
            "id": {"type": "string", "enum": ["source_window"]},
            "session_id": {
                "anyOf": [
                    {"type": "string", "minLength": 1, "maxLength": 128, "pattern": "^[A-Za-z0-9_.:-]+$"},
                    {"type": "integer"},
                ],
            },
            "window_id": {
                "anyOf": [
                    {"type": "string", "minLength": 1, "maxLength": 128, "pattern": "^[A-Za-z0-9_.:-]+$"},
                    {"type": "integer"},
                ],
            },
        },
    },
    "connect_to_source": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "addressType": {"type": ["string", "null"], "enum": ["broker", "api", "hdfs", "storage", None]},
            "type": {"type": ["string", "null"], "enum": ["broker", "api", "hdfs", "storage", None]},
            "address": {"type": ["string", "null"], "maxLength": 2048},
            "broker": {"type": ["string", "null"], "maxLength": 512},
            "broker_url": {"type": ["string", "null"], "maxLength": 512},
            "api": {"type": ["string", "null"], "maxLength": 2048},
            "url": {"type": ["string", "null"], "maxLength": 2048},
            "storage": {"type": ["string", "null"], "maxLength": 512},
            "hdfs": {"type": ["string", "null"], "maxLength": 512},
            "topic": {"type": ["string", "null"], "maxLength": 255},
            "kafka_topic": {"type": ["string", "null"], "maxLength": 255},
            "source_mode": {"type": ["string", "null"], "enum": ["batch", "realtime", None]},
            "mode": {"type": ["string", "null"], "enum": ["batch", "realtime", None]},
            "session_id": {"type": ["string", "null"], "maxLength": 128, "pattern": "^[A-Za-z0-9_.:-]*$"},
            "source_id": {"type": ["string", "null"], "maxLength": 128, "pattern": "^[A-Za-z0-9_.:-]*$"},
        },
    },
    "disconnect_source": {
        "type": "object",
        "required": ["session_id"],
        "additionalProperties": False,
        "properties": {
            "broker": {"type": ["string", "null"], "maxLength": 512},
            "hdfs": {"type": ["string", "null"], "maxLength": 512},
            "session_id": {"type": "string", "minLength": 1, "maxLength": 128, "pattern": "^[A-Za-z0-9_.:-]+$"},
        },
    },
    "connect_to_tool": {
        "type": "object",
        "required": ["tool_name", "url", "username", "password", "source_id"],
        "additionalProperties": False,
        "properties": {
            "tool_name": {"type": "string", "minLength": 1, "maxLength": 64, "pattern": "^[A-Za-z0-9_.:-]+$"},
            "url": {"type": "string", "minLength": 1, "maxLength": 2048},
            "username": {"type": "string", "minLength": 1, "maxLength": 255},
            "password": {"type": "string", "minLength": 1, "maxLength": 2048},
            "database": {"type": "string", "maxLength": 255, "pattern": "^[A-Za-z0-9_.:-]*$"},
            "source_id": {"type": "string", "minLength": 1, "maxLength": 128, "pattern": "^[A-Za-z0-9_.:-]+$"},
        },
    },
    "disconnect_tool": {
        "type": "object",
        "required": ["tool_name", "source_id"],
        "additionalProperties": False,
        "properties": {
            "tool_name": {"type": "string", "minLength": 1, "maxLength": 64, "pattern": "^[A-Za-z0-9_.:-]+$"},
            "source_id": {"type": "string", "minLength": 1, "maxLength": 128, "pattern": "^[A-Za-z0-9_.:-]+$"},
        },
    },
    "upload_batch_files": {
        "type": "object",
        "required": ["session_id"],
        "additionalProperties": False,
        "properties": {
            "session_id": {"type": "string", "minLength": 1, "maxLength": 128, "pattern": "^[A-Za-z0-9_.:-]+$"},
        },
    },
    "live_batch_files": {
        "type": "object",
        "required": ["id", "session_id"],
        "properties": {
            "id": {"type": "string", "enum": ["search", "create_DF", "stream", "end_session"]},
            "session_id": {"type": "string", "minLength": 1, "maxLength": 128, "pattern": "^[A-Za-z0-9_.:-]+$"},
            "value": {"type": ["object", "array", "string", "number", "boolean", "null"]},
            "kind": {"type": "string", "maxLength": 64},
            "type": {"type": "string", "maxLength": 64},
            "date": {"type": ["string", "null"], "maxLength": 64},
        },
        "additionalProperties": True,
    },
    "graph_link": {
        "type": "object",
        "required": ["id", "source_id"],
        "additionalProperties": False,
        "properties": {
            "id": {"type": "string", "enum": ["link"]},
            "source_id": {"type": "string", "minLength": 1, "maxLength": 128, "pattern": "^[A-Za-z0-9_.:-]+$"},
        },
    },
    "get_graph": {
        "type": "object",
        "required": ["id", "source_id"],
        "additionalProperties": False,
        "properties": {
            "id": {"type": "string", "enum": ["relationship"]},
            "source_id": {"type": "string", "minLength": 1, "maxLength": 128, "pattern": "^[A-Za-z0-9_.:-]+$"},
            "relationship": {"type": "string", "minLength": 1, "maxLength": 128, "pattern": "^(\\*|[A-Za-z0-9_:-]+)$"},
        },
    },
    "socket_connect": {
        "type": "object",
        "required": ["token"],
        "additionalProperties": False,
        "properties": {"token": {"type": "string", "minLength": 1, "maxLength": 8192}},
    },
    "socket_session": {
        "type": "object",
        "required": ["session_id"],
        "additionalProperties": False,
        "properties": {"session_id": {"type": "string", "minLength": 1, "maxLength": 128, "pattern": "^[A-Za-z0-9_.:-]+$"}},
    },
    "socket_log_stream": {
        "type": "object",
        "required": ["session_id", "filename"],
        "additionalProperties": False,
        "properties": {
            "session_id": {"type": "string", "minLength": 1, "maxLength": 128, "pattern": "^[A-Za-z0-9_.:-]+$"},
            "filename": {"type": "string", "minLength": 1, "maxLength": 255, "pattern": "^[A-Za-z0-9_.:-]+$"},
        },
    },
}
