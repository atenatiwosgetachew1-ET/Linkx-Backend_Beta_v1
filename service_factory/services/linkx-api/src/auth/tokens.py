import base64
import hashlib
import hmac
import json
import os
import time
from datetime import datetime, timezone

from flask import current_app


TOKEN_MAX_AGE_SECONDS = int(os.getenv("LINKX_AUTH_TOKEN_SECONDS", "3600"))
SERVICE_TOKEN_MAX_AGE_SECONDS = int(os.getenv("LINKX_SERVICE_TOKEN_SECONDS", "3600"))


def _secret_key():
    return str(current_app.config.get("SECRET_KEY") or current_app.secret_key).encode("utf-8")


def _b64encode(value):
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _b64decode(value):
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode((value + padding).encode("ascii"))


def _jwt_encode(payload):
    header = {"alg": "HS256", "typ": "JWT"}
    header_segment = _b64encode(json.dumps(header, separators=(",", ":")).encode("utf-8"))
    payload_segment = _b64encode(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    signing_input = f"{header_segment}.{payload_segment}".encode("ascii")
    signature = hmac.new(_secret_key(), signing_input, hashlib.sha256).digest()
    return f"{header_segment}.{payload_segment}.{_b64encode(signature)}"


def _jwt_decode(token):
    try:
        header_segment, payload_segment, signature_segment = token.split(".")
        signing_input = f"{header_segment}.{payload_segment}".encode("ascii")
        expected = hmac.new(_secret_key(), signing_input, hashlib.sha256).digest()
        actual = _b64decode(signature_segment)
        if not hmac.compare_digest(expected, actual):
            return None

        header = json.loads(_b64decode(header_segment))
        if header.get("alg") != "HS256" or header.get("typ") != "JWT":
            return None

        payload = json.loads(_b64decode(payload_segment))
        exp = payload.get("exp")
        if exp is not None and int(exp) < int(time.time()):
            return None
        return payload
    except Exception:
        return None


def create_access_token(user):
    now = int(time.time())
    payload = {
        "sub": str(user["id"]),
        "actor_type": "user",
        "username": user["username"],
        "iat": now,
        "exp": now + TOKEN_MAX_AGE_SECONDS,
        "issued_at": datetime.now(timezone.utc).isoformat(),
    }
    return _jwt_encode(payload)


def create_service_token(service):
    now = int(time.time())
    payload = {
        "sub": str(service["id"]),
        "actor_type": "service",
        "client_id": service["client_id"],
        "iat": now,
        "exp": now + SERVICE_TOKEN_MAX_AGE_SECONDS,
        "issued_at": datetime.now(timezone.utc).isoformat(),
    }
    return _jwt_encode(payload)


def verify_access_token(token):
    if not token:
        return None
    return _jwt_decode(token)


def extract_bearer_token(auth_header):
    prefix = "Bearer "
    if not auth_header or not auth_header.startswith(prefix):
        return None
    return auth_header[len(prefix):].strip()
