import os

from cryptography.fernet import Fernet

SENSITIVE_KEY_PARTS = ("password", "secret", "token", "credential", "client_secret", "x-api-key", "authorization")
MASKED_SECRET = "***"


def is_sensitive_key(key):
    lowered = str(key or "").lower()
    if lowered.endswith("_ref") or lowered.endswith("_id"):
        return False
    return any(part in lowered for part in SENSITIVE_KEY_PARTS)


def should_store_secret(value):
    if value is None or isinstance(value, (dict, list, tuple, set)):
        return False
    text = str(value)
    return bool(text) and text != MASKED_SECRET


def _fernet():
    key = os.getenv("LINKX_SECRET_ENCRYPTION_KEY")
    if not key:
        raise RuntimeError("LINKX_SECRET_ENCRYPTION_KEY is required to store configuration secrets")
    return Fernet(key.encode("utf-8"))


def encrypt_secret(value):
    return _fernet().encrypt(str(value).encode("utf-8")).decode("utf-8")


def decrypt_secret(value):
    if not value:
        return None
    return _fernet().decrypt(str(value).encode("utf-8")).decode("utf-8")
