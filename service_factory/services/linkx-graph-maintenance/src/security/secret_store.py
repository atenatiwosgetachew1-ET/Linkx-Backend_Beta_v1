import os

from cryptography.fernet import Fernet

MASKED_SECRET = "***"


def _fernet():
    key = os.getenv("LINKX_SECRET_ENCRYPTION_KEY")
    if not key:
        raise RuntimeError("LINKX_SECRET_ENCRYPTION_KEY is required to decrypt configuration secrets")
    return Fernet(key.encode("utf-8"))


def decrypt_secret(value):
    if not value:
        return None
    return _fernet().decrypt(str(value).encode("utf-8")).decode("utf-8")
