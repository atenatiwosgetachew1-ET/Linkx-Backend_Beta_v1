import base64
import hashlib
import hmac
import json
import os
import time
from datetime import datetime, timezone
from typing import Optional, Dict, Any

import jwt
from flask import current_app

from .jwks_client import get_ctms_jwks_client


TOKEN_MAX_AGE_SECONDS = int(os.getenv("LINKX_AUTH_TOKEN_SECONDS", "3600"))
SERVICE_TOKEN_MAX_AGE_SECONDS = int(os.getenv("LINKX_SERVICE_TOKEN_SECONDS", "3600"))


def _expected_issuer():
    return os.getenv("LINKX_AUTH_ISSUER") or None


def _expected_audience():
    return os.getenv("LINKX_AUTH_AUDIENCE") or None


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

        expected_issuer = _expected_issuer()
        if expected_issuer and payload.get("iss") != expected_issuer:
            return None

        expected_audience = _expected_audience()
        if expected_audience:
            audience = payload.get("aud")
            if isinstance(audience, list):
                if expected_audience not in audience:
                    return None
            elif audience != expected_audience:
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
    issuer = _expected_issuer()
    audience = _expected_audience()
    if issuer:
        payload["iss"] = issuer
    if audience:
        payload["aud"] = audience
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
    issuer = _expected_issuer()
    audience = _expected_audience()
    if issuer:
        payload["iss"] = issuer
    if audience:
        payload["aud"] = audience
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


def verify_ctms_token(token: str) -> Optional[Dict[str, Any]]:
    """
    Verify and decode a CTMS JWT token (ES256 signed).
    
    CTMS tokens are signed with ES256 (ECDSA P-256 SHA-256) and must be
    verified against the CTMS public key fetched from the JWKS endpoint.
    
    Validation checks:
    - Algorithm is ES256 (prevents algorithm confusion attacks)
    - Signature is valid using CTMS public key
    - token_type is "access" (rejects refresh tokens)
    - exp (expiration) is in the future
    - sub (subject UUID) is present
    
    Args:
        token: The CTMS JWT token string
    
    Returns:
        Decoded token payload (dict) if valid, None if invalid
    """
    if not token:
        return None
    
    try:
        # Decode header without verification to get kid
        header = jwt.get_unverified_header(token)
        alg = header.get("alg")
        kid = header.get("kid")
        
        # Strict algorithm check: only ES256 allowed
        if alg != "ES256":
            current_app.logger.warning(f"CTMS token rejected: invalid algorithm '{alg}' (expected ES256)")
            return None
        
        # Get CTMS JWKS client
        jwks_client = get_ctms_jwks_client()
        if not jwks_client:
            current_app.logger.warning("CTMS JWKS client not configured")
            return None
        
        # Get public key from JWKS
        try:
            public_key = jwks_client.get_key(kid)
        except Exception as e:
            current_app.logger.warning(f"Failed to get CTMS public key: {e}")
            return None
        
        # Verify signature and decode payload
        payload = jwt.decode(
            token,
            public_key,
            algorithms=["ES256"],  # Only allow ES256
            options={"verify_exp": True}  # Automatically check expiration
        )
        
        # Additional validations
        token_type = payload.get("token_type")
        if token_type != "access":
            current_app.logger.warning(f"CTMS token rejected: token_type '{token_type}' (expected 'access')")
            return None
        
        sub = payload.get("sub")
        if not sub:
            current_app.logger.warning("CTMS token rejected: missing 'sub' (subject UUID)")
            return None
        
        return payload
    
    except jwt.ExpiredSignatureError:
        current_app.logger.warning("CTMS token rejected: expired")
        return None
    except jwt.InvalidSignatureError:
        current_app.logger.warning("CTMS token rejected: invalid signature")
        return None
    except jwt.InvalidAlgorithmError:
        current_app.logger.warning("CTMS token rejected: invalid algorithm")
        return None
    except Exception as e:
        current_app.logger.warning(f"CTMS token verification failed: {e}")
        return None
