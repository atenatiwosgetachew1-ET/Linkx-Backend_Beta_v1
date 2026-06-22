import base64
import json
import ipaddress
import os
import socket
import time
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen

try:
    from cryptography.exceptions import InvalidSignature
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.hazmat.primitives.asymmetric.utils import encode_dss_signature
    from cryptography.hazmat.backends import default_backend
except Exception:  # pragma: no cover - dependency checked at runtime
    InvalidSignature = None
    hashes = None
    serialization = None
    ec = None
    encode_dss_signature = None
    default_backend = None

_JWKS_CACHE = {"url": None, "loaded_at": 0, "keys": None}


def _truthy_env(name):
    return str(os.getenv(name, "")).lower() in {"1", "true", "yes", "on"}


def _network_host(value):
    parsed = urlparse(str(value or ""))
    return (parsed.hostname or "").strip().lower()


def _validate_parent_auth_url(url):
    parsed = urlparse(str(url or ""))
    if parsed.scheme not in {"https", "http"} or not parsed.netloc:
        raise ParentJwtError("parent_auth_url_invalid")
    if parsed.scheme != "https" and not _truthy_env("LINKX_PARENT_AUTH_ALLOW_HTTP"):
        raise ParentJwtError("parent_auth_https_required")

    host = _network_host(url)
    allowed_hosts = {h.strip().lower() for h in os.getenv("LINKX_PARENT_AUTH_ALLOWED_HOSTS", "").split(",") if h.strip()}
    if allowed_hosts and host not in allowed_hosts:
        raise ParentJwtError("parent_auth_host_not_allowed")
    try:
        addresses = socket.getaddrinfo(host, None, type=socket.SOCK_STREAM)
    except socket.gaierror as exc:
        raise ParentJwtError("parent_auth_host_resolution_failed") from exc
    for info in addresses:
        ip = ipaddress.ip_address(info[4][0])
        if ip.is_loopback or ip.is_link_local or ip.is_multicast or ip.is_unspecified:
            raise ParentJwtError("parent_auth_unsafe_host_address")


def _jwks_url():
    explicit = os.getenv("LINKX_PARENT_JWT_JWKS_URL")
    if explicit:
        return explicit
    base_url = os.getenv("LINKX_PARENT_AUTH_BASE_URL")
    if base_url:
        return urljoin(base_url.rstrip("/") + "/", ".well-known/jwks.json")
    return ""


def _fetch_jwks():
    url = _jwks_url()
    if not url:
        return None
    ttl = int(os.getenv("LINKX_PARENT_JWKS_CACHE_SECONDS", "300"))
    now = time.monotonic()
    if _JWKS_CACHE["url"] == url and _JWKS_CACHE["keys"] is not None and now - _JWKS_CACHE["loaded_at"] < ttl:
        return _JWKS_CACHE["keys"]

    _validate_parent_auth_url(url)
    timeout = float(os.getenv("LINKX_PARENT_AUTH_TIMEOUT_SECONDS", "5"))
    req = Request(url, headers={"Accept": "application/json", "User-Agent": "linkx-api-parent-jwks/1.0"})
    try:
        with urlopen(req, timeout=timeout) as response:
            if response.status >= 400:
                raise ParentJwtError("parent_jwks_fetch_failed")
            body = response.read(1024 * 1024)
    except ParentJwtError:
        raise
    except Exception as exc:
        raise ParentJwtError("parent_jwks_fetch_failed") from exc

    try:
        data = json.loads(body.decode("utf-8"))
        keys = data.get("keys") if isinstance(data, dict) else None
        if not isinstance(keys, list):
            raise ValueError("keys list missing")
    except Exception as exc:
        raise ParentJwtError("parent_jwks_invalid") from exc

    _JWKS_CACHE.update({"url": url, "loaded_at": now, "keys": keys})
    return keys


def _jwk_public_key(jwk):
    if ec is None:
        raise ParentJwtError("jwt_crypto_unavailable")
    if jwk.get("kty") != "EC" or jwk.get("crv") not in {"P-256", "secp256r1", "prime256v1"}:
        raise ParentJwtError("parent_jwk_not_p256")
    x = int.from_bytes(_b64decode(jwk.get("x") or ""), "big")
    y = int.from_bytes(_b64decode(jwk.get("y") or ""), "big")
    numbers = ec.EllipticCurvePublicNumbers(x, y, ec.SECP256R1())
    return numbers.public_key(default_backend())


def _load_public_key_from_jwks(kid=None):
    try:
        keys = _fetch_jwks()
    except ParentJwtError:
        keys = None
    if not keys:
        return None
    candidates = []
    for jwk in keys:
        if jwk.get("kty") == "EC" and jwk.get("crv") == "P-256" and (not kid or jwk.get("kid") == kid):
            candidates.append(jwk)
    if not candidates:
        raise ParentJwtError("parent_jwk_not_found")
    return _jwk_public_key(candidates[0])


class ParentJwtError(ValueError):
    pass


def _b64decode(value):
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode((value + padding).encode("ascii"))


def _json_segment(value):
    return json.loads(_b64decode(value).decode("utf-8"))


def _pem_from_env():
    pem = os.getenv("LINKX_PARENT_JWT_PUBLIC_KEY") or ""
    if pem:
        return pem.replace("\\n", "\n").encode("utf-8")
    path = os.getenv("LINKX_PARENT_JWT_PUBLIC_KEY_FILE")
    if path:
        try:
            with open(path, "rb") as fh:
                return fh.read()
        except OSError as exc:
            raise ParentJwtError("parent_public_key_file_unavailable") from exc
    return b""


def _load_public_key(kid=None):
    jwks_key = _load_public_key_from_jwks(kid)
    if jwks_key is not None:
        return jwks_key
    if serialization is None:
        raise ParentJwtError("jwt_crypto_unavailable")
    pem = _pem_from_env()
    if not pem:
        raise ParentJwtError("parent_public_key_not_configured")
    try:
        return serialization.load_pem_public_key(pem)
    except Exception as exc:
        raise ParentJwtError("parent_public_key_invalid") from exc


def _verify_es256(signing_input, signature, kid=None):
    if len(signature) != 64:
        raise ParentJwtError("invalid_signature_format")
    public_key = _load_public_key(kid)
    if not isinstance(public_key, ec.EllipticCurvePublicKey):
        raise ParentJwtError("parent_public_key_not_ec")
    if public_key.curve.name not in {"secp256r1", "prime256v1"}:
        raise ParentJwtError("parent_public_key_not_p256")
    der_signature = encode_dss_signature(
        int.from_bytes(signature[:32], "big"),
        int.from_bytes(signature[32:], "big"),
    )
    try:
        public_key.verify(der_signature, signing_input, ec.ECDSA(hashes.SHA256()))
    except InvalidSignature as exc:
        raise ParentJwtError("invalid_parent_signature") from exc


def _expected_roles_from_payload(payload):
    roles = []
    role = payload.get("role")
    if role:
        roles.append(str(role))
    assignable_roles = payload.get("assignable_roles") or []
    if isinstance(assignable_roles, str):
        assignable_roles = [assignable_roles]
    roles.extend(str(item) for item in assignable_roles if item)
    return roles


def verify_parent_access_token(token):
    try:
        header_segment, payload_segment, signature_segment = str(token or "").split(".")
    except ValueError as exc:
        raise ParentJwtError("malformed_parent_token") from exc

    header = _json_segment(header_segment)
    if header.get("alg") != "ES256" or header.get("typ") != "JWT":
        raise ParentJwtError("unsupported_parent_token_header")

    signing_input = f"{header_segment}.{payload_segment}".encode("ascii")
    _verify_es256(signing_input, _b64decode(signature_segment), header.get("kid"))

    payload = _json_segment(payload_segment)
    now = int(time.time())
    leeway = int(os.getenv("LINKX_PARENT_JWT_LEEWAY_SECONDS", "30"))

    if payload.get("token_type") != "access":
        raise ParentJwtError("parent_access_token_required")
    exp = payload.get("exp")
    if exp is None or int(exp) < now - leeway:
        raise ParentJwtError("parent_token_expired")
    iat = payload.get("iat")
    if iat is not None and int(iat) > now + leeway:
        raise ParentJwtError("parent_token_iat_in_future")

    issuer = os.getenv("LINKX_PARENT_JWT_ISSUER")
    if issuer and payload.get("iss") != issuer:
        raise ParentJwtError("parent_issuer_mismatch")
    audience = os.getenv("LINKX_PARENT_JWT_AUDIENCE")
    if audience:
        aud = payload.get("aud")
        if isinstance(aud, list):
            valid_audience = audience in aud
        else:
            valid_audience = aud == audience
        if not valid_audience:
            raise ParentJwtError("parent_audience_mismatch")

    subject = str(payload.get("sub") or "").strip()
    if not subject:
        raise ParentJwtError("parent_subject_missing")

    return {
        "sub": subject,
        "username": f"parent:{subject}",
        "display_name": payload.get("name") or payload.get("display_name") or subject,
        "roles": _expected_roles_from_payload(payload),
        "permissions": payload.get("permissions") or [],
        "claims": payload,
    }
