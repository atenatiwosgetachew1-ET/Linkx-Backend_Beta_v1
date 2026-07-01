import os
from urllib.parse import urljoin

import requests

from .parent_jwt import ParentJwtError, _validate_parent_auth_url


class ParentOAuthError(ValueError):
    pass


def _truthy_env(name):
    return str(os.getenv(name, "")).lower() in {"1", "true", "yes", "on"}


def _base_url():
    return (os.getenv("LINKX_PARENT_SSO_BASE_URL") or os.getenv("LINKX_PARENT_AUTH_BASE_URL") or "").rstrip("/")


def _url(env_name, default_path):
    explicit = os.getenv(env_name)
    if explicit:
        return explicit
    base = _base_url()
    if not base:
        return ""
    return urljoin(base + "/", default_path.lstrip("/"))


def token_url():
    return _url("LINKX_PARENT_SSO_TOKEN_URL", "/sso/token")


def userinfo_url():
    return _url("LINKX_PARENT_SSO_USERINFO_URL", "/sso/userinfo")


def revoke_url():
    return _url("LINKX_PARENT_SSO_REVOKE_URL", "/sso/revoke")


def client_id():
    return os.getenv("LINKX_PARENT_OAUTH_CLIENT_ID") or os.getenv("OAUTH_CLIENT_ID") or ""


def client_secret():
    return os.getenv("LINKX_PARENT_OAUTH_CLIENT_SECRET") or os.getenv("OAUTH_CLIENT_SECRET") or ""


def default_redirect_uri():
    return os.getenv("LINKX_PARENT_OAUTH_REDIRECT_URI") or os.getenv("LINKX_CALLBACK_URL") or ""


def allowed_redirect_uris():
    raw = os.getenv("LINKX_PARENT_OAUTH_ALLOWED_REDIRECT_URIS") or default_redirect_uri()
    return {item.strip() for item in raw.split(",") if item.strip()}


def timeout_seconds():
    try:
        return float(os.getenv("LINKX_PARENT_AUTH_TIMEOUT_SECONDS", "5"))
    except (TypeError, ValueError):
        return 5.0


def validate_redirect_uri(redirect_uri):
    value = str(redirect_uri or default_redirect_uri() or "").strip()
    allowed = allowed_redirect_uris()
    if not value:
        raise ParentOAuthError("parent_redirect_uri_required")
    if allowed and value not in allowed:
        raise ParentOAuthError("parent_redirect_uri_not_allowed")
    return value


def _require_oauth_config():
    missing = []
    if not token_url():
        missing.append("LINKX_PARENT_SSO_TOKEN_URL")
    if not userinfo_url():
        missing.append("LINKX_PARENT_SSO_USERINFO_URL")
    if not client_id():
        missing.append("LINKX_PARENT_OAUTH_CLIENT_ID")
    if not client_secret():
        missing.append("LINKX_PARENT_OAUTH_CLIENT_SECRET")
    if missing:
        raise ParentOAuthError("parent_oauth_not_configured:" + ",".join(missing))


def _validate_url(url):
    try:
        _validate_parent_auth_url(url)
    except ParentJwtError as exc:
        raise ParentOAuthError(str(exc)) from exc


def exchange_authorization_code(code, code_verifier, redirect_uri=None):
    _require_oauth_config()
    redirect_uri = validate_redirect_uri(redirect_uri)
    payload = {
        "grant_type": "authorization_code",
        "code": str(code or ""),
        "redirect_uri": redirect_uri,
        "client_id": client_id(),
        "client_secret": client_secret(),
        "code_verifier": str(code_verifier or ""),
    }
    if not payload["code"] or not payload["code_verifier"]:
        raise ParentOAuthError("parent_code_and_verifier_required")
    return _post_token(payload)


def refresh_access_token(refresh_token):
    _require_oauth_config()
    payload = {
        "grant_type": "refresh_token",
        "refresh_token": str(refresh_token or ""),
        "client_id": client_id(),
        "client_secret": client_secret(),
    }
    if not payload["refresh_token"]:
        raise ParentOAuthError("parent_refresh_token_required")
    return _post_token(payload)


def _post_token(payload):
    url = token_url()
    _validate_url(url)
    try:
        response = requests.post(url, json=payload, timeout=timeout_seconds())
    except requests.RequestException as exc:
        raise ParentOAuthError("parent_token_endpoint_unreachable") from exc
    if response.status_code >= 500:
        raise ParentOAuthError("parent_token_endpoint_error")
    if response.status_code >= 400:
        raise ParentOAuthError("parent_token_exchange_rejected")
    try:
        data = response.json()
    except ValueError as exc:
        raise ParentOAuthError("parent_token_invalid_response") from exc
    if not isinstance(data, dict) or not data.get("access_token"):
        raise ParentOAuthError("parent_access_token_missing")
    return data


def fetch_userinfo(access_token):
    url = userinfo_url()
    if not url:
        raise ParentOAuthError("parent_userinfo_not_configured")
    _validate_url(url)
    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
    try:
        response = requests.get(url, headers=headers, timeout=timeout_seconds())
    except requests.RequestException as exc:
        raise ParentOAuthError("parent_userinfo_unreachable") from exc
    if response.status_code >= 500:
        raise ParentOAuthError("parent_userinfo_error")
    if response.status_code >= 400:
        raise ParentOAuthError("parent_userinfo_rejected")
    try:
        data = response.json()
    except ValueError as exc:
        raise ParentOAuthError("parent_userinfo_invalid_response") from exc
    if not isinstance(data, dict) or not data.get("sub"):
        raise ParentOAuthError("parent_userinfo_missing_subject")
    return data


def revoke_token(token):
    url = revoke_url()
    if not url or not token:
        return False
    _validate_url(url)
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    payload = {"client_id": client_id()}
    try:
        response = requests.post(url, json=payload, headers=headers, timeout=timeout_seconds())
    except requests.RequestException as exc:
        raise ParentOAuthError("parent_revoke_unreachable") from exc
    if response.status_code >= 500:
        raise ParentOAuthError("parent_revoke_error")
    if response.status_code >= 400:
        raise ParentOAuthError("parent_revoke_rejected")
    return True


def oauth_enabled():
    return _truthy_env("LINKX_PARENT_OAUTH_ENABLED") or bool(token_url() and userinfo_url() and client_id() and client_secret())
