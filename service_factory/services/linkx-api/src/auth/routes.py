import hmac
import os
import time
from datetime import datetime, timedelta, timezone
from collections import defaultdict, deque
from functools import wraps

from flask import Blueprint, jsonify, request

from .decorators import auth_required, current_actor_from_request, permission_required
from .repository import (
    authenticate_service_account,
    actor_can_manage_roles,
    authenticate_user,
    create_or_update_service_account,
    create_or_update_user,
    delete_service_account,
    delete_user,
    get_parent_oauth_session,
    get_service_account_by_id,
    get_user_by_id,
    list_security_audit_events,
    list_service_accounts,
    list_users,
    public_actor,
    record_security_event,
    revoke_parent_oauth_session,
    revoke_token_jti,
    update_service_account,
    update_user,
    upsert_parent_oauth_session,
    upsert_external_user,
)
from .tokens import create_access_token, create_service_token, extract_bearer_token, verify_access_token, verify_parent_project_token
from .parent_jwt import ParentJwtError, verify_parent_access_token
from .parent_oauth import ParentOAuthError, exchange_authorization_code, fetch_userinfo, refresh_access_token, revoke_token
from security.payload_validation import COMMON_SCHEMAS, validate_json_payload, validated_json
from globals import _session_store
from batch_manager.processing.session_manager import end_session
from service_orchestration import get_actor_active_session_ids, lock_actor_session, public_lock_state, request_session_cancellation, unlock_actor_locks
from session_config_store import get_user_preferences, save_user_preferences


auth_api = Blueprint("auth_api", __name__)


_RATE_LIMIT_BUCKETS = defaultdict(deque)


def _rate_limited(name, *, limit_env, window_env, default_limit, default_window):
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            limit = _env_int(limit_env, default_limit)
            window_seconds = _env_int(window_env, default_window)
            if limit <= 0 or window_seconds <= 0:
                return fn(*args, **kwargs)
            client_ip = request.remote_addr or "unknown"
            key = (name, client_ip)
            now = time.monotonic()
            bucket = _RATE_LIMIT_BUCKETS[key]
            cutoff = now - window_seconds
            while bucket and bucket[0] <= cutoff:
                bucket.popleft()
            if len(bucket) >= limit:
                retry_after = max(1, int(window_seconds - (now - bucket[0])))
                response = jsonify({"message": "rate_limited", "retry_after": retry_after})
                response.headers["Retry-After"] = str(retry_after)
                return response, 429
            bucket.append(now)
            return fn(*args, **kwargs)
        return wrapper
    return decorator


def _env_int(name, default):
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return int(default)


def _audit_context():
    return {
        "ip_address": request.headers.get("X-Forwarded-For", request.remote_addr or "").split(",")[0].strip() or None,
        "user_agent": (request.headers.get("User-Agent") or "")[:512] or None,
    }


def _record_security_event_safe(event_type, **kwargs):
    try:
        ctx = _audit_context()
        kwargs.setdefault("ip_address", ctx["ip_address"])
        kwargs.setdefault("user_agent", ctx["user_agent"])
        return record_security_event(event_type, **kwargs)
    except Exception:
        return None


def _revoke_current_bearer_token(actor, reason):
    token = extract_bearer_token(request.headers.get("Authorization"))
    payload = verify_access_token(token, check_revocation=False)
    if not payload:
        return False, "current_token_unavailable"
    jti = payload.get("jti")
    if not jti:
        return False, "current_token_has_no_jti"
    expires_at = None
    try:
        expires_at = datetime.fromtimestamp(int(payload.get("exp")), timezone.utc)
    except Exception:
        expires_at = None
    revoked = revoke_token_jti(jti, actor=actor, reason=reason, expires_at=expires_at)
    _record_security_event_safe(
        "auth.token_revoke",
        actor=actor,
        target_type="auth_token",
        target_id=jti,
        success=revoked,
        metadata={"reason": reason, "expires_at": expires_at.isoformat() if expires_at else None},
    )
    return revoked, "revoked" if revoked else "revoke_failed"


def _idle_policy():
    idle_lock_ms = _env_int("LINKX_IDLE_LOCK_MS", _env_int("LINKX_IDLE_TIMEOUT_MS", 900000))
    idle_warning_ms = _env_int("LINKX_IDLE_WARNING_MS", max(0, idle_lock_ms - 60000))
    max_idle_timeout_ms = _env_int("LINKX_MAX_IDLE_TIMEOUT_MS", 3600000)
    lock_requires_reauth = str(os.getenv("LINKX_LOCK_REQUIRES_REAUTH", "true")).lower() not in {"0", "false", "no"}
    return {
        "idle_warning_ms": idle_warning_ms,
        "idle_lock_ms": idle_lock_ms,
        "max_idle_timeout_ms": max_idle_timeout_ms,
        "lock_requires_reauth": lock_requires_reauth,
    }


def _session_tree_keys(session_id):
    base = str(session_id or "")
    keys = []
    for key in list(_session_store.keys()):
        raw = str(key)
        if raw == base or raw.endswith(f"_{base}"):
            keys.append(raw)
    return keys


def _stop_api_memory_sessions(session_id, reason):
    stopped = []
    for key in _session_tree_keys(session_id):
        result = end_session({"session_id": key, "reason": reason})
        stopped.append({"session_id": key, "result": result})
    return stopped


def _parent_access_token_is_fresh(parent_session):
    expires_at = (parent_session or {}).get("access_token_expires_at")
    if not expires_at:
        return False
    try:
        parsed = datetime.fromisoformat(str(expires_at).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
    except ValueError:
        return False
    return parsed > datetime.now(timezone.utc) + timedelta(seconds=30)


def _refresh_parent_session_access_token(actor, parent_session):
    refresh_token = (parent_session or {}).get("refresh_token")
    if not refresh_token:
        return None
    token_data = refresh_access_token(refresh_token)
    upsert_parent_oauth_session(
        actor.get("id"),
        (parent_session or {}).get("parent_subject"),
        refresh_token=token_data.get("refresh_token"),
        access_token=token_data.get("access_token"),
        expires_in=token_data.get("expires_in"),
        metadata={**((parent_session or {}).get("metadata") or {}), "source": "refresh_token"},
    )
    return token_data.get("access_token")


def _parent_revoke_access_token(actor, parent_session):
    if not parent_session:
        return False
    access_token = parent_session.get("access_token")
    if not access_token or not _parent_access_token_is_fresh(parent_session):
        access_token = _refresh_parent_session_access_token(actor, parent_session)
    if not access_token:
        return False
    return revoke_token(access_token)


@auth_api.route("/session-policy", methods=["GET"])
@auth_required
def session_policy():
    return jsonify({"message": "success", "results": _idle_policy()}), 200


@auth_api.route("/lock", methods=["POST"])
@auth_required
@validate_json_payload(COMMON_SCHEMAS["lock_session"])
def lock_session_endpoint():
    actor = current_actor_from_request()
    data = validated_json() or {}
    reason = data.get("reason") or "idle_lock"
    lock = lock_actor_session(actor=actor, reason=reason)
    return jsonify({
        "message": "success",
        "results": {
            "status": "locked",
            "reason": reason,
            "lock": public_lock_state(lock),
        },
    }), 200


@auth_api.route("/unlock", methods=["POST"])
@auth_required
@validate_json_payload(COMMON_SCHEMAS["unlock_session"])
def unlock_session():
    actor = current_actor_from_request()
    data = validated_json() or {}
    reason = data.get("reason") or "idle_lock"
    unlocked_locks = unlock_actor_locks(actor=actor, reason=reason)
    return jsonify({
        "message": "success",
        "results": {
            "status": "active",
            "reason": reason,
            "unlocked_count": len(unlocked_locks),
            "token": None,
            "token_refreshed": False,
        },
    }), 200


@auth_api.route("/logout", methods=["POST"])
@auth_required
@validate_json_payload(COMMON_SCHEMAS["logout"])
def logout():
    actor = current_actor_from_request()
    data = validated_json() or {}
    reason = data.get("reason") or "user_logout"
    sessions = get_actor_active_session_ids(actor, limit=50)
    stopped_sessions = []
    cancellations = []
    for session_id in sessions:
        stopped_sessions.extend(_stop_api_memory_sessions(session_id, reason))
        cancellations.append(request_session_cancellation(
            session_id,
            reason=reason,
            requested_by=f"{actor.get('actor_type')}:{actor.get('id')}",
            neo4j_credentials=None,
        ))
    unlocked_locks = unlock_actor_locks(actor=actor, reason=reason)
    token_invalidated, token_invalidation_detail = _revoke_current_bearer_token(actor, reason)
    parent_revoked = False
    parent_revoke_error = None
    if actor.get("actor_type") == "user":
        parent_session = get_parent_oauth_session(actor.get("id"), include_refresh_token=True, include_access_token=True)
        if parent_session:
            try:
                parent_revoked = _parent_revoke_access_token(actor, parent_session)
            except ParentOAuthError as exc:
                parent_revoke_error = str(exc)
            revoke_parent_oauth_session(actor.get("id"))
            _record_security_event_safe(
                "auth.parent_revoke",
                actor=actor,
                target_type="parent_user",
                target_id=(parent_session or {}).get("parent_subject"),
                success=parent_revoke_error is None,
                metadata={"error": parent_revoke_error} if parent_revoke_error else {},
            )
    return jsonify({
        "message": "success",
        "results": {
            "status": "logged_out",
            "reason": reason,
            "cleanup_requested": bool(cancellations),
            "cancelled_sessions": len(cancellations),
            "stopped_api_sessions": len(stopped_sessions),
            "unlocked_count": len(unlocked_locks),
            "parent_session_revoked": parent_revoked,
            "parent_revoke_error": parent_revoke_error,
            "token_invalidated": token_invalidated,
            "token_invalidation_detail": token_invalidation_detail,
        },
    }), 202


@auth_api.route("/idle-timeout", methods=["POST"])
@auth_required
@validate_json_payload(COMMON_SCHEMAS["idle_timeout"])
def idle_timeout():
    actor = current_actor_from_request()
    data = validated_json() or {}
    reason = data.get("reason") or "max_idle_expired"
    cleanup_requested = data.get("cleanup", True) is not False
    sessions = get_actor_active_session_ids(actor, limit=50)
    stopped_sessions = []
    cancellations = []
    for session_id in sessions:
        stopped_sessions.extend(_stop_api_memory_sessions(session_id, reason))
        if cleanup_requested:
            cancellations.append(request_session_cancellation(
                session_id,
                reason=reason,
                requested_by=f"{actor.get('actor_type')}:{actor.get('id')}",
                neo4j_credentials=None,
            ))
    unlocked_locks = unlock_actor_locks(actor=actor, reason=reason)
    token_invalidated, token_invalidation_detail = _revoke_current_bearer_token(actor, reason)
    return jsonify({
        "message": "success",
        "results": {
            "status": "expired",
            "reason": reason,
            "cleanup_requested": cleanup_requested,
            "cancelled_sessions": len(cancellations),
            "stopped_api_sessions": len(stopped_sessions),
            "unlocked_count": len(unlocked_locks),
            "token_invalidated": token_invalidated,
            "token_invalidation_detail": token_invalidation_detail,
        },
    }), 202


@auth_api.route("/login", methods=["POST"])
@_rate_limited("auth_login", limit_env="LINKX_LOGIN_RATE_LIMIT", window_env="LINKX_LOGIN_RATE_WINDOW_SECONDS", default_limit=5, default_window=60)
@validate_json_payload(COMMON_SCHEMAS["login"])
def login():
    data = validated_json()
    username = str(data.get("username") or "").strip()
    password = str(data.get("password") or "")
    if not username or not password:
        return jsonify({"message": "missing_credentials"}), 400

    user = authenticate_user(username, password)
    if not user:
        _record_security_event_safe("auth.login", username=username, success=False)
        return jsonify({"message": "invalid_credentials"}), 401

    _record_security_event_safe("auth.login", actor=user, username=username, success=True)
    return jsonify({
        "message": "success",
        "token": create_access_token(user),
        "actor": public_actor(user),
        "user": public_actor(user),
    }), 200


@auth_api.route("/service-token", methods=["POST"])
@_rate_limited("auth_service_token", limit_env="LINKX_SERVICE_TOKEN_RATE_LIMIT", window_env="LINKX_SERVICE_TOKEN_RATE_WINDOW_SECONDS", default_limit=10, default_window=60)
@validate_json_payload(COMMON_SCHEMAS["service_token"])
def service_token():
    data = validated_json()
    client_id = str(data.get("client_id") or "").strip()
    client_secret = str(data.get("client_secret") or "")
    if not client_id or not client_secret:
        return jsonify({"message": "missing_client_credentials"}), 400

    service = authenticate_service_account(client_id, client_secret)
    if not service:
        _record_security_event_safe("auth.service_token", username=client_id, target_type="service_account", success=False)
        return jsonify({"message": "invalid_client_credentials"}), 401

    _record_security_event_safe("auth.service_token", actor=service, username=client_id, target_type="service_account", target_id=service.get("id"), success=True)
    return jsonify({
        "message": "success",
        "token": create_service_token(service),
        "actor": public_actor(service),
    }), 200


def _parent_token_error(error):
    mapping = {
        "parent_access_token_required": (400, "Only access tokens accepted"),
        "unsupported_parent_token_header": (401, "Invalid algorithm"),
        "parent_token_expired": (401, "Parent token expired"),
        "invalid_parent_signature": (401, "Invalid parent token"),
        "parent_subject_invalid": (401, "Invalid parent token"),
    }
    status, message = mapping.get(str(error), (401, "Invalid parent token"))
    return jsonify({"error": message}), status



def _parent_oauth_error_response(error):
    mapping = {
        "parent_oauth_not_configured": (503, "parent_oauth_disabled"),
        "parent_redirect_uri_required": (400, "redirect_uri_required"),
        "parent_redirect_uri_not_allowed": (400, "invalid_redirect_uri"),
        "parent_code_and_verifier_required": (400, "code_and_verifier_required"),
        "parent_token_exchange_rejected": (401, "invalid_authorization_code"),
        "parent_access_token_missing": (502, "parent_access_token_missing"),
        "parent_userinfo_rejected": (401, "parent_userinfo_rejected"),
        "parent_userinfo_missing_subject": (502, "parent_userinfo_missing_subject"),
    }
    key = str(error).split(":", 1)[0]
    status, message = mapping.get(key, (502, key or "parent_oauth_error"))
    return jsonify({"message": message}), status


def _parent_display_name(parent_data, fallback):
    return (
        parent_data.get("full_name")
        or parent_data.get("display_name")
        or parent_data.get("name")
        or parent_data.get("email")
        or parent_data.get("username")
        or fallback
    )


def _parent_roles_from_claims(parent_data):
    roles = []
    if parent_data.get("role"):
        roles.append(str(parent_data.get("role")))
    raw_roles = parent_data.get("roles") or parent_data.get("parent_roles") or []
    if isinstance(raw_roles, str):
        raw_roles = [raw_roles]
    roles.extend(str(role) for role in raw_roles if role)
    return roles


def _map_parent_permissions_to_roles(permissions):
    values = {str(permission) for permission in (permissions or []) if permission}
    manage_permission = os.getenv("LINKX_PARENT_PERMISSION_MANAGE", "LinkAnalysisManage")
    read_permission = os.getenv("LINKX_PARENT_PERMISSION_READ", "LinkAnalysisRead")
    if manage_permission and manage_permission in values:
        return ["analyst"]
    if read_permission and read_permission in values:
        return ["viewer"]
    return []


def _map_parent_roles_to_linkx(parent_roles, permissions=None):
    mapped_from_permissions = _map_parent_permissions_to_roles(permissions)
    if mapped_from_permissions:
        return mapped_from_permissions

    if str(os.getenv("LINKX_PARENT_REQUIRE_LINKX_PERMISSION", "true")).lower() not in {"0", "false", "no"}:
        return []

    role_mapping = {
        "SUPER_ADMIN": "admin",
        "ADMIN": "admin",
        "HIGHER_OFFICIAL": "admin",
        "DIRECTOR": "manager",
        "TEAM_LEADER": "manager",
        "ANALYST": "analyst",
        "VIEWER": "viewer",
        "DATA_ENCODER": "viewer",
        "RECEIVING_OFFICER": None,
        "team_leader": "manager",
        "analyst": "analyst",
        "viewer": "viewer",
    }
    mapped = []
    for role in parent_roles or []:
        value = str(role).strip()
        linkx_role = role_mapping.get(value) or role_mapping.get(value.upper())
        if linkx_role and linkx_role not in mapped:
            mapped.append(linkx_role)
    if not mapped:
        mapped.extend(role for role in _map_parent_permissions_to_roles(permissions) if role not in mapped)
    return mapped


def _issue_parent_linkx_token(parent_data, token_data=None, source="parent_token"):
    if parent_data.get("is_active") is False:
        return jsonify({"message": "parent_user_inactive"}), 403

    sub = str(parent_data.get("sub") or "").strip()
    if not sub:
        return jsonify({"message": "parent_subject_missing"}), 401

    parent_roles = _parent_roles_from_claims(parent_data)
    permissions = parent_data.get("permissions") or []
    linkx_roles = _map_parent_roles_to_linkx(parent_roles, permissions)
    if not linkx_roles:
        return jsonify({"message": "parent_role_not_authorized"}), 403

    username = f"parent:{sub}"
    user = upsert_external_user(
        username,
        display_name=_parent_display_name(parent_data, sub),
        parent_roles=linkx_roles,
    )

    if token_data is not None:
        upsert_parent_oauth_session(
            user.get("id"),
            sub,
            refresh_token=token_data.get("refresh_token"),
            access_token=token_data.get("access_token"),
            expires_in=token_data.get("expires_in"),
            metadata={
                "source": source,
                "scope": token_data.get("scope"),
                "token_type": token_data.get("token_type"),
                "parent_roles": parent_roles,
                "mapped_roles": linkx_roles,
                "entity_id": parent_data.get("entity_id"),
                "branch_id": parent_data.get("branch_id"),
                "team_id": parent_data.get("team_id"),
            },
        )

    _record_security_event_safe(
        "auth.parent_oauth" if token_data is not None else "auth.parent_token",
        actor=user,
        username=username,
        target_type="parent_user",
        target_id=sub,
        success=True,
        metadata={
            "source": source,
            "roles": parent_roles,
            "mapped_roles": linkx_roles,
            "permissions": permissions,
        },
    )
    token = create_access_token(user)
    public = public_actor(user)
    return jsonify({
        "message": "success",
        "token": token,
        "access_token": token,
        "token_type": "Bearer",
        "actor": public,
        "user": public,
        "parent": {
            "sub": sub,
            "roles": parent_roles,
            "mapped_roles": linkx_roles,
            "entity_id": parent_data.get("entity_id"),
            "branch_id": parent_data.get("branch_id"),
            "team_id": parent_data.get("team_id"),
        },
    }), 200


@auth_api.route("/exchange", methods=["POST"])
@_rate_limited("auth_parent_oauth_exchange", limit_env="LINKX_PARENT_OAUTH_EXCHANGE_RATE_LIMIT", window_env="LINKX_PARENT_OAUTH_EXCHANGE_RATE_WINDOW_SECONDS", default_limit=30, default_window=60)
@validate_json_payload(COMMON_SCHEMAS["parent_oauth_exchange"])
def exchange_parent_oauth_code():
    data = validated_json() or {}
    try:
        token_data = exchange_authorization_code(
            data.get("code"),
            data.get("code_verifier"),
            redirect_uri=data.get("redirect_uri"),
        )
        parent_data = fetch_userinfo(token_data.get("access_token"))
    except ParentOAuthError as exc:
        _record_security_event_safe("auth.parent_oauth", success=False, metadata={"error": str(exc)})
        return _parent_oauth_error_response(exc)
    return _issue_parent_linkx_token(parent_data, token_data=token_data, source="authorization_code")

@auth_api.route("/parent-token", methods=["POST"])
@_rate_limited("auth_parent_token", limit_env="LINKX_PARENT_TOKEN_RATE_LIMIT", window_env="LINKX_PARENT_TOKEN_RATE_WINDOW_SECONDS", default_limit=30, default_window=60)
@validate_json_payload(COMMON_SCHEMAS["parent_token"], required=False)
def parent_token():
    data = validated_json() or {}
    parent_access_token = data.get("access_token") or data.get("token")
    bearer_header = request.headers.get("Authorization") or ""
    if bearer_header.startswith("Bearer "):
        parent_access_token = parent_access_token or bearer_header[len("Bearer "):].strip()

    # Mode 1: Parent project ES256 token
    if parent_access_token:
        parent_payload = verify_parent_project_token(parent_access_token)
        if parent_payload:
            return _issue_parent_linkx_token(parent_payload, source="parent_access_token")

        # Fallback to the stricter parent JWT verifier for deployments that use
        # the generic parent key configuration instead of the JWKS helper.
        try:
            identity = verify_parent_access_token(parent_access_token)
        except ParentJwtError as exc:
            _record_security_event_safe("auth.parent_token", success=False, metadata={"error": str(exc)})
            return _parent_token_error(exc)
        return _issue_parent_linkx_token(identity["claims"], source="parent_access_token")

    # Mode 2: Legacy HMAC header mode. Disabled by default for the Parent project integration.
    if not parent_access_token and str(os.getenv("LINKX_ENABLE_LEGACY_PARENT_TOKEN", "")).lower() not in {"1", "true", "yes", "on"}:
        _record_security_event_safe("auth.parent_token", success=False, metadata={"error": "access_token_required"})
        return jsonify({"error": "access_token is required"}), 400
    if not parent_access_token and not data.get("username") and not data.get("sub"):
        return jsonify({"error": "access_token is required"}), 400

    shared_secret = os.getenv("LINKX_PARENT_SHARED_SECRET")
    if not shared_secret:
        return jsonify({"message": "parent_federation_disabled"}), 503

    provided_secret = request.headers.get("X-Linkx-Parent-Secret") or ""
    if not hmac.compare_digest(provided_secret, shared_secret):
        return jsonify({"message": "unauthorized"}), 401

    username = str(data.get("username") or data.get("sub") or "").strip()
    display_name = data.get("display_name") or data.get("name") or username
    roles = data.get("roles") or data.get("parent_roles") or []
    if isinstance(roles, str):
        roles = [roles]

    if not username:
        return jsonify({"message": "missing_username"}), 400
    if not isinstance(roles, list):
        return jsonify({"message": "roles_must_be_list"}), 400

    user = upsert_external_user(username, display_name=display_name, parent_roles=roles)
    return jsonify({
        "message": "success",
        "token": create_access_token(user),
        "actor": public_actor(user),
        "user": public_actor(user),
    }), 200


@auth_api.route("/preferences", methods=["GET"])
@auth_required
def get_preferences():
    actor = current_actor_from_request()
    if not actor or actor.get("actor_type") != "user":
        return jsonify({"message": "user_required"}), 403
    preferences = get_user_preferences(actor.get("id"))
    return jsonify({"message": "success", "results": {"preferences": preferences}}), 200


@auth_api.route("/preferences", methods=["PATCH"])
@auth_required
def patch_preferences():
    actor = current_actor_from_request()
    if not actor or actor.get("actor_type") != "user":
        return jsonify({"message": "user_required"}), 403
    data = request.get_json(silent=True)
    if data is None or not isinstance(data, dict):
        return jsonify({"message": "validation_error", "detail": "json_object_required"}), 400
    preferences = data.get("preferences") if isinstance(data.get("preferences"), dict) else data
    allowed = {"remember_layout", "enable_notifications", "enable_background_animations"}
    unknown = sorted(set(preferences.keys()) - allowed)
    if unknown:
        return jsonify({"message": "validation_error", "detail": "unknown_preferences", "fields": unknown}), 400
    saved = save_user_preferences(actor.get("id"), preferences, merge=True)
    return jsonify({"message": "success", "results": {"preferences": saved}}), 200


@auth_api.route("/me", methods=["GET"])
@auth_required
def me():
    actor = current_actor_from_request()
    payload = {
        "message": "success",
        "actor": public_actor(actor),
    }
    if actor.get("actor_type") == "user":
        payload["user"] = payload["actor"]
    return jsonify(payload), 200


@auth_api.route("/verify", methods=["POST"])
@validate_json_payload(COMMON_SCHEMAS["verify"], required=False)
def verify():
    data = validated_json() or {}
    token = data.get("token")
    if token:
        payload = verify_access_token(token)
        if not payload:
            return jsonify({"message": "unauthorized"}), 401
        actor_type = payload.get("actor_type") or "user"
        actor = get_service_account_by_id(payload.get("sub")) if actor_type == "service" else get_user_by_id(payload.get("sub"))
        if not actor:
            return jsonify({"message": "unauthorized"}), 401
        response = {"message": "success", "actor": public_actor(actor)}
        if actor.get("actor_type") == "user":
            response["user"] = response["actor"]
        return jsonify(response), 200

    actor = current_actor_from_request()
    if not actor:
        return jsonify({"message": "unauthorized"}), 401

    response = {"message": "success", "actor": public_actor(actor)}
    if actor.get("actor_type") == "user":
        response["user"] = response["actor"]
    return jsonify(response), 200


@auth_api.route("/admin/service-accounts", methods=["GET"])
@permission_required("users:manage")
def admin_list_service_accounts():
    return jsonify({
        "message": "success",
        "results": [public_actor(service) for service in list_service_accounts()],
    }), 200


@auth_api.route("/admin/service-accounts", methods=["POST"])
@permission_required("users:manage")
@validate_json_payload(COMMON_SCHEMAS["service_account_create"])
def admin_create_service_account():
    data = validated_json()
    client_id = str(data.get("client_id") or "").strip()
    client_secret = str(data.get("client_secret") or "")
    permissions = data.get("permissions") or []
    display_name = data.get("display_name") or client_id

    if not client_id or not client_secret:
        return jsonify({"message": "missing_client_credentials"}), 400
    if not isinstance(permissions, list):
        return jsonify({"message": "permissions_must_be_list"}), 400

    service = create_or_update_service_account(
        client_id,
        client_secret,
        permissions=permissions,
        display_name=display_name,
    )
    _record_security_event_safe(
        "admin.service_account.create",
        actor=current_actor_from_request(),
        target_type="service_account",
        target_id=service.get("id"),
        username=client_id,
        success=True,
        metadata={"permissions": permissions},
    )
    return jsonify({
        "message": "success",
        "result": public_actor(service),
    }), 201


@auth_api.route("/admin/service-accounts/<int:service_id>", methods=["PATCH"])
@permission_required("users:manage")
@validate_json_payload(COMMON_SCHEMAS["service_account_update"])
def admin_update_service_account(service_id):
    data = validated_json()
    permissions = data.get("permissions") if "permissions" in data else None
    if permissions is not None and not isinstance(permissions, list):
        return jsonify({"message": "permissions_must_be_list"}), 400

    service = update_service_account(
        service_id,
        client_secret=data.get("client_secret"),
        permissions=permissions,
        display_name=data.get("display_name") if "display_name" in data else None,
        is_active=data.get("is_active") if "is_active" in data else None,
    )
    if not service:
        _record_security_event_safe("admin.service_account.update", actor=current_actor_from_request(), target_type="service_account", target_id=service_id, success=False, metadata={"error": "not_found"})
        return jsonify({"message": "not_found"}), 404

    _record_security_event_safe(
        "admin.service_account.update",
        actor=current_actor_from_request(),
        target_type="service_account",
        target_id=service_id,
        success=True,
        metadata={"fields": sorted(data.keys())},
    )
    return jsonify({
        "message": "success",
        "result": public_actor(service),
    }), 200


@auth_api.route("/admin/service-accounts/<int:service_id>", methods=["DELETE"])
@permission_required("users:manage")
def admin_delete_service_account(service_id):
    if not delete_service_account(service_id):
        _record_security_event_safe("admin.service_account.delete", actor=current_actor_from_request(), target_type="service_account", target_id=service_id, success=False, metadata={"error": "not_found"})
        return jsonify({"message": "not_found"}), 404
    _record_security_event_safe("admin.service_account.delete", actor=current_actor_from_request(), target_type="service_account", target_id=service_id, success=True)
    return jsonify({"message": "success"}), 200


@auth_api.route("/admin/users", methods=["GET"])
@permission_required("users:manage")
def admin_list_users():
    return jsonify({
        "message": "success",
        "results": [public_actor(user) for user in list_users()],
    }), 200


@auth_api.route("/admin/users", methods=["POST"])
@permission_required("users:manage")
@validate_json_payload(COMMON_SCHEMAS["user_create"])
def admin_create_user():
    actor = current_actor_from_request()
    data = validated_json()
    username = str(data.get("username") or "").strip()
    password = str(data.get("password") or "")
    roles = data.get("roles") or ["viewer"]
    display_name = data.get("display_name") or username
    is_active = data.get("is_active", True)

    if not username or not password:
        return jsonify({"message": "missing_user_credentials"}), 400
    if isinstance(roles, str):
        roles = [roles]
    if not isinstance(roles, list):
        return jsonify({"message": "roles_must_be_list"}), 400
    if not actor_can_manage_roles(actor, roles):
        return jsonify({"message": "forbidden", "detail": "role_scope_exceeded"}), 403

    user = create_or_update_user(
        username,
        password=password,
        roles=roles,
        display_name=display_name,
        is_active=is_active,
    )
    _record_security_event_safe(
        "admin.user.create",
        actor=actor,
        target_type="user",
        target_id=user.get("id"),
        username=username,
        success=True,
        metadata={"roles": roles, "is_active": bool(is_active)},
    )
    return jsonify({
        "message": "success",
        "result": public_actor(user),
    }), 201


@auth_api.route("/admin/users/<int:user_id>", methods=["PATCH"])
@permission_required("users:manage")
@validate_json_payload(COMMON_SCHEMAS["user_update"])
def admin_update_user(user_id):
    actor = current_actor_from_request()
    data = validated_json()
    roles = data.get("roles") if "roles" in data else None
    if isinstance(roles, str):
        roles = [roles]
    if roles is not None and not isinstance(roles, list):
        return jsonify({"message": "roles_must_be_list"}), 400
    if roles is not None and not actor_can_manage_roles(actor, roles):
        return jsonify({"message": "forbidden", "detail": "role_scope_exceeded"}), 403

    user = update_user(
        user_id,
        password=data.get("password"),
        roles=roles,
        display_name=data.get("display_name") if "display_name" in data else None,
        is_active=data.get("is_active") if "is_active" in data else None,
    )
    if not user:
        _record_security_event_safe("admin.user.update", actor=actor, target_type="user", target_id=user_id, success=False, metadata={"error": "not_found"})
        return jsonify({"message": "not_found"}), 404

    _record_security_event_safe(
        "admin.user.update",
        actor=actor,
        target_type="user",
        target_id=user_id,
        success=True,
        metadata={"fields": sorted(data.keys())},
    )
    return jsonify({
        "message": "success",
        "result": public_actor(user),
    }), 200


@auth_api.route("/admin/users/<int:user_id>", methods=["DELETE"])
@permission_required("users:manage")
def admin_delete_user(user_id):
    if not delete_user(user_id):
        _record_security_event_safe("admin.user.delete", actor=current_actor_from_request(), target_type="user", target_id=user_id, success=False, metadata={"error": "not_found"})
        return jsonify({"message": "not_found"}), 404
    _record_security_event_safe("admin.user.delete", actor=current_actor_from_request(), target_type="user", target_id=user_id, success=True)
    return jsonify({"message": "success"}), 200


@auth_api.route("/admin/audit/security", methods=["GET"])
@permission_required("users:manage")
def admin_security_audit():
    filters = {
        "event_type": request.args.get("event_type"),
        "actor_type": request.args.get("actor_type"),
        "target_type": request.args.get("target_type"),
        "target_id": request.args.get("target_id"),
        "session_id": request.args.get("session_id"),
        "success": request.args.get("success"),
        "limit": request.args.get("limit"),
        "offset": request.args.get("offset"),
    }
    return jsonify({"message": "success", "results": list_security_audit_events(filters)}), 200
