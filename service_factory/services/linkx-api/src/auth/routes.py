import hmac
import os
import time
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
    get_service_account_by_id,
    get_user_by_id,
    list_service_accounts,
    list_users,
    public_actor,
    update_service_account,
    update_user,
    upsert_external_user,
)
from .tokens import create_access_token, create_service_token, verify_access_token, verify_ctms_token
from .parent_jwt import ParentJwtError, verify_parent_access_token
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
    return jsonify({
        "message": "success",
        "results": {
            "status": "logged_out",
            "reason": reason,
            "cleanup_requested": bool(cancellations),
            "cancelled_sessions": len(cancellations),
            "stopped_api_sessions": len(stopped_sessions),
            "unlocked_count": len(unlocked_locks),
            "token_invalidated": False,
            "token_invalidation_detail": "access tokens are stateless JWTs in the current auth implementation",
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
    return jsonify({
        "message": "success",
        "results": {
            "status": "expired",
            "reason": reason,
            "cleanup_requested": cleanup_requested,
            "cancelled_sessions": len(cancellations),
            "stopped_api_sessions": len(stopped_sessions),
            "unlocked_count": len(unlocked_locks),
            "token_invalidated": False,
            "token_invalidation_detail": "access tokens are stateless JWTs in the current auth implementation",
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
        return jsonify({"message": "invalid_credentials"}), 401

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
        return jsonify({"message": "invalid_client_credentials"}), 401

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


def _map_ctms_roles_to_linkx(ctms_roles):
    """
    Map CTMS role names to LinkX role names.
    
    CTMS role hierarchy (provided by parent system):
    - SUPER_ADMIN, HIGHER_OFFICIAL → admin
    - DIRECTOR, TEAM_LEADER → manager
    - ANALYST → analyst
    - VIEWER, DATA_ENCODER → viewer
    
    This aligns with LinkX ROLE_ALIASES which uses:
    "admin", "manager", "analyst", "viewer"
    """
    ctms_role_mapping = {
        "SUPER_ADMIN": "admin",
        "HIGHER_OFFICIAL": "admin",
        "DIRECTOR": "manager",
        "TEAM_LEADER": "manager",
        "ANALYST": "analyst",
        "VIEWER": "viewer",
        "DATA_ENCODER": "viewer",
    }
    
    linkx_roles = set()
    for ctms_role in (ctms_roles or []):
        mapped = ctms_role_mapping.get(ctms_role)
        if mapped:
            linkx_roles.add(mapped)
    
    # Ensure at least viewer role if something was provided
    if ctms_roles and not linkx_roles:
        linkx_roles.add("viewer")
    
    return sorted(linkx_roles) if linkx_roles else []


@auth_api.route("/parent-token", methods=["POST"])
@_rate_limited("auth_parent_token", limit_env="LINKX_PARENT_TOKEN_RATE_LIMIT", window_env="LINKX_PARENT_TOKEN_RATE_WINDOW_SECONDS", default_limit=30, default_window=60)
@validate_json_payload(COMMON_SCHEMAS["parent_token"], required=False)
def parent_token():
    data = validated_json() or {}
    parent_access_token = data.get("access_token") or data.get("token")
    bearer_header = request.headers.get("Authorization") or ""
    if bearer_header.startswith("Bearer "):
        parent_access_token = parent_access_token or bearer_header[len("Bearer "):].strip()

    # Mode 1: CTMS ES256 token
    if parent_access_token:
        # Try CTMS verification first
        ctms_payload = verify_ctms_token(parent_access_token)
        if ctms_payload:
            # Extract CTMS user info
            sub = ctms_payload.get("sub")
            username = f"parent:{sub}"  # Create parent namespace username
            display_name = ctms_payload.get("name") or ctms_payload.get("sub")
            
            # Map CTMS roles to LinkX roles
            ctms_roles = ctms_payload.get("roles") or []
            parent_roles = _map_ctms_roles_to_linkx(ctms_roles)
            
            user = upsert_external_user(
                username,
                display_name=display_name,
                parent_roles=parent_roles,
            )
            return jsonify({
                "message": "success",
                "token": create_access_token(user),
                "actor": public_actor(user),
                "user": public_actor(user),
                "parent": {
                    "sub": sub,
                    "roles": ctms_roles,
                    "mapped_roles": parent_roles,
                },
            }), 200
        
        # Fallback to legacy parent JWT verification
        try:
            identity = verify_parent_access_token(parent_access_token)
        except ParentJwtError as exc:
            return _parent_token_error(exc)
        user = upsert_external_user(
            identity["username"],
            display_name=identity["display_name"],
            parent_roles=identity["roles"],
        )
        return jsonify({
            "message": "success",
            "token": create_access_token(user),
            "actor": public_actor(user),
            "user": public_actor(user),
            "parent": {
                "sub": identity["sub"],
                "role": identity["claims"].get("role"),
                "token_type": identity["claims"].get("token_type"),
            },
        }), 200

    # Mode 2: Legacy HMAC header mode
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
        return jsonify({"message": "not_found"}), 404

    return jsonify({
        "message": "success",
        "result": public_actor(service),
    }), 200


@auth_api.route("/admin/service-accounts/<int:service_id>", methods=["DELETE"])
@permission_required("users:manage")
def admin_delete_service_account(service_id):
    if not delete_service_account(service_id):
        return jsonify({"message": "not_found"}), 404
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
        return jsonify({"message": "not_found"}), 404

    return jsonify({
        "message": "success",
        "result": public_actor(user),
    }), 200


@auth_api.route("/admin/users/<int:user_id>", methods=["DELETE"])
@permission_required("users:manage")
def admin_delete_user(user_id):
    if not delete_user(user_id):
        return jsonify({"message": "not_found"}), 404
    return jsonify({"message": "success"}), 200
