import hmac
import os

import requests
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
    reserve_sso_code_exchange,
    update_service_account,
    update_user,
    upsert_external_user,
)
from .tokens import create_access_token, create_service_token, verify_access_token, verify_ctms_token
from security.payload_validation import COMMON_SCHEMAS, validate_json_payload, validated_json


auth_api = Blueprint("auth_api", __name__)


def _allowed_sso_clients():
    raw = os.getenv("LINKX_SSO_ALLOWED_CLIENTS", "linkx_frontend")
    return {item.strip() for item in raw.split(",") if item.strip()}


def _parent_sso_url():
    return os.getenv("LINKX_PARENT_SSO_EXCHANGE_URL") or os.getenv("LINKX_PARENT_SSO_INTROSPECTION_URL")


def _validate_parent_sso_code(code, state, client, redirect_uri):
    parent_url = _parent_sso_url()
    if not parent_url:
        return None, {"message": "sso_disabled"}, 503

    payload = {
        "code": code,
        "state": state,
        "client": client,
        "redirect_uri": redirect_uri,
    }
    headers = {"Accept": "application/json"}
    parent_client_id = os.getenv("LINKX_PARENT_SSO_CLIENT_ID")
    parent_client_secret = os.getenv("LINKX_PARENT_SSO_CLIENT_SECRET")
    bearer_token = os.getenv("LINKX_PARENT_SSO_BEARER_TOKEN")
    if parent_client_id:
        headers["X-Linkx-Client-Id"] = parent_client_id
    if parent_client_secret:
        headers["X-Linkx-Client-Secret"] = parent_client_secret
    if bearer_token:
        headers["Authorization"] = f"Bearer {bearer_token}"

    timeout = float(os.getenv("LINKX_PARENT_SSO_TIMEOUT_SECONDS", "5"))
    try:
        response = requests.post(parent_url, json=payload, headers=headers, timeout=timeout)
    except requests.RequestException:
        return None, {"message": "sso_parent_unreachable"}, 502

    if response.status_code >= 500:
        return None, {"message": "sso_parent_error"}, 502
    if response.status_code in {400, 401, 403, 404, 409, 410, 422}:
        return None, {"message": "invalid_sso_code"}, 401
    if response.status_code >= 400:
        return None, {"message": "sso_parent_rejected"}, 401

    try:
        parent_data = response.json()
    except ValueError:
        return None, {"message": "sso_parent_invalid_response"}, 502

    valid = parent_data.get("valid")
    active = parent_data.get("active")
    if valid is False or active is False:
        return None, {"message": "invalid_sso_code"}, 401
    return parent_data, None, None


def _parent_user_identity(parent_data):
    user_data = parent_data.get("user") if isinstance(parent_data.get("user"), dict) else parent_data
    claims = parent_data.get("claims") if isinstance(parent_data.get("claims"), dict) else {}
    username = (
        user_data.get("username")
        or user_data.get("preferred_username")
        or user_data.get("email")
        or user_data.get("sub")
        or claims.get("username")
        or claims.get("preferred_username")
        or claims.get("email")
        or claims.get("sub")
    )
    display_name = (
        user_data.get("display_name")
        or user_data.get("name")
        or claims.get("display_name")
        or claims.get("name")
        or username
    )
    roles = (
        parent_data.get("roles")
        or parent_data.get("parent_roles")
        or user_data.get("roles")
        or user_data.get("parent_roles")
        or claims.get("roles")
        or claims.get("parent_roles")
        or []
    )
    if isinstance(roles, str):
        roles = [roles]
    return str(username or "").strip(), display_name, roles


@auth_api.route("/login", methods=["POST"])
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
        "message": "success!",
        "token": create_access_token(user),
        "actor": public_actor(user),
        "user": public_actor(user),
    }), 200


@auth_api.route("/service-token", methods=["POST"])
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
        "message": "success!",
        "token": create_service_token(service),
        "actor": public_actor(service),
    }), 200


@auth_api.route("/parent-token", methods=["POST"])
@validate_json_payload(COMMON_SCHEMAS["parent_token"])
def parent_token():
    """
    Exchange parent (CTMS) credentials for LinkX session token.
    
    Supports two modes:
    1. CTMS Mode (ES256 JWT):
       - POST with {"access_token": "<CTMS_ES256_JWT>"}
       - Validates CTMS JWT signature against JWKS endpoint
       - Extracts identity and roles from JWT claims
       - Maps CTMS roles to LinkX roles
    
    2. Legacy Mode (HMAC-based federation):
       - POST with {"username": "...", "roles": [...]}
       - Validates X-Linkx-Parent-Secret header (HMAC)
       - For backwards compatibility with existing parent projects
    
    Returns LinkX HS256 access token on success.
    """
    data = validated_json()
    
    # Mode 1: CTMS ES256 JWT token
    if "access_token" in data:
        return _handle_ctms_token(data.get("access_token"))
    
    # Mode 2: Legacy HMAC-based parent federation
    return _handle_legacy_parent_token(data)


def _handle_ctms_token(access_token: str):
    """Handle CTMS ES256 JWT token exchange."""
    if not access_token:
        return jsonify({"error": "access_token is required"}), 400
    
    # Verify CTMS token (ES256 JWT)
    payload = verify_ctms_token(access_token)
    if not payload:
        return jsonify({"error": "Invalid parent token"}), 401
    
    # Validate token_type
    if payload.get("token_type") != "access":
        return jsonify({"error": "Only access tokens accepted"}), 400
    
    # Validate exp
    import time
    if payload.get("exp") and payload.get("exp") < time.time():
        return jsonify({"error": "Parent token expired"}), 401
    
    # Extract identity
    sub = payload.get("sub")  # CTMS user UUID
    if not sub:
        return jsonify({"error": "Missing user identity (sub)"}), 401
    
    # Extract roles from CTMS token
    ctms_roles = payload.get("roles") or []
    if isinstance(ctms_roles, str):
        ctms_roles = [ctms_roles]
    
    # Optional: Check permissions array for finer control
    permissions = payload.get("permissions") or []
    
    # Map CTMS roles to LinkX roles
    linkx_roles = []
    for role in ctms_roles:
        role_str = str(role).strip()
        # Role mapping happens in upsert_external_user via normalize_parent_role
        linkx_roles.append(role_str)
    
    # Create or update LinkX user with CTMS identity
    username = f"parent:{sub}"  # Prefix to indicate external user
    display_name = payload.get("name") or sub
    
    try:
        user = upsert_external_user(
            username,
            display_name=display_name,
            parent_roles=linkx_roles,
        )
    except Exception as e:
        from flask import current_app
        current_app.logger.error(f"Failed to create CTMS user: {e}")
        return jsonify({"error": "Failed to create session"}), 500
    
    return jsonify({
        "message": "success",
        "token": create_access_token(user),
        "actor": public_actor(user),
        "user": public_actor(user),
        "parent": {
            "sub": sub,
            "role": ctms_roles[0] if ctms_roles else None,
            "token_type": "access"
        }
    }), 200


def _handle_legacy_parent_token(data: dict):
    """Handle legacy HMAC-based parent federation."""
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
        "message": "success!",
        "token": create_access_token(user),
        "actor": public_actor(user),
        "user": public_actor(user),
    }), 200


@auth_api.route("/sso/exchange", methods=["POST"])
@validate_json_payload(COMMON_SCHEMAS["sso_exchange"])
def sso_exchange():
    data = validated_json()
    code = str(data.get("code") or "").strip()
    state = str(data.get("state") or "").strip()
    client = str(data.get("client") or "").strip()
    redirect_uri = str(data.get("redirect_uri") or "").strip()

    if client not in _allowed_sso_clients():
        return jsonify({"message": "unauthorized_client"}), 403
    if not _parent_sso_url():
        return jsonify({"message": "sso_disabled"}), 503

    try:
        ttl_seconds = int(os.getenv("LINKX_SSO_CODE_TTL_SECONDS", "120"))
    except (TypeError, ValueError):
        ttl_seconds = 120
    if not reserve_sso_code_exchange(code, state=state, client=client, ttl_seconds=ttl_seconds):
        return jsonify({"message": "sso_code_already_used"}), 409

    parent_data, error_body, status = _validate_parent_sso_code(code, state, client, redirect_uri)
    if error_body:
        return jsonify(error_body), status

    parent_state = parent_data.get("state") if isinstance(parent_data, dict) else None
    if parent_state and not hmac.compare_digest(str(parent_state), state):
        return jsonify({"message": "invalid_sso_state"}), 401

    username, display_name, roles = _parent_user_identity(parent_data)
    if not username:
        return jsonify({"message": "sso_parent_missing_identity"}), 502
    if not isinstance(roles, list):
        return jsonify({"message": "sso_parent_invalid_roles"}), 502

    user = upsert_external_user(username, display_name=display_name, parent_roles=roles)
    public = public_actor(user)
    return jsonify({
        "message": "success!",
        "token": create_access_token(user),
        "actor": public,
        "user": public,
    }), 200


@auth_api.route("/me", methods=["GET"])
@auth_required
def me():
    actor = current_actor_from_request()
    payload = {
        "message": "success!",
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
        response = {"message": "success!", "actor": public_actor(actor)}
        if actor.get("actor_type") == "user":
            response["user"] = response["actor"]
        return jsonify(response), 200

    actor = current_actor_from_request()
    if not actor:
        return jsonify({"message": "unauthorized"}), 401

    response = {"message": "success!", "actor": public_actor(actor)}
    if actor.get("actor_type") == "user":
        response["user"] = response["actor"]
    return jsonify(response), 200


@auth_api.route("/admin/service-accounts", methods=["GET"])
@permission_required("users:manage")
def admin_list_service_accounts():
    return jsonify({
        "message": "success!",
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
        "message": "success!",
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
        "message": "success!",
        "result": public_actor(service),
    }), 200


@auth_api.route("/admin/service-accounts/<int:service_id>", methods=["DELETE"])
@permission_required("users:manage")
def admin_delete_service_account(service_id):
    if not delete_service_account(service_id):
        return jsonify({"message": "not_found"}), 404
    return jsonify({"message": "success!"}), 200


@auth_api.route("/admin/users", methods=["GET"])
@permission_required("users:manage")
def admin_list_users():
    return jsonify({
        "message": "success!",
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
        "message": "success!",
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
        "message": "success!",
        "result": public_actor(user),
    }), 200


@auth_api.route("/admin/users/<int:user_id>", methods=["DELETE"])
@permission_required("users:manage")
def admin_delete_user(user_id):
    if not delete_user(user_id):
        return jsonify({"message": "not_found"}), 404
    return jsonify({"message": "success!"}), 200
