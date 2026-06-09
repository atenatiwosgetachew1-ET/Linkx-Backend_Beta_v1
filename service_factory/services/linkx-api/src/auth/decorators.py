from functools import wraps

from flask import g, jsonify, request

from .repository import (
    actor_has_permission,
    get_service_account_by_id,
    get_user_by_id,
)
from .tokens import extract_bearer_token, verify_access_token


def current_actor_from_request():
    if getattr(g, "current_actor", None):
        return g.current_actor

    token = extract_bearer_token(request.headers.get("Authorization"))
    payload = verify_access_token(token)
    if not payload:
        return None

    actor_type = payload.get("actor_type") or "user"
    if actor_type == "service":
        actor = get_service_account_by_id(payload.get("sub"))
    else:
        actor = get_user_by_id(payload.get("sub"))

    if actor:
        g.current_actor = actor
        if actor.get("actor_type") == "user":
            g.current_user = actor
    return actor


def current_user_from_request():
    actor = current_actor_from_request()
    if actor and actor.get("actor_type") == "user":
        return actor
    return None


def auth_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        actor = current_actor_from_request()
        if not actor:
            return jsonify({"message": "unauthorized"}), 401
        return fn(*args, **kwargs)
    return wrapper


def login_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        user = current_user_from_request()
        if not user:
            return jsonify({"message": "unauthorized"}), 401
        return fn(*args, **kwargs)
    return wrapper


def permission_required(permission):
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            actor = current_actor_from_request()
            if not actor:
                return jsonify({"message": "unauthorized"}), 401
            if not actor_has_permission(actor, permission):
                return jsonify({"message": "forbidden", "permission": permission}), 403
            return fn(*args, **kwargs)
        return wrapper
    return decorator
