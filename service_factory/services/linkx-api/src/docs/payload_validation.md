# Payload Sanitization And Validation

This page documents the backend-side payload validation layer used by Linkx. It is mandatory for protected request handlers and public JSON endpoints that accept structured input.

## Location

```text
security/payload_validation.py
```

The layer exposes:

```text
validate_json_payload(schema, required=True, error_message="validation_error", include_detail=True)
validated_json()
COMMON_SCHEMAS
```

Routes decorate handlers with `validate_json_payload(...)`, then read sanitized input through `validated_json()` instead of `request.get_json()`.

## What It Enforces

- Request body must be JSON when the endpoint requires JSON.
- JSON body must be an object, not an array/string/number.
- Unknown fields are rejected on the schemas that set `additionalProperties: false`.
- Required fields must be present.
- Field types, min/max length, enums, and simple identifier patterns are enforced.
- Unsafe control characters are rejected from all strings.
- Non-sensitive string values are trimmed before route logic runs.
- Sensitive values such as `password`, `client_secret`, `token`, and `*_secret` are not stripped, so secrets are validated without accidental mutation.

## Response Shape

Most validation failures return HTTP `400`:

```json
{
  "message": "validation_error",
  "detail": "username: bad value does not match the expected pattern",
  "field": "username"
}
```

Some legacy public endpoints keep their older response body for compatibility. For example, `/api/STR_link_analysis` still returns:

```json
{
  "message": "failed!"
}
```

## Covered Endpoints

| Endpoint | Schema |
| --- | --- |
| `POST /auth/login` | `login` |
| `POST /auth/service-token` | `service_token` |
| `POST /auth/exchange` | `parent_oauth_exchange` |
| `POST /api/auth/exchange` | `parent_oauth_exchange` |
| `POST /auth/parent-token` | `parent_token` |
| `POST /auth/verify` | `verify` |
| `POST /auth/admin/service-accounts` | `service_account_create` |
| `PATCH /auth/admin/service-accounts/<id>` | `service_account_update` |
| `POST /auth/admin/users` | `user_create` |
| `PATCH /auth/admin/users/<id>` | `user_update` |
| `POST /init` | `init` |
| `POST /api/STR_link_analysis` | `str_link_analysis` |

## Integration Notes

- Clients should send `Content-Type: application/json` for all JSON endpoints.
- Do not send unused extra keys. They are rejected to reduce attack surface and contract drift.
- Use `Authorization: Bearer <token>` for protected endpoints before payload validation becomes relevant.
- Admin/user management screens should show validation errors from `detail` when available.
- Sibling services should keep service-account payloads small and explicit: `client_id`, `client_secret`, and the requested API payload only.

## Developer Pattern

```python
from security.payload_validation import COMMON_SCHEMAS, validate_json_payload, validated_json

@app.route("/example", methods=["POST"])
@validate_json_payload(COMMON_SCHEMAS["example"])
def example():
    data = validated_json()
    ...
```

Add a new schema in `COMMON_SCHEMAS` before accepting a new structured payload. The backend route should not consume raw `request.get_json()` after validation is added.
