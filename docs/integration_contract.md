# Linkx Integration Contract

This document lists the parameters, endpoints, headers, and permission names that parent-project and sibling-service developers need in order to integrate with the Linkx backend.

## Base URL

```text
LINKX_BASE_URL=http://<linkx-host>:8000
```

## Machine-Readable API And Gateway Docs

Swagger/OpenAPI import file:

```text
docs/openapi.json
```

API gateway route/auth guide:

```text
docs/api_gateway_config.md
```

Payload validation and sanitization contract:

```text
docs/payload_validation.md
```

Nginx gateway template:

```text
deploy/nginx/linkx-api-gateway.conf
```

## Human Login

For parent project users:

```http
POST /auth/login
Content-Type: application/json
```

Body:

```json
{
  "username": "admin",
  "password": "<admin-password>"
}
```

Response:

```json
{
  "message": "success!",
  "token": "<user_token>",
  "actor": {
    "id": 1,
    "actor_type": "user",
    "username": "admin",
    "display_name": "admin",
    "roles": ["admin"],
    "permissions": ["session:create", "graph:read"]
  }
}
```

Use the token on protected calls:

```http
Authorization: Bearer <user_token>
```



## Frontend SSO Code Exchange

For the frontend SSO redirect/postMessage flow:

```http
POST /auth/sso/exchange
Content-Type: application/json
```

Request:

```json
{
  "code": "<one-time-code-from-parent-project>",
  "state": "<csrf-state-or-correlation-id>",
  "client": "linkx_frontend",
  "redirect_uri": "https://linkx.example.com/path"
}
```

Response shape matches /auth/login:

```json
{
  "message": "success!",
  "token": "<linkx_user_token>",
  "user": {
    "id": 1,
    "username": "user",
    "display_name": "User",
    "roles": ["analyst"],
    "permissions": ["graph:read"]
  }
}
```

Backend behavior:

- Linkx validates code, state, client, and redirect_uri with the parent backend over server-to-server HTTP.
- Linkx does not trust frontend-decoded JWTs.
- Linkx stores a hash of each exchanged code in PostgreSQL and rejects replay with 409 sso_code_already_used.
- Code lifetime is controlled by LINKX_SSO_CODE_TTL_SECONDS, clamped to 30-300 seconds. Use 30-120 seconds in production.
- Parent roles are mapped with the existing Linkx mapping: team_leader -> admin, analyst -> analyst, viewer -> viewer.

Required/optional config:

```text
LINKX_PARENT_SSO_EXCHANGE_URL       required parent server-to-server validation URL
LINKX_PARENT_SSO_INTROSPECTION_URL  fallback alias if EXCHANGE_URL is not set
LINKX_SSO_ALLOWED_CLIENTS           optional comma list, default linkx_frontend
LINKX_SSO_CODE_TTL_SECONDS          optional, default 120
LINKX_SSO_CODE_HASH_SECRET          optional HMAC secret for stored code hashes
LINKX_PARENT_SSO_CLIENT_ID          optional parent client id header
LINKX_PARENT_SSO_CLIENT_SECRET      optional parent client secret header
LINKX_PARENT_SSO_BEARER_TOKEN       optional bearer token for parent validation
LINKX_PARENT_SSO_TIMEOUT_SECONDS    optional, default 5
```

## Parent Role Exchange

For the parent project or parent gateway to exchange a verified parent identity for a Linkx token:

```http
POST /auth/parent-token
Content-Type: application/json
X-Linkx-Parent-Secret: <LINKX_PARENT_SHARED_SECRET>
```

Body:

```json
{
  "username": "user@example.com",
  "display_name": "User Name",
  "roles": ["team_leader"]
}
```

Role mapping performed by Linkx:

```text
superuser -> top-level Linkx operator
team_leader -> admin
analyst     -> analyst
viewer      -> viewer
```

Response:

```json
{
  "message": "success!",
  "token": "<user_jwt>",
  "actor": {
    "actor_type": "user",
    "username": "user@example.com",
    "roles": ["admin"],
    "permissions": ["users:manage"]
  }
}
```

This endpoint should only be called by the trusted parent project/gateway after it has already authenticated the user.

## Service Login

For sibling microservices:

```http
POST /auth/service-token
Content-Type: application/json
```

Body:

```json
{
  "client_id": "reporting_service",
  "client_secret": "<service-secret>"
}
```

Response:

```json
{
  "message": "success!",
  "token": "<service_token>",
  "actor": {
    "id": 2,
    "actor_type": "service",
    "client_id": "reporting_service",
    "display_name": "Reporting Service",
    "roles": ["service_account"],
    "permissions": ["session:read", "graph:read", "reports:read"]
  }
}
```

Use the service token on protected calls:

```http
Authorization: Bearer <service_token>
```

## Verify Token

```http
POST /auth/verify
Authorization: Bearer <token>
```

Or send the token in the body:

```json
{
  "token": "<token>"
}
```

Response:

```json
{
  "message": "success!",
  "actor": {
    "actor_type": "user or service",
    "permissions": []
  }
}
```

## Current Actor

```http
GET /auth/me
Authorization: Bearer <token>
```

Returns the current user or service actor.

## Initialize Linkx Session

```http
POST /init
Authorization: Bearer <token>
Content-Type: application/json
```

Body:

```json
{
  "id": "init",
  "existing_session": "optional-existing-linkx-session-id"
}
```

Response:

```json
{
  "results": "499767",
  "configurations": {},
  "message": "success!"
}
```

Important:

```text
results = Linkx session_id
```

Store the Linkx `session_id` separately from the auth token.

## Socket.IO

Connect with:

```js
io(LINKX_BASE_URL, {
  auth: { token }
});
```

Missing or invalid tokens are rejected by the backend.

## STR Link Analysis API

```http
POST /api/STR_link_analysis
Content-Type: application/json
```

Optional API key if configured:

```http
X-API-Key: <LINKX_PUBLIC_API_KEY>
```

Body:

```json
{
  "entity": "bank",
  "type": "account_number",
  "value": "5642153",
  "session_id": "optional-linkx-session-id",
  "date": "optional-date"
}
```

Success response:

```json
{
  "message": "success!",
  "session_id": "str_report_or_given_session",
  "wait_for_prepare": false,
  "socket_emit": []
}
```

## Role Mapping

The parent project should map roles like this:

```text
superuser -> top-level Linkx operator
team_leader -> admin
analyst     -> analyst
viewer      -> viewer
```

## Permission Names

Share these exact permission keys:

```text
auth:verify
session:create
session:read
config:read
config:write
source:create
source:connect
source:disconnect
graph:create
graph:read
graph:link
batch:upload
batch:query
analysis:run
reports:read
users:manage
```

## Admin Service Account Management

Only admin/team-leader tokens can call these endpoints.

```http
GET /auth/admin/service-accounts
Authorization: Bearer <admin_token>
```

```http
POST /auth/admin/service-accounts
Authorization: Bearer <admin_token>
Content-Type: application/json
```

Body:

```json
{
  "client_id": "reporting_service",
  "client_secret": "store-this-in-reporting-service",
  "display_name": "Reporting Service",
  "permissions": ["session:read", "graph:read", "reports:read"]
}
```

Update service account:

```http
PATCH /auth/admin/service-accounts/<id>
Authorization: Bearer <admin_token>
Content-Type: application/json
```

Body:

```json
{
  "permissions": ["session:read", "graph:read", "reports:read"],
  "is_active": true
}
```

Rotate service secret:

```json
{
  "client_secret": "new-secret"
}
```

Delete service account:

```http
DELETE /auth/admin/service-accounts/<id>
Authorization: Bearer <admin_token>
```

## Environment Variables To Share

```text
LINKX_BASE_URL
LINKX_PUBLIC_API_KEY              optional, only for public STR API protection
LINKX_AUTH_TOKEN_SECONDS          optional user token lifetime
LINKX_SERVICE_TOKEN_SECONDS       optional service token lifetime
LINKX_PARENT_SHARED_SECRET        required for /auth/parent-token
```

For each sibling service:

```text
LINKX_CLIENT_ID=<service-client-id>
LINKX_CLIENT_SECRET=<service-client-secret>
LINKX_BASE_URL=http://<linkx-host>:8000
```

## Important Rule

```text
Auth token identifies the caller. Tokens are HS256 JWT bearer tokens signed by Linkx.
Permissions define what the caller can do.
Linkx session_id identifies the analysis workspace.
Do not mix auth token and Linkx session_id.
```


## Admin User Management

Admin/team-leader tokens can manage analyst and viewer users. Superuser tokens can manage any user role, including admin and superuser.

```http
GET /auth/admin/users
Authorization: Bearer <admin_or_superuser_token>
```

```http
POST /auth/admin/users
Authorization: Bearer <admin_or_superuser_token>
Content-Type: application/json
```

Body:

```json
{
  "username": "link",
  "password": "temporary-password",
  "display_name": "Analyst One",
  "roles": ["analyst"],
  "is_active": true
}
```

Update user:

```http
PATCH /auth/admin/users/<id>
Authorization: Bearer <admin_or_superuser_token>
Content-Type: application/json
```

Delete user:

```http
DELETE /auth/admin/users/<id>
Authorization: Bearer <admin_or_superuser_token>
```

Role scope:

```text
superuser can create/update/delete any role.
admin can create/update/delete analyst and viewer accounts.
```
