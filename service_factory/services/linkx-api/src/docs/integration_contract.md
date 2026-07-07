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

## Parent Project OAuth Code Exchange

Preferred browser SSO flow:

```http
POST /auth/exchange
Content-Type: application/json
```

Compatibility alias for frontend integrations that use an `/api` prefix:

```http
POST /api/auth/exchange
Content-Type: application/json
```

Request:

```json
{
  "code": "<authorization-code-from-parent-project>",
  "code_verifier": "<pkce-code-verifier>",
  "redirect_uri": "https://172.27.23.21/auth/callback"
}
```

Backend behavior:

- LinkX exchanges the authorization code with the Parent project token endpoint server-side.
- LinkX sends the PKCE `code_verifier`; the frontend must never send a client secret.
- LinkX calls Parent project `userinfo` and treats that response as the identity and authorization source.
- LinkX maps Parent project Link Analysis permissions to LinkX roles, creates or updates the local LinkX user for session/config ownership, stores Parent project tokens encrypted server-side, and returns a LinkX JWT.
- Browser clients continue using the LinkX token on later calls as `Authorization: Bearer <linkx_token>`.

Response:

```json
{
  "message": "success",
  "token": "<linkx_user_jwt>",
  "access_token": "<linkx_user_jwt>",
  "token_type": "Bearer",
  "actor": {
    "actor_type": "user",
    "username": "parent:<parent-sub>",
    "roles": ["analyst"]
  },
  "parent": {
    "sub": "<parent-sub>",
    "roles": ["ANALYST"],
    "mapped_roles": ["analyst"]
  }
}
```

Required API environment:

```text
LINKX_PARENT_SSO_TOKEN_URL
LINKX_PARENT_SSO_USERINFO_URL
LINKX_PARENT_SSO_REVOKE_URL
LINKX_PARENT_OAUTH_CLIENT_ID
LINKX_PARENT_OAUTH_CLIENT_SECRET
LINKX_PARENT_OAUTH_REDIRECT_URI
LINKX_PARENT_OAUTH_ALLOWED_REDIRECT_URIS
LINKX_PARENT_JWKS_URL
LINKX_PARENT_JWT_ISSUER      required when parent tokens include iss
LINKX_PARENT_JWT_AUDIENCE    required when parent tokens include aud
LINKX_PARENT_REQUIRE_LINKX_PERMISSION=true
LINKX_PARENT_PERMISSION_READ=LinkAnalysisRead
LINKX_PARENT_PERMISSION_MANAGE=LinkAnalysisManage
LINKX_PARENT_FRAME_ORIGIN    optional iframe embedding origin
```

Authorization mapping currently performed by LinkX:

```text
LinkAnalysisManage -> analyst
LinkAnalysisRead   -> viewer
no Link Analysis permission -> rejected
```

Parent project role names are recorded for audit metadata, but they do not grant LinkX access while `LINKX_PARENT_REQUIRE_LINKX_PERMISSION=true`. This keeps LinkX aligned with the Parent project scoped-permission contract and prevents broad role names from creating local access accidentally.

## Parent Project Direct Token Exchange

For rollback or non-browser integrations, LinkX still accepts a verified Parent project access JWT:

```http
POST /auth/parent-token
Content-Type: application/json
```

Body:

```json
{
  "access_token": "<parent-project-es256-access-jwt>"
}
```

Legacy shared-secret parent federation remains disabled unless `LINKX_ENABLE_LEGACY_PARENT_TOKEN=true` is explicitly approved.

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

## Authorization Mapping

LinkX currently grants access from Parent project Link Analysis permissions, not broad Parent project role names:

```text
LinkAnalysisManage -> analyst
LinkAnalysisRead   -> viewer
no Link Analysis permission -> rejected
```

Parent project role names may still be returned and are stored in audit metadata, but they do not grant LinkX access while `LINKX_PARENT_REQUIRE_LINKX_PERMISSION=true`.

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

Server-side LinkX backend values:

```text
LINKX_BASE_URL
LINKX_PUBLIC_API_KEY                   optional, only for public STR API protection
LINKX_AUTH_TOKEN_SECONDS               optional user token lifetime
LINKX_SERVICE_TOKEN_SECONDS            optional service token lifetime
LINKX_PARENT_SSO_TOKEN_URL             required for /auth/exchange
LINKX_PARENT_SSO_USERINFO_URL          required for /auth/exchange
LINKX_PARENT_SSO_REVOKE_URL            recommended for logout cleanup
LINKX_PARENT_OAUTH_CLIENT_ID           required for /auth/exchange
LINKX_PARENT_OAUTH_CLIENT_SECRET       required for /auth/exchange
LINKX_PARENT_OAUTH_REDIRECT_URI        required unless request always supplies exact redirect
LINKX_PARENT_OAUTH_ALLOWED_REDIRECT_URIS recommended allow-list
LINKX_PARENT_JWKS_URL                  required for Parent project token verification
LINKX_PARENT_JWT_ISSUER                required when parent tokens include iss
LINKX_PARENT_JWT_AUDIENCE              required when parent tokens include aud
LINKX_PARENT_REQUIRE_LINKX_PERMISSION  recommended true
LINKX_PARENT_PERMISSION_READ           default LinkAnalysisRead
LINKX_PARENT_PERMISSION_MANAGE         default LinkAnalysisManage
```

Rollback/direct-token route only:

```text
LINKX_ENABLE_LEGACY_PARENT_TOKEN=false
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
