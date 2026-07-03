# Linkx RBAC And Token Workflow

## What A Token Is

A token is an HS256 JWT bearer token that proves a caller has already authenticated. Instead of sending a username/password or client secret on every request, the caller authenticates once, receives a token, then sends it on later requests:

```http
Authorization: Bearer <token>
```

In this backend, tokens are implemented in `auth/tokens.py`.

Human user tokens are created with:

```python
create_access_token(user)
```

Service account tokens are created with:

```python
create_service_token(service)
```

A human token contains an identity payload like:

```json
{
  "sub": "1",
  "actor_type": "user",
  "username": "admin",
  "iat": "..."
}
```

A service token contains an identity payload like:

```json
{
  "sub": "3",
  "actor_type": "service",
  "client_id": "reporting_service",
  "iat": "..."
}
```

The token is an HS256 JWT signed with the Flask secret key. A caller may decode the token, but cannot safely edit it. If it is edited, backend verification fails.

Verification happens through:

```python
verify_access_token(token)
```

## Where Auth Is Implemented

```text
auth/tokens.py
Creates and verifies signed user/service tokens.

auth/routes.py
Exposes auth endpoints:
- POST /auth/login
- POST /auth/service-token
- POST /auth/exchange
- POST /api/auth/exchange
- POST /auth/parent-token
- GET /auth/me
- POST /auth/verify
- GET /auth/admin/service-accounts
- POST /auth/admin/service-accounts
- PATCH /auth/admin/service-accounts/<id>
- DELETE /auth/admin/service-accounts/<id>

auth/repository.py
Stores and reads users, roles, permissions, service accounts, and session ownership in PostgreSQL.

auth/decorators.py
Reads Authorization headers, verifies tokens, loads the actor, and enforces permissions.

main.py
Protects /init and binds Linkx analysis sessions to the authenticated actor.

io_sockets.py
Validates token identity when Socket.IO connects.
```

## Identity Types

There are two actor types:

```text
user
service
```

A `user` is a human working through the parent project or another client.

A `service` is a sibling microservice calling Linkx programmatically.

## Human Roles

Parent project permissions and roles are handled separately.

Authorization mapping currently enforced by LinkX:

```text
LinkAnalysisManage -> analyst
LinkAnalysisRead   -> viewer
no Link Analysis permission -> rejected
```

Parent project role names are retained in local audit metadata, but they do not grant LinkX access while `LINKX_PARENT_REQUIRE_LINKX_PERMISSION=true`. This keeps LinkX access tied to the Parent project scoped-permission contract.

Local Linkx roles:

```text
admin
Full access:
- config:read
- config:write
- source:create
- source:connect
- source:disconnect
- graph:create
- graph:read
- graph:link
- batch:upload
- batch:query
- analysis:run
- reports:read
- users:manage
- session:create
- session:read
```

```text
analyst
Operational access:
- config:read
- source:create
- source:connect
- source:disconnect
- graph:create
- graph:read
- graph:link
- batch:upload
- batch:query
- analysis:run
- reports:read
- session:create
- session:read
```

```text
viewer
Read-only access:
- config:read
- graph:read
- reports:read
- session:read
```

## Service Accounts

Service accounts are for sibling microservices. They should not be treated as human admins or analysts. They get exact permissions for the work they need to do.

Current default service presets:

```text
parent_gateway_service
- auth:verify
- session:create
- session:read
- graph:read
- reports:read
```

```text
reporting_service
- session:read
- graph:read
- reports:read
```

Admins can manage service accounts through the admin endpoints. These endpoints require `users:manage`, so they are intended for the local `admin` role, which maps from parent `team_leader`.

## Human Login Flow

```text
Frontend or parent gateway
  |
  | POST /auth/login
  | username + password
  v
Linkx backend
  |
  | checks users table
  | checks password hash
  | loads roles + permissions
  v
Returns user token
  |
  | Authorization: Bearer <user_token>
  v
Protected Linkx APIs
```


## Parent OAuth Exchange Flow

The Parent project can exchange a verified authorization code for a LinkX user token:

```text
Frontend sends browser user to Parent project authorize endpoint
  |
  | Parent project returns code to LinkX callback
  | POST /auth/exchange or /api/auth/exchange
  | code + code_verifier + redirect_uri
  v
Linkx backend
  |
  | exchanges code server-side with Parent project token endpoint
  | validates returned access token through JWKS rules
  | calls Parent project userinfo
  | maps LinkAnalysisManage/LinkAnalysisRead permissions
  | upserts local user + role assignment + encrypted parent session
  v
Returns Linkx HS256 JWT user token
```

This completes the SSO bridge without requiring the Parent project to share user passwords or client secrets with the browser.

## Service-To-Service Flow

```text
Sibling microservice
  |
  | POST /auth/service-token
  | client_id + client_secret
  v
Linkx backend
  |
  | checks service_accounts table
  | checks secret hash
  | loads service permissions
  v
Returns service token
  |
  | Authorization: Bearer <service_token>
  v
Protected Linkx APIs
```

## Socket.IO Flow

```text
Caller obtains user or service token
  |
  | io(API_URL, { auth: { token } })
  v
io_sockets.py connect handler
  |
  | verifies token
  | loads user or service actor
  v
Socket is accepted or rejected
```

## Session Ownership

Token identity answers: who is calling?

Permissions answer: what can this caller do?

Session ownership answers: which Linkx session can this caller touch?

`/init` binds sessions to the authenticated actor:

```text
user token    -> analysis_sessions.owner_user_id
service token -> analysis_sessions.owner_service_id
```

The `analysis_sessions` table supports:

```text
session_id
owner_user_id
owner_service_id
created_by_type: user | service
created_by_id
parent_session_id
created_at
last_seen_at
```

## Simple Diagram

```text
                  +----------------------+
                  |   Parent project      |
                  |   authorized user     |
                  +----------+-----------+
                             |
                             | auth code + PKCE
                             v
+----------------+  POST /auth/exchange   +----------------------+
| Frontend/GW    | -----------------------> | Linkx Backend Auth   |
| human user     |  code + verifier        | auth/routes.py       |
+-------+--------+                          +----------+-----------+
        |                                              |
        | Authorization: Bearer user_token             |
        v                                              v
+----------------------+                 +-------------------------+
| Protected APIs       |                 | PostgreSQL RBAC Tables |
| /init, graph, batch  |                 | users, roles, perms    |
+----------+-----------+                 | service_accounts       |
           |                             | analysis_sessions      |
           v                             +-------------------------+
+----------------------+
| Linkx Session        |
| owned by user        |
+----------------------+


+-------------------------+
| Sibling Microservice    |
| reporting / parent gw   |
+------------+------------+
             |
             | POST /auth/service-token
             | client_id + client_secret
             v
+-------------------------+
| Linkx Backend Auth      |
| issues service_token    |
+------------+------------+
             |
             | Authorization: Bearer service_token
             v
+-------------------------+
| Protected APIs          |
| session/report/graph    |
+------------+------------+
             |
             v
+-------------------------+
| Linkx Session           |
| owned by service        |
+-------------------------+
```

## Creating A Service Account

Use the backend utility:

```bash
venv/bin/python scripts/create_service_account.py reporting_service
```

It prints a generated `client_secret` once. Store it in the calling service. The secret is hashed in PostgreSQL and cannot be recovered later.

Custom permissions can be granted with repeated `--permission` flags:

```bash
venv/bin/python scripts/create_service_account.py my_service   --permission session:create   --permission session:read   --permission graph:read
```

## Admin Service Account Management API

Admin-only endpoints require `Authorization: Bearer <admin_user_token>` and the `users:manage` permission.

```http
GET /auth/admin/service-accounts
```

```http
POST /auth/admin/service-accounts
{
  "client_id": "reporting_service",
  "client_secret": "store-this-in-the-calling-service",
  "display_name": "Reporting Service",
  "permissions": ["session:read", "graph:read", "reports:read"]
}
```

```http
PATCH /auth/admin/service-accounts/1
{
  "display_name": "Reporting Service",
  "is_active": true,
  "permissions": ["session:read", "graph:read", "reports:read"]
}
```

To rotate a service secret:

```http
PATCH /auth/admin/service-accounts/1
{
  "client_secret": "new-secret-value"
}
```

```http
DELETE /auth/admin/service-accounts/1
```
