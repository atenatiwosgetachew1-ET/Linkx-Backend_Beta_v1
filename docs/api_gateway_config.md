# Linkx API Gateway Configuration

This document describes the route/auth behavior expected at the API gateway layer.

## Gateway Responsibility

The Linkx backend remains the source of truth for JWT validation, RBAC, service-account checks, and session ownership.

The gateway should:

- Route Linkx paths to the backend service.
- Preserve `Authorization: Bearer <JWT>`.
- Preserve `X-Linkx-Parent-Secret` only for `/auth/parent-token`.
- Preserve `X-API-Key` for `/api/STR_link_analysis` if `LINKX_PUBLIC_API_KEY` is configured.
- Support WebSocket/Socket.IO upgrade headers for `/socket.io/`.
- Restrict `/auth/parent-token` to the trusted parent gateway/network.

## Nginx Template

A ready-to-adapt Nginx template is provided here:

```text
deploy/nginx/linkx-api-gateway.conf
```

Update this upstream block for your environment:

```nginx
upstream linkx_backend {
    server 127.0.0.1:8000;
}
```

## Route Matrix

| Route | Gateway Auth Behavior | Backend Auth Behavior |
|---|---|---|
| `GET /db/health` | Public/internal health route | Checks PostgreSQL health |
| `POST /auth/login` | Public route | Validates username/password |
| `POST /auth/service-token` | Internal/sibling-service route | Validates client id/secret |
| `POST /auth/parent-token` | Parent gateway only, preserve `X-Linkx-Parent-Secret` | Validates parent shared secret and maps roles |
| `GET /auth/me` | Preserve `Authorization` | Validates JWT |
| `POST /auth/verify` | Preserve `Authorization` | Validates JWT/body token |
| `/auth/admin/*` | Preserve `Authorization` | Requires `users:manage` |
| `POST /init` | Preserve `Authorization` | Requires authenticated actor |
| `POST /api/STR_link_analysis` | Preserve optional `X-API-Key` | Validates API key only if configured |
| `/socket.io/*` | Preserve upgrade headers | Validates Socket.IO auth token |

## Required Headers

JWT-protected routes:

```http
Authorization: Bearer <jwt>
```

Parent-token exchange:

```http
X-Linkx-Parent-Secret: <LINKX_PARENT_SHARED_SECRET>
```

Public STR API if API key is enabled:

```http
X-API-Key: <LINKX_PUBLIC_API_KEY>
```

## Recommended Production Controls

- Put `/auth/parent-token` behind an IP allow-list or private network only.
- Keep `LINKX_PARENT_SHARED_SECRET` out of frontend/browser code.
- Use HTTPS/TLS at the gateway.
- Set `client_max_body_size` to match `LINKX_MAX_UPLOAD_BYTES`.
- Keep direct backend port access private when possible.
