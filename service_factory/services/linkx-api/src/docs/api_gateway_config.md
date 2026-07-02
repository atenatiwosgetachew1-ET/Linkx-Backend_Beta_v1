# Linkx API Gateway Configuration

This document describes the route/auth behavior expected at the API gateway layer.

## Gateway Responsibility

The Linkx backend remains the source of truth for JWT validation, RBAC, service-account checks, and session ownership.

The gateway should:

- Route Linkx paths to the backend service.
- Preserve `Authorization: Bearer <JWT>`.
- Preserve `Authorization: Bearer <JWT>` for protected LinkX calls.
- Route `/auth/exchange` and `/api/auth/exchange` to the API service for Parent project OAuth code exchange.
- Preserve `X-API-Key` for `/api/STR_link_analysis` if `LINKX_PUBLIC_API_KEY` is configured.
- Support WebSocket/Socket.IO upgrade headers for `/socket.io/`.
- Restrict legacy `/auth/parent-token` direct-token usage to trusted networks when it remains enabled for rollback.

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

Validate the template before copying it into an active nginx site:

```bash
service_factory/deploy/nginx/validate-linkx-api-gateway.sh \
  service_factory/deploy/nginx/linkx-api-gateway.conf
```

## Route Matrix

| Route | Gateway Auth Behavior | Backend Auth Behavior |
|---|---|---|
| `GET /db/health` | Public/internal health route | Checks PostgreSQL health |
| `POST /auth/login` | Public route | Validates username/password |
| `POST /auth/service-token` | Internal/sibling-service route | Validates client id/secret |
| `POST /auth/exchange` | Public from approved LinkX frontend origin | Exchanges Parent project authorization code server-side and maps userinfo |
| `POST /api/auth/exchange` | Public from approved LinkX frontend origin | Compatibility alias for `/auth/exchange` |
| `POST /auth/parent-token` | Trusted rollback/direct-token route | Validates Parent project ES256 access JWT; legacy shared-secret mode disabled by default |
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

Parent project OAuth exchange:

```http
Content-Type: application/json
Origin: <approved LinkX frontend origin>
```

Legacy parent-token shared secret is disabled unless explicitly approved.

Public STR API if API key is enabled:

```http
X-API-Key: <LINKX_PUBLIC_API_KEY>
```

## Recommended Production Controls

- Allow `/auth/exchange` and `/api/auth/exchange` only from approved LinkX frontend origins.
- Keep Parent project OAuth client secrets out of frontend/browser code.
- Keep legacy `/auth/parent-token` behind an IP allow-list if it is temporarily retained.
- Use HTTPS/TLS at the gateway.
- Set `client_max_body_size` to match `LINKX_MAX_UPLOAD_BYTES`.
- Keep direct backend port access private when possible.
