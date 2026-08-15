# Admin Auto-Login & Authentication Bypass Handoff

**Date**: 2026-08-15  
**Scope**: LinkX API Backend (`/var/www/linkx-backend`), Split Service Factory (`service_factory/services/linkx-api`), and React Frontend (`/home/linkx/Linkx`)

---

## 1. Summary of Changes

To allow direct access to the main LinkX workspace without manual login prompts or token errors, the system was configured with an **automatic admin authentication fallback**:

1. **Unauthenticated HTTP Requests**: If an incoming request does not provide a Bearer token or provides an empty header, the backend automatically resolves the actor as the default `admin` account (`id: 1`, `username: "admin"`).
2. **Socket.IO Connections**: If a WebSocket handshake does not supply an auth token, the connection automatically binds to the `admin` actor.
3. **Auto-Login Route**: Added `GET/POST /auth/auto-login` (and `/auth/auto_login`) to issue an active admin JWT token and user profile on demand.
4. **Frontend Navigation**: The frontend navigation bar replaced the manual login prompt with the logged-in admin indicator, allowing direct entry into the analysis workspace.

---

## 2. Modified Files

| File Path | Description of Change |
| :--- | :--- |
| [`auth/decorators.py`](file:///var/www/linkx-backend/auth/decorators.py) | Added admin fallback in `current_actor_from_request()` when token is missing and `LINKX_AUTO_LOGIN_ADMIN` is enabled. |
| [`io_sockets.py`](file:///var/www/linkx-backend/io_sockets.py) | Added admin fallback in `handle_connect()` for Socket.IO clients connecting without tokens. |
| [`auth/routes.py`](file:///var/www/linkx-backend/auth/routes.py) | Added `/auth/auto-login` endpoint returning admin JWT tokens and actor metadata. |
| [`service_factory/services/linkx-api/src/auth/decorators.py`](file:///var/www/linkx-backend/service_factory/services/linkx-api/src/auth/decorators.py) | Synced decorator admin fallback to split API service factory. |
| [`service_factory/services/linkx-api/src/auth/routes.py`](file:///var/www/linkx-backend/service_factory/services/linkx-api/src/auth/routes.py) | Synced `/auth/auto-login` endpoint to split API service factory. |
| [`service_factory/auth/decorators.py`](file:///var/www/linkx-backend/service_factory/auth/decorators.py) | Synced decorator admin fallback to repo auth utilities. |
| [`service_factory/auth/routes.py`](file:///var/www/linkx-backend/service_factory/auth/routes.py) | Synced `/auth/auto-login` endpoint to repo auth routes. |
| [`/home/linkx/Linkx/src/App.jsx`](file:///home/linkx/Linkx/src/App.jsx) | Updated `NavBar` component to disable the login modal prompt and display admin status. |

---

## 3. How the Fallback Works

In [`auth/decorators.py`](file:///var/www/linkx-backend/auth/decorators.py), `current_actor_from_request()` checks for a valid Bearer token first. If none exists, it verifies the `LINKX_AUTO_LOGIN_ADMIN` environment setting (defaults to `true` if not set):

```python
if not actor:
    auto_admin = os.getenv("LINKX_AUTO_LOGIN_ADMIN", "true").lower() in ("1", "true", "yes")
    if auto_admin:
        actor = get_user_by_username("admin") or get_user_by_id(1)

if actor:
    g.current_actor = actor
    if actor.get("actor_type") == "user":
        g.current_user = actor
    return actor
```

---

## 4. How to Restore Back to Original State

You can restore strict authentication in two ways:

### Method A: Zero-Code / Environment Variable Toggle (Recommended)

You do not need to edit code to re-enable strict authentication. Simply disable the auto-login flag via systemd or environment:

1. Add `Environment="LINKX_AUTO_LOGIN_ADMIN=false"` into `/etc/systemd/system/linkx-backend.service.d/override.conf`:
   ```ini
   [Service]
   Environment="LINKX_AUTO_LOGIN_ADMIN=false"
   ```
2. Restart the backend service:
   ```bash
   pkill -f "python -u main.py"
   ```
   *(or `sudo systemctl daemon-reload && sudo systemctl restart linkx-backend.service`)*

When `LINKX_AUTO_LOGIN_ADMIN=false`, all requests without a valid Bearer JWT will immediately be rejected with `401 {"message": "unauthorized"}` as before.

---

### Method B: Complete Code Rollback via Git

To completely revert all code changes in the backend repository and frontend:

#### 1. Revert Backend Repository
```bash
cd /var/www/linkx-backend
git restore auth/decorators.py auth/routes.py io_sockets.py
git restore service_factory/services/linkx-api/src/auth/decorators.py
git restore service_factory/services/linkx-api/src/auth/routes.py
git restore service_factory/auth/decorators.py
git restore service_factory/auth/routes.py
```

#### 2. Revert Frontend Changes
In [`/home/linkx/Linkx/src/App.jsx`](file:///home/linkx/Linkx/src/App.jsx), change line 71 back to:
```jsx
function NavBar({ onNavAction }) {
  return (
    <nav id='nav_bar'>
      <span onClick={() => onNavAction('login')}>Login</span>
      <span onClick={() => onNavAction('about')}>About</span>
    </nav>
  );
}
```

#### 3. Restart Backend Service
```bash
pkill -f "python -u main.py"
```

---

## 5. Verification & Testing Commands

### Verify Auto-Login Mode (Current State)
```bash
# 1. Unauthenticated /init endpoint succeeds (200 OK)
curl -s -X POST http://127.0.0.1:8000/init -H "Content-Type: application/json" -d '{}' | jq '.message, .results'

# 2. Unauthenticated /auth/me returns admin actor
curl -s http://127.0.0.1:8000/auth/me | jq '.message, .actor.username'

# 3. Explicit auto-login token issuance
curl -s http://127.0.0.1:8000/auth/auto-login | jq '.message, .token'
```

### Verify Strict Mode (After Restoration)
```bash
# Unauthenticated requests will return 401 Unauthorized
curl -s -X POST http://127.0.0.1:8000/init -H "Content-Type: application/json" -d '{}'
# Output: {"message":"unauthorized"}
```
