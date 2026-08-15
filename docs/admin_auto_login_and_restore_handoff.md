# Admin Auto-Login, Gateway Routing & Restoration Handoff

**Date**: 2026-08-15  
**Scope**: LinkX API Backend (`/var/www/linkx-backend`), Split API Service (`service_factory/services/linkx-api`), Nginx Gateway Templates, Production Server 1 (`node-19` - `172.27.23.95`), and React Frontend (`/home/linkx/Linkx`).

---

## 1. Summary of Changes

To allow direct access to the main LinkX workspace without manual login prompts or token errors, the system was configured with an **automatic admin authentication fallback** and **gateway route exposure**:

1. **Unauthenticated HTTP Requests**: If an incoming request does not provide a Bearer token or provides an empty header, the backend automatically resolves the actor as the default `admin` account (`id: 1`, `username: "admin"`).
2. **Socket.IO Connections**: If a WebSocket handshake does not supply an auth token, the connection automatically binds to the `admin` actor.
3. **Auto-Login Route**: Added `GET/POST /auth/auto-login` (and `/auth/auto_login`) to issue an active admin JWT token and user profile on demand.
4. **Nginx API Gateway Routing**: Added public location block `location = /auth/auto-login` and `location = /auth/auto_login` to Nginx gateway configs to ensure reverse proxies route the endpoint to backend without requiring prior auth headers.
5. **Frontend Navigation**: The frontend navigation bar replaced the manual login prompt with the logged-in admin indicator, allowing direct entry into the analysis workspace.

---

## 2. Modified Files

| File Path | Description of Change |
| :--- | :--- |
| [`auth/decorators.py`](file:///var/www/linkx-backend/auth/decorators.py) | Added admin fallback in `current_actor_from_request()` when token is missing and `LINKX_AUTO_LOGIN_ADMIN` is enabled. |
| [`io_sockets.py`](file:///var/www/linkx-backend/io_sockets.py) | Added admin fallback in `handle_connect()` for Socket.IO clients connecting without tokens. |
| [`auth/routes.py`](file:///var/www/linkx-backend/auth/routes.py) | Added `/auth/auto-login` endpoint returning admin JWT tokens and actor metadata. |
| [`deploy/nginx/linkx-api-gateway.conf`](file:///var/www/linkx-backend/deploy/nginx/linkx-api-gateway.conf) | Added public proxy location block for `/auth/auto-login`. |
| [`service_factory/deploy/nginx/linkx-api-gateway.conf`](file:///var/www/linkx-backend/service_factory/deploy/nginx/linkx-api-gateway.conf) | Added public proxy location block for `/auth/auto-login`. |
| [`service_factory/services/linkx-api/deploy/nginx/linkx-api-server.conf`](file:///var/www/linkx-backend/service_factory/services/linkx-api/deploy/nginx/linkx-api-server.conf) | Added public proxy location block for `/auth/auto-login`. |
| [`service_factory/services/linkx-api/src/deploy/nginx/linkx-api-gateway.conf`](file:///var/www/linkx-backend/service_factory/services/linkx-api/src/deploy/nginx/linkx-api-gateway.conf) | Added public proxy location block for `/auth/auto-login`. |
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

## 4. Production Deployment to Server 1 (`node-19` - `172.27.23.95`)

To apply these changes on the live Server 1 backend and Nginx gateway, run on Server 1:

```bash
# 1. Pull latest code from GitHub repository
sudo git -C /opt/linkx-backend-update pull

# 2. Copy API source to production path
sudo cp -r /opt/linkx-backend-update/service_factory/services/linkx-api/src/. /opt/linkx-backend-api/src/

# 3. Update Nginx configuration & reload
sudo cp /opt/linkx-backend-update/service_factory/deploy/nginx/linkx-api-gateway.conf /etc/nginx/sites-available/linkx-api-gateway.conf 2>/dev/null || true
sudo cp /opt/linkx-backend-update/service_factory/services/linkx-api/deploy/nginx/linkx-api-server.conf /etc/nginx/sites-available/linkx-api-server.conf 2>/dev/null || true
sudo nginx -t && sudo systemctl reload nginx

# 4. Restart backend service & wait for pool startup
sudo systemctl restart linkx-api
sleep 4
```

---

## 5. How to Restore Back to Original Strict State

You can restore strict authentication in two ways:

### Method A: Zero-Code / Environment Toggle (Recommended)

You do not need to rewrite code to re-enable strict authentication. Simply set the environment flag `LINKX_AUTO_LOGIN_ADMIN=false`:

#### On Local/Monolith Backend:
1. Add `Environment="LINKX_AUTO_LOGIN_ADMIN=false"` into `/etc/systemd/system/linkx-backend.service.d/override.conf`:
   ```ini
   [Service]
   Environment="LINKX_AUTO_LOGIN_ADMIN=false"
   ```
2. Restart the service:
   ```bash
   pkill -f "python -u main.py"
   ```

#### On Production Server 1 (`172.27.23.95`):
1. Add `Environment="LINKX_AUTO_LOGIN_ADMIN=false"` to `/etc/systemd/system/linkx-api.service.d/override.conf`:
   ```ini
   [Service]
   Environment="LINKX_AUTO_LOGIN_ADMIN=false"
   ```
2. Reload systemd and restart service:
   ```bash
   sudo systemctl daemon-reload && sudo systemctl restart linkx-api
   ```

When `LINKX_AUTO_LOGIN_ADMIN=false`, all requests without a valid Bearer JWT will immediately be rejected with `401 {"message": "unauthorized"}` as before.

---

### Method B: Complete Code Rollback & Deployment

To completely revert all code changes across backend, Nginx, and frontend:

#### 1. Revert Backend Git Repository
```bash
cd /var/www/linkx-backend

# Revert auth decorators, routes, and socket handlers
git restore auth/decorators.py auth/routes.py io_sockets.py
git restore service_factory/auth/decorators.py service_factory/auth/routes.py
git restore service_factory/services/linkx-api/src/auth/decorators.py
git restore service_factory/services/linkx-api/src/auth/routes.py

# Revert Nginx gateway templates
git restore deploy/nginx/linkx-api-gateway.conf
git restore service_factory/deploy/nginx/linkx-api-gateway.conf
git restore service_factory/services/linkx-api/deploy/nginx/linkx-api-server.conf
git restore service_factory/services/linkx-api/src/deploy/nginx/linkx-api-gateway.conf

# Commit and push reversal
git commit -am "revert: restore strict authentication and remove auto-login bypass"
git push origin main
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

#### 3. Deploy Reversal to Server 1 (`172.27.23.95`)
```bash
sudo git -C /opt/linkx-backend-update pull
sudo cp -r /opt/linkx-backend-update/service_factory/services/linkx-api/src/. /opt/linkx-backend-api/src/
sudo cp /opt/linkx-backend-update/service_factory/deploy/nginx/linkx-api-gateway.conf /etc/nginx/sites-available/linkx-api-gateway.conf 2>/dev/null || true
sudo cp /opt/linkx-backend-update/service_factory/services/linkx-api/deploy/nginx/linkx-api-server.conf /etc/nginx/sites-available/linkx-api-server.conf 2>/dev/null || true
sudo nginx -t && sudo systemctl reload nginx
sudo systemctl restart linkx-api
sleep 4
```

---

## 6. Verification & Testing Commands

### Verify Auto-Login Mode (Current State)
```bash
# 1. Unauthenticated /init endpoint succeeds (200 OK)
curl -s -X POST http://127.0.0.1:8000/init -H "Content-Type: application/json" -d '{}' | jq '.message, .results'

# 2. Unauthenticated /auth/me returns admin actor
curl -s http://127.0.0.1:8000/auth/me | jq '.message, .actor.username'

# 3. Auto-login token issuance via gateway proxy
curl -k -s https://172.27.23.21/auth/auto-login | jq '.message, .token'
```

### Verify Strict Mode (After Restoration)
```bash
# Unauthenticated requests will return 401 Unauthorized
curl -s -X POST http://127.0.0.1:8000/init -H "Content-Type: application/json" -d '{}'
# Output: {"message":"unauthorized"}
```
