# LinkX Security Hardening Handoff

Last updated: 2026-07-02

## Purpose

This handoff summarizes the current security posture of the LinkX backend split and groups the remaining hardening work by priority instead of by timeline. It is intended to help the next implementation session pick one priority group at a time and close it fully before moving on.

This document is based on repository evidence from:

- `docs/service_split_handoff_and_load_audit.md`
- `service_factory/services/linkx-api/**`
- `service_factory/services/linkx-worker/**`
- `service_factory/services/linkx-control-data/**`
- `service_factory/services/linkx-graph-maintenance/**`

## Scope

Active backend split:

- Server 1: `linkx-api`
- Server 2: `linkx-control-data` (`PostgreSQL`, `Redis`)
- Server 3: `linkx-worker`
- Server 4: `linkx-graph-maintenance` (`Neo4j`, cleanup services)

Frontend is separate and talks to Server 1 only.

## Verified Current Security Strengths

The following are already implemented in code or documented as current backend posture:

- Flask startup fails if `LINKX_CORS_ORIGINS` is not configured unless explicitly placed in insecure dev mode.
- Flask startup fails if `LINKX_FLASK_SECRET_KEY` is missing or left at the dev default unless insecure dev mode is explicitly enabled.
- Request size controls exist for uploads and JSON bodies.
- Basic security headers are applied at the API layer.
- Auth routes use rate limiting for login, service-token issuance, and parent-token/OAuth exchange.
- Parent/CTMS token verification includes explicit algorithm checking and issuer/audience validation hooks.
- Sensitive runtime config values are designed to be stored through encrypted secret references rather than raw session config JSON.
- Security audit event recording exists in the auth layer.
- Session/job orchestration has already been moved away from pure API process memory in the split architecture.

## Important Assessment Notes

- Findings below are based on repository code, deployment artifacts, and live P0 verification completed on 2026-07-02.
- Where the existing handoff document says UFW or ops restrictions are already applied, treat those as operational mitigations only if the running servers still match that state.
- A few issues are already partially mitigated operationally, but still remain fragile because the secure state is not fully encoded into deployment defaults.

## Priority Summary Table

| Priority Group | Security feature | Condition | Potential risks | Servers |
|---|---|---|---|---|
| P0 | Redis network isolation and authentication | Implemented | Residual risk is limited to credential handling, firewall drift, and future compose/env regression | Server 2 |
| P0 | Secret redaction in STR analysis and related logs | Implemented | Historical logs may retain old entries; residual risk is future direct debug prints or unreviewed adjacent paths | Server 1, Server 4 |
| P0 | AI partner least-privilege scoping | Implemented | Future co-analyst integration must keep the service secret server-side and preserve session-scoped access | Server 1 |
| P1 | Token revocation and post-logout invalidation | Implemented | Residual risk is limited to legacy no-jti tokens until expiry and future revocation-table cleanup automation | Server 1 |
| P1 | Internal transport protection for JWKS, partner, and east-west sensitive channels | Implemented | Residual risk is accepted plaintext private east-west transport where TLS is not yet available; monitor with validation script | Server 1, Server 2, Server 3, Server 4 |
| P1 | Deployable gateway configuration integrity | Implemented | Residual risk is live nginx drift or environment-specific allow-list mistakes during deployment | Server 1 |
| P2 | Service-account permission segmentation and audit depth | Partially implemented | Over-broad service capabilities, weak forensic visibility into partner reads, harder blast-radius control | Server 1 |
| P2 | Security configuration drift detection | Not implemented | UFW/systemd/compose drift can silently undo hardening without repo visibility | Server 1, Server 2, Server 3, Server 4 |
| P2 | Backup automation and recovery assurance for hardened state | Partially implemented | Recovery gaps for secrets, artifacts, graph state, or control-plane state during incident response | Server 2, Server 4 |
| P3 | Continuous security verification in CI/CD | Not implemented | Reintroduction of logging leaks, invalid configs, unsafe defaults, and auth regressions | Server 1, Server 2, Server 3, Server 4 |

## Priority Details

### P0

#### 1. Redis network isolation and authentication

Condition: Implemented

Why it was P0:

- Redis is a shared control-plane dependency for the split services.
- Without authentication and private binding, compromise or accidental exposure could allow queue tampering, job manipulation, and state poisoning.

Verified evidence:

- Repo now starts Redis with `--requirepass "$${REDIS_PASSWORD:?REDIS_PASSWORD is required}"`.
- Repo now binds Redis through `${REDIS_BIND_ADDR:-127.0.0.1}:${REDIS_PORT:-6379}:6379`.
- Repo healthcheck now authenticates with Redis before reporting healthy.
- Server 2 live verification showed unauthenticated `redis-cli ping` returns `NOAUTH Authentication required`.
- Server 2 live verification showed authenticated Redis ping returns `PONG`.
- Server 2 live `docker compose ps` showed Redis published on `172.27.23.106:6379`.
- Servers 1, 3, and 4 socket-level Redis checks returned `auth=+OK` and `authenticated_ping=+PONG`.

Completed hardening:

- Redis authentication is required in the deployed compose command.
- Redis is bound to the private control-data address.
- API, worker, and cleanup services use authenticated Redis URLs.
- Redis healthcheck validates the authenticated path.

Residual risk / follow-up:

- Keep the Redis password out of shell history, tickets, and chat logs.
- Re-check firewall and bind state after future control-data changes.
- Consider moving Redis to a private-only Docker network or managed private Redis later if the topology changes.

Primary server concerns:

- Server 2

#### 2. Secret redaction in STR analysis and related logs

Condition: Implemented

Why it was P0:

- Analyzer and cleanup logs can enter journald, support bundles, or log shipping.
- Any raw credential-shaped payload logging would undermine secret handling even when the runtime path itself is protected.

Verified evidence:

- Server 1 deployed `service_factory/services/linkx-api/src/api/STR_link_analysis.py` equivalent uses `redact_value(payload)` for the STR analyzer payload log.
- Server 1 `py_compile` passed for `api/STR_link_analysis.py`.
- Server 4 deployed cleanup task no longer logs `creds={...}` for Neo4j credential source.
- Server 4 live cleanup logs now show metadata only: `source=payload database=default password_ref=missing`.

Completed hardening:

- STR analyzer payload logging is redacted.
- Cleanup Neo4j credential-source logging is metadata-only.
- Changed files were compile-verified where applicable.

Residual risk / follow-up:

- Historical journald entries still contain the older credential-shaped cleanup log format, although the password value was masked as `***`.
- Continue P2/P3 work to add automated no-secret-log regression tests.
- Review adjacent worker and analysis paths before adding new debug logging.

Primary server concerns:

- Server 1
- Server 4

#### 3. AI partner least-privilege scoping

Condition: Implemented

Why it was P0:

- The AI service is correctly forced through Server 1 and no longer has direct DB/Neo4j access.
- The remaining risk was broad session/artifact visibility if the service token were misused or stolen.

Verified evidence:

- Server 1 `.env` has `LINKX_AI_ALLOW_GLOBAL_READ=false`.
- Server 1 `.env` has `LINKX_AI_ALLOWED_SESSION_IDS=` empty.
- Server 1 deployed `api/ai_service.py` includes `LINKX_AI_ALLOW_GLOBAL_READ`, `LINKX_AI_ALLOWED_SESSION_IDS`, and `ai_session_not_allowed`.
- Server 1 `py_compile` passed for `api/ai_service.py`.
- Server 1 `ai` service account exists, is active, and has only `ai:read,graph:read,reports:read,session:read`.
- Server 1 `/auth/service-token` returned a valid token for `client_id=ai`.
- Server 1 `/ai/sessions/not-a-real-session` returned `404 not_found`, confirming authenticated access works without broad object disclosure.

Completed hardening:

- AI read endpoints are gated by session ownership or explicitly allowed session IDs unless global read is intentionally enabled.
- Global AI read is disabled in production config.
- AI service-token issuance was verified with the scoped service account.

Frontend / AI alignment:

- No current frontend AI/co-analyst integration exists, so no immediate frontend change is required.
- Future co-analyst work must store the `ai` service secret only in server-side service config, never in browser-exposed frontend code or build artifacts.

Residual risk / follow-up:

- When the AI/co-analyst service is implemented, verify real owned/allowed sessions return data and unrelated sessions return `403 ai_session_not_allowed`.
- Consider P2 permission segmentation for artifact metadata, graph metadata, reports, and cleanup-run reads if the AI service needs narrower blast-radius controls.

Primary server concerns:

- Server 1

### P1

#### 4. Token revocation and post-logout invalidation

Condition: Implemented

Why it was P1:

- JWT issuance and validation were working, but access tokens previously remained valid until expiry after logout or idle timeout.
- This created a stolen-token risk window after user-initiated logout, idle expiry, or forced session termination.

Verified evidence:

- `service_factory/services/linkx-api/src/auth/tokens.py` now adds a `jti` to user and service tokens.
- `service_factory/services/linkx-api/src/auth/repository.py` now creates `token_revocations` and checks revoked `jti` values.
- `service_factory/services/linkx-api/src/auth/routes.py` now revokes the current bearer token on `/auth/logout` and `/auth/idle-timeout`.
- `verify_access_token()` now rejects revoked tokens, so protected HTTP endpoints and Socket.IO auth both honor revocation.
- Local `py_compile` passed for `auth/repository.py`, `auth/tokens.py`, `auth/routes.py`, `auth/decorators.py`, and `io_sockets.py`.

Completed hardening:

- New user and service tokens are uniquely identifiable by `jti`.
- Logout and idle-timeout insert the current token `jti` into a revocation table.
- Logout and idle-timeout responses now return `token_invalidated=true` when revocation succeeds.
- Revocation events are written to the security audit log as `auth.token_revoke`.

Residual risk / follow-up:

- Tokens issued before this change do not contain `jti`; they remain valid until normal expiry to avoid mass logout during rollout.
- `prune_expired_token_revocations()` exists but should be scheduled or called during maintenance to keep the revocation table small.
- Consider a future actor-wide token-version model if admins need instant invalidation of all tokens for a user or service account after credential rotation.

Primary server concerns:

- Server 1

#### 5. Internal transport protection for JWKS, partner, and east-west sensitive channels

Condition: Implemented

Why it was P1:

- Parent/JWKS and partner identity traffic can carry tokens or trust anchors and must not silently downgrade to plaintext HTTP.
- East-west database/cache/graph traffic may remain private-network plaintext where TLS is not yet operational, but that state must be explicit and auditable.

Verified evidence:

- `service_factory/services/linkx-api/src/auth/parent_jwt.py` rejects non-HTTPS Parent auth/JWKS URLs unless `LINKX_PARENT_AUTH_ALLOW_HTTP=true` is explicitly set.
- `service_factory/services/linkx-api/src/auth/parent_oauth.py` validates token, userinfo, and revoke URLs through the same Parent auth URL validator.
- `service_factory/services/linkx-api/src/auth/jwks_client.py` now validates JWKS URLs before fetching and sends an explicit JSON/User-Agent request.
- `service_factory/deploy/security/validate-transport-security.py` validates env files for unsafe Parent HTTP config and warns about plaintext Postgres, Redis, and remote Neo4j URLs.
- `service_factory/deploy/env/linkx-api.env.example` now documents HTTPS Parent/JWKS defaults and the explicit HTTP exception flag.

Completed hardening:

- Parent/JWKS external trust traffic is HTTPS-by-default in code and examples.
- HTTP Parent auth/JWKS is blocked unless the operator sets an explicit exception flag.
- A repeatable transport validation script is available for Server 1-4 env files.
- Plaintext east-west transport is no longer invisible; it is reported as warning by default and can be made failing with `--strict-east-west`.

Residual risk / follow-up:

- Current Redis/Postgres/Neo4j private-network transport may still be plaintext depending on deployed DSNs and service TLS support.
- Enabling TLS for Postgres, Redis, and Neo4j requires certificate provisioning and client trust-store configuration, so this remains an operational hardening follow-up if private VLAN risk is not accepted.
- Keep `LINKX_PARENT_AUTH_ALLOW_HTTP=true` only for temporary/private-network exceptions and pair it with `LINKX_PARENT_AUTH_ALLOWED_HOSTS`.

Primary server concerns:

- Server 1
- Server 2
- Server 3
- Server 4

#### 6. Deployable gateway configuration integrity

Condition: Implemented

Why it was P1:

- The repo contained one clean gateway template and one malformed service-level nginx config.
- The malformed file could break deployments, strip required auth headers, or expose routes incorrectly if copied during manual rollout.

Verified evidence:

- `service_factory/services/linkx-api/deploy/nginx/linkx-api-server.conf` has been replaced with the same valid gateway template used by `service_factory/deploy/nginx/linkx-api-gateway.conf`.
- The gateway template preserves `Authorization`, Socket.IO upgrade headers, `X-Forwarded-*`, optional `X-API-Key`, and parent-token secret forwarding.
- `service_factory/deploy/nginx/validate-linkx-api-gateway.sh` provides a repeatable `nginx -t` validation wrapper for the template.
- API gateway docs now include the validation command.

Completed hardening:

- Removed the deployable malformed nginx artifact from the API service tree.
- Preserved one canonical gateway shape across repo deployment paths.
- Added a validation helper to catch future syntax regressions before copying nginx config live.

Residual risk / follow-up:

- The live server may still use an older `/etc/nginx/sites-*` file; verify before replacing it.
- Production `/auth/parent-token` allow-list values are environment-specific and must be set on Server 1 if that route is exposed through nginx.
- HTTPS/TLS remains covered by the separate P1 internal/external transport item.

Primary server concerns:

- Server 1

### P2

#### 7. Service-account permission segmentation and audit depth

Condition: Partially implemented

Why it is P2:

- RBAC exists, which is a strong starting point.
- The remaining work is about refinement: narrower service roles, stronger audit semantics, and better blast-radius control.

What to fix in this priority:

- Separate AI, reporting, and parent gateway permissions more sharply.
- Add per-endpoint or per-object audit events for partner/service reads.
- Review whether graph metadata, reports, session reads, and artifact reads should remain grouped.

Primary server concerns:

- Server 1

#### 8. Security configuration drift detection

Condition: Not implemented

Why it is P2:

- Current hardening depends heavily on systemd, UFW, manual file copies, and operator discipline.
- That means a secure state can silently drift away from what the repo and handoff describe.

What to fix in this priority:

- Add operational verification scripts for open ports, expected systemd units, env sanity, and deployment paths.
- Add a repeatable checklist or machine-readable validation for each of the four servers.
- Track deployed config hashes or versions where practical.

Primary server concerns:

- Server 1
- Server 2
- Server 3
- Server 4

#### 9. Backup automation and recovery assurance for hardened state

Condition: Partially implemented

Why it is P2:

- The prior handoff already says recovery evidence is mostly proven, but some scheduled/off-host pieces are still not fully live.
- This matters for security because incident recovery depends on it.

Verified evidence:

- `docs/service_split_handoff_and_load_audit.md` says backup timers and off-host retention helpers are present but not yet fully deployed

What to fix in this priority:

- Deploy and verify the scheduled backup timer units.
- Configure encrypted off-host backup targets.
- Re-run restore drills after representative data exists in Neo4j and artifacts.
- Confirm secret recovery material handling remains separate and documented.

Primary server concerns:

- Server 2
- Server 4

### P3

#### 10. Continuous security verification in CI/CD

Condition: Not implemented

Why it is P3:

- The codebase has enough custom auth, queue, and analysis behavior that regression prevention is now important.
- This is best addressed once the P0/P1 controls are fixed.

What to fix in this priority:

- Add tests for secret redaction and no-plaintext-log assertions.
- Add config validation for nginx and env sanity.
- Add regression tests for auth revocation, AI scoping, and parent-token validation behavior.
- Add dependency and secret scanning if not already present elsewhere.

Primary server concerns:

- Server 1
- Server 2
- Server 3
- Server 4

## Recommended Working Order

Use this order for implementation sessions:

1. Start P1 now that P0 is closed.
2. Re-verify the repo and live deployment state after each P1 change.
3. Keep P0 checks as smoke tests after future deploys.
4. Treat P2 as hardening depth and resilience work.
5. Treat P3 as continuous assurance once the higher-priority controls are in place.

## Session Handoff Guidance

For the next engineering session:

- Start by choosing one row from the priority summary table.
- Confirm whether the live servers still match the assumptions in `docs/service_split_handoff_and_load_audit.md`.
- Make the secure state the default in code/config, not just an operational note.
- After each completed priority item, update this file by changing its `Condition`, evidence notes, and residual risk.
