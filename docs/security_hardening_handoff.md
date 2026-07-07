# LinkX Security Hardening Handoff

Last updated: 2026-07-06

## Purpose

This handoff summarizes the current security posture of the LinkX backend split and records the hardening work completed across P0-P3. It is intended to help future implementation sessions understand what changed, what was verified on the live servers, and what residual risks remain.

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

## Current Completion Snapshot

As of 2026-07-03, the priority-driven hardening track is functionally complete for P0, P1, P2, and P3. The project now has authenticated Redis, scoped AI/service access, token revocation, sensitive-log redaction, deployable gateway validation, off-host backup automation, restore/secret-recovery evidence, drift checks, CI security regression tests, dependency audit, and committed-secret scanning.

Security maturity rating: 7.5/10

Rating rationale:

- The project is now production-defensible and no longer in a soft-target posture.
- Baseline application, service, backup, and CI controls are implemented and verified.
- Remaining maturity gaps are mostly operational and infrastructure-level: TLS/mTLS for east-west traffic, stronger centralized or encrypted secret management, representative Neo4j/artifact restore drills, fuller monitoring/alerting, and more policy-managed infrastructure.

Target maturity path:

- 8.5/10: enforce TLS for Postgres/Redis/Neo4j/private API traffic, complete representative restore drills, encrypt off-host backups, and add alerting for failed security checks/backups.
- 9/10: add mTLS or service identity between backend services, move high-risk secrets into systemd credentials/SOPS/Vault/KMS, automate secret rotation evidence, and manage firewall/deploy policy through auditable IaC.

## Verified Current Security Strengths

The following are already implemented in code or documented as current backend posture:

- Flask startup fails if `LINKX_CORS_ORIGINS` is not configured unless explicitly placed in insecure dev mode.
- Flask startup fails if `LINKX_FLASK_SECRET_KEY` is missing or left at the dev default unless insecure dev mode is explicitly enabled.
- Request size controls exist for uploads and JSON bodies.
- Basic security headers are applied at the API layer.
- Auth routes use rate limiting for login, service-token issuance, and parent-token/OAuth exchange.
- Parent project token verification and OAuth exchange include explicit algorithm checking, issuer/audience validation hooks, rate limiting, and server-side token handling.
- Sensitive runtime config values are designed to be stored through encrypted secret references rather than raw session config JSON.
- Security audit event recording exists in the auth layer.
- Session/job orchestration has already been moved away from pure API process memory in the split architecture.

## Important Assessment Notes

- Findings below are based on repository code, deployment artifacts, live server verification completed across 2026-07-02 and 2026-07-03, and CI security checks that are now green.
- Server 1 HTTPS Parent project SSO env alignment was also re-verified on 2026-07-03 after restart: API health, auth 401 behavior, nginx gateway checks, and drift checks all passed.
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
| P2 | Service-account permission segmentation and audit depth | Implemented | Residual risk is future partner/service endpoints bypassing granular permissions or audit conventions | Server 1 |
| P2 | Security configuration drift detection | Implemented | Residual risk is limited to checks not yet encoded in the drift script, such as full firewall policy and off-host monitoring state | Server 1, Server 2, Server 3, Server 4 |
| P2 | Backup automation and recovery assurance for hardened state | Partially implemented | Local and off-host backup automation is live; PostgreSQL restore and managed-secret decrypt proof are verified; representative Neo4j/artifact restore drills remain future evidence items | Server 1, Server 2, Server 4 |
| P3 | Continuous security verification in CI/CD | Implemented | Residual risk is gaps outside the current CI checks, such as full integration tests, live restore drills, and infrastructure policy checks | Server 1, Server 2, Server 3, Server 4 |

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
- Production `/auth/parent-token` allow-list values are environment-specific and must be set on Server 1 if that rollback/direct-token route is exposed through nginx.
- HTTPS/TLS remains covered by the separate P1 internal/external transport item.

Primary server concerns:

- Server 1

### P2

#### 7. Service-account permission segmentation and audit depth

Condition: Implemented

Why it was P2:

- Service-account RBAC already existed, but AI partner reads were grouped under one broad `ai:read` permission.
- Broader permissions make blast-radius control harder if a service token is misused or a future partner integration needs only one read surface.

Verified evidence:

- `service_factory/services/linkx-api/src/auth/repository.py` now seeds granular AI permissions: `ai:session:read`, `ai:artifact:read`, `ai:cleanup:read`, and `ai:graph:metadata:read`.
- `service_factory/services/linkx-api/src/api/ai_service.py` now requires the granular permission matching each AI endpoint group.
- AI object reads already record success/failure for session access decisions; artifact and graph metadata result reads now also record count/size-oriented audit metadata.

Completed hardening:

- `ai:read` remains only the base AI service permission used for `/ai/health`.
- Session listing/detail requires `ai:session:read`.
- Artifact reads require `ai:artifact:read`.
- Cleanup-run reads require `ai:cleanup:read`.
- Graph metadata reads require `ai:graph:metadata:read`.
- Additional audit events capture artifact read result counts and graph metadata result sizes.

Residual risk / follow-up:

- Existing deployed AI service account has been updated with the new granular permissions; future service accounts must follow the same pattern.
- Future partner/service endpoints should follow the same pattern: specific permission plus object-level audit event.
- If a future AI co-analyst needs narrower scope, remove unused granular permissions instead of reusing broad `ai:read`.

Primary server concerns:

- Server 1

#### 8. Security configuration drift detection

Condition: Implemented

Why it is P2:

- Current hardening depends heavily on systemd, UFW, manual file copies, and operator discipline.
- That means a secure state can silently drift away from what the repo and handoff describe.

Verified evidence:

- `service_factory/deploy/security/verify-linkx-server.py` provides role-specific drift checks for `api`, `control-data`, `worker`, and `graph-maintenance`.
- The script checks expected deploy paths, systemd units, Redis authentication behavior, API security code markers, nginx gateway installation/health, and cleanup credential-log redaction markers.
- The script exits non-zero on failing checks and prints a PASS/WARN/FAIL summary suitable for manual handoff or later CI/ops automation.
- Server 1 (`node-19`) live `--role api` verification completed with `summary: failures=0 warnings=0`.
- Server 2 (`node-20`) live `--role control-data` verification completed with `summary: failures=0 warnings=0`.
- Server 3 (`node-21`) live `--role worker` verification completed with `summary: failures=0 warnings=0`.
- Server 4 (`node-22`) live `--role graph-maintenance` verification completed with `summary: failures=0 warnings=0`.

Completed hardening:

- Added a repeatable machine-readable validation command for each of the four servers.
- Encoded the already-hardened Redis auth state as a live socket test instead of only checking config text.
- Encoded API logout revocation, AI granular permissions, and STR redaction as deployed source-code marker checks.
- Encoded graph-maintenance credential-log redaction as a deployed source-code marker check.
- Verified that all four deployed server roles currently match the encoded drift checks with no warnings.

Residual risk / follow-up:

- Firewall/UFW policy and cloud/security-group rules should still be checked with server-specific commands because this repository does not own every network control.
- The drift script should be expanded as future P2/P3 controls are completed.
- Consider storing signed config hashes or a deployment manifest if manual server copies remain the primary deployment method.

Primary server concerns:

- Server 1
- Server 2
- Server 3
- Server 4

#### 9. Backup automation and recovery assurance for hardened state

Condition: Partially implemented

Why it is P2:

- The prior handoff already says recovery evidence is mostly proven, but future representative-data restore evidence and secret recovery validation are not fully live.
- This matters for security because incident recovery depends on it.

Verified evidence:

- `docs/service_split_handoff_and_load_audit.md` says backup timers and off-host retention helpers are present but not yet fully deployed.
- `service_factory/deploy/security/verify-linkx-backups.py` now verifies backup timer installation, enablement, recent local backup files, checksum evidence, script syntax, and off-host target configuration warnings.
- Backup shell scripts pass local `bash -n` syntax checks.
- Server 2 (`node-20`) PostgreSQL backup timer is installed, enabled, active, listed in timers, and verified with a recent dump plus checksum; verifier returned `summary: failures=0 warnings=2`.
- Server 1 (`node-19`) artifact backup timer is installed, enabled, active, listed in timers, and verified with a recent tar snapshot plus checksum; verifier returned `summary: failures=0 warnings=2`.
- Server 4 (`node-22`) Neo4j offline backup timer is installed, enabled, active, listed in timers, and verified with a recent dump plus checksum; verifier returned `summary: failures=0 warnings=2`.
- Server 1 (`node-19`) artifact off-host sync to `backup-user@172.20.107.94:/srv/linkx-backups/node-19-artifacts` is configured and verified; systemd backup run completed successfully and the backup server contains `.tar.gz` plus `.sha256` files.
- Server 2 (`node-20`) PostgreSQL off-host sync to `backup-user@172.20.107.94:/srv/linkx-backups/node-20-postgres` is configured and verified; verifier returned `summary: failures=0 warnings=1` and the backup server contains `.dump` plus `.sha256` files.
- Server 4 (`node-22`) Neo4j off-host sync to `backup-user@172.20.107.94:/srv/linkx-backups/node-22-neo4j` is configured and verified; verifier returned `summary: failures=0 warnings=1` and the backup server contains `neo4j.dump` plus checksum files.
- Server 2 PostgreSQL restore drill succeeded from the off-host backup path into isolated database `linkx_restore_test`; checksum verification passed and restored table counts included `users=1`, `jobs=912`, `session_configs=100`, and `managed_secrets=255`.
- Server 1 managed-secret recovery proof succeeded without printing the secret value: a sample `tool_credentials.password` secret was present, decrypt returned `True`, and plaintext length was verified as 44.
- Server 4 Neo4j live graph count was `0`, so the current restore proof is structurally useful but not representative of a populated graph.

Completed hardening:

- Added a repeatable backup automation verifier for PostgreSQL, shared artifacts, and Neo4j backup families.
- Deployed and verified scheduled local backup timers for PostgreSQL, shared artifacts, and Neo4j.
- Configured and verified off-host backup sync for artifacts, PostgreSQL, and Neo4j to the dedicated backup server.
- Captured PostgreSQL restore-drill evidence from off-host backup into isolated restore database `linkx_restore_test`.
- Captured managed-secret decrypt proof without exposing plaintext.
- Kept off-host backup target detection as a warning so local scheduled backups can be enabled first without hiding resilience gaps.

What remains open:

- Re-run artifact restore into an isolated `/tmp/linkx-restore-tests` directory and record file/directory counts if that evidence was not captured in the same session.
- Re-run Neo4j restore after representative non-empty graph data exists.
- Confirm secret recovery material handling remains separate and documented after any secret rotation.
- Consider encrypting off-host backup archives before or during sync if the backup server becomes multi-user or less trusted.

Primary server concerns:

- Server 1
- Server 2
- Server 4

### P3

#### 10. Continuous security verification in CI/CD

Condition: Implemented

Why it is P3:

- The codebase has enough custom auth, queue, and analysis behavior that regression prevention is now important.
- This is best addressed once the P0/P1 controls are fixed.

Verified evidence:

- `.github/workflows/security-checks.yml` now runs security regression checks, dependency auditing, and secret scanning on `main` pushes and pull requests.
- `tests/security/test_security_regressions.py` covers recursive secret redaction, STR analyzer redacted logging, cleanup Neo4j metadata-only logging, token revocation markers, granular AI permissions, off-host backup SSH-key handling, and removal of the vulnerable unused `nltk`/`textblob` dependency path.
- CI compiles deploy security helpers, validates backup/deploy shell script syntax, validates the nginx gateway template, runs the transport-security validator against a strict secure sample env, audits Python requirement files with `pip-audit`, and scans for committed secrets with Gitleaks.
- GitHub security checks are green for `security-regression`, `dependency-audit`, and `secret-scan` after the nginx validator was made CI-safe and unused vulnerable dependencies were removed.
- Local verification passed for `python3 -m unittest discover -s tests/security -p 'test_*.py'`.
- Local verification passed for deploy security helper `py_compile`, backup/deploy `bash -n`, gateway validation, and strict transport-validator smoke testing.

Completed hardening:

- Added a lightweight CI security gate that does not require production services or secrets.
- Added regression tests for the most important P0/P1/P2 fixes so future edits fail fast if they remove redaction, revocation, AI permission segmentation, or backup SSH-key safety.
- Added repeatable validation for gateway and transport-security config artifacts.
- Added dependency vulnerability scanning for all Python requirements files and committed-secret scanning for the repository history.
- Removed unused `TextBlob` imports and unpinned the unused `nltk`/`textblob` dependency chain because `nltk==3.9.4` had `PYSEC-2026-597` with no fixed version. Current rule behavior is unchanged because sentiment logic already consumes stored `POLARITY`/`SENTIMENT` graph fields.

Residual risk / follow-up:

- Add integration tests for real token revocation behavior against a test database.
- Add restore-drill automation or evidence capture after representative graph/artifact data exists.

Primary server concerns:

- Server 1
- Server 2
- Server 3
- Server 4

## Server 1 Stress Test Handoff

This section records the current load-testing snapshot for Server 1 so future sessions can separate real capacity issues from auth or throttle noise.

### Server 1 Specification

- Host: `node-19`
- Role: `linkx-api` Flask API / RBAC / Socket service
- Address: `172.27.23.95`
- CPU: `4 vCPU`
- RAM: `8 GB`
- Storage: `290 GB`

### Stress Test Setup

- Runner: external desktop machine on the same network
- Tool: `k6`
- Target: `http://172.27.23.95:8000`
- Paths exercised:
  - `GET /db/health`
  - `POST /auth/login`
  - `POST /init`
  - `GET /auth/me`
  - `POST /auth/verify`
  - `GET /workspace/layout`
  - `POST /workspace/layout` when write mode is enabled
  - `GET /auth/preferences`
  - `PATCH /auth/preferences` when write mode is enabled
  - `POST /graph_link`
  - `POST /get_graph`
  - `POST /api/STR_link_analysis`
- Test focus: validate authenticated API behavior under concurrent traffic without letting login throttling dominate every result

### Latest Clean Result

The last clean run used valid login values and showed:

- `vus_max`: `20`
- `http_req_failed`: `0.00%`
- `http_req_duration p(95)`: `3.68s`
- `http_req_duration avg`: `2.05s`
- `iteration_duration p(95)`: `7.08s`
- `checks_succeeded`: `100.00%`
- `checks_failed`: `0.00%`
- `health`, `login`, and `init` all returned successful responses

### Control Plane Results

The session/control-plane script reused one login and one session id per run. The stable operating point is around 15 VUs. Results recorded:

| VUs | p95 latency | Failure rate | Notes |
|---|---:|---:|---|
| 5 | 452.43 ms | 0.00% | Healthy baseline |
| 10 | 931.03 ms | 0.00% | Still within threshold |
| 15 | 1.43 s | 0.00% | Passes the current `1.5s` threshold |
| 16 | 1.58 s | 0.00% | Threshold crossed |
| 17 | 1.68 s | 0.00% | Threshold crossed |
| 20 | 1.98 s | 0.00% | Clear overload signal for latency |

### Graph Route Results

The graph route script reused one login and one session id per run. Results recorded:

| VUs | p95 latency | Failure rate | Notes |
|---|---:|---:|---|
| 5 | 650.11 ms | 0.00% | Healthy baseline |
| 10 | 1.22 s | 0.00% | Still within threshold |
| 11 | 1.43 s | 0.00% | Passes the current `1.5s` threshold |
| 12 | 1.57 s | 0.00% | Threshold crossed |
| 13 | 1.64 s | 0.00% | Threshold crossed |
| 15 | 1.97 s | 0.00% | Threshold crossed |
| 20 | 2.52 s | 0.00% | Clear overload signal for latency |

### STR Search / Dataframe Results

The STR script also reused one login and one session id per run, but the current Server 1 deployment does not have the STR linking logic available in a way that allows the full search/dataframe/Neo4j pipeline to complete. Treat the STR failures below as an expected environment limitation, not as evidence that the whole API is broken.

| VUs | p95 latency | Failure rate | Notes |
|---|---:|---:|---|
| 5 | 285.19 ms | 99.70% | Route returns non-success because STR linking logic is not available right now |
| 10 | 669.98 ms | 99.72% | Same limitation |
| 15 | 1.17 s | 99.72% | Same limitation |
| 18 | 1.40 s | 99.72% | Same limitation |
| 19 | 1.54 s | 99.72% | Same limitation and threshold crossed |
| 20 | 1.61 s | 99.72% | Same limitation and threshold crossed |

## Server 3 Stress Results

Server 3 was validated through the worker-backed graph path while Server 1 remained the frontend/API entrypoint. The stress script now logs in, creates a fresh session, connects Neo4j into that same session with `/connect_to_tool`, then exercises `/graph_link` and `/get_graph`. That makes this section a direct check of the live Server 3 graph-worker path rather than a pure API-only graph fetch.

### Server 3 Specification

- Host: `node-21`
- Role: `linkx-worker`
- Address: `172.27.23.18`
- Runtime: Python venv + systemd
- Queues: `ingestion`, `dataframe`, `analysis`, `graph`

### Worker Surface And Paths

Server 3 is not a browser-facing API host, so it does not have a public route list like Server 1. Its practical stress surface in this pass was:

- Server 1 `POST /auth/login`
- Server 1 `POST /init`
- Server 1 `POST /connect_to_tool`
- Server 1 `POST /graph_link`
- Server 1 `POST /get_graph`
- Server 3 worker queue consumption for graph jobs
- Server 3 to Server 2 control-data access on `5432` and `6379`
- Server 3 to Server 4 Neo4j Bolt access on `7687`

### Stress Setup

- Runner: external desktop machine on the same network
- Tool: `k6`
- Entry target: `http://172.27.23.95:8000`
- Worker setting used during the final ladder: `WORKER_CONCURRENCY=5`
- Validation signals: k6 output plus Server 3 worker logs

### Worker-Backed Graph Results

Server 3 runtime notes during this pass:

- `WORKER_CONCURRENCY` was explicitly set to `5` before the ladder run.
- The worker host did not have `prometheus-node-exporter` installed yet, so this pass used k6 results plus worker logs rather than host-exporter graphs.
- The graph test seeded Neo4j credentials into each fresh session before graph fetches so the worker-backed path matched the intended runtime flow.

Results recorded:

| VUs | p95 latency | Failure rate | Notes |
|---|---:|---:|---|
| 5 | 585.43 ms | 0.00% | Healthy baseline with fresh session + Neo4j connect step |
| 10 | 1.18 s | 0.00% | Still within threshold |
| 12 | 1.36 s | 0.00% | Highest passing point under the current `1.5s` threshold |
| 13 | 1.52 s | 0.00% | First threshold crossing |
| 15 | 2.03 s | 0.00% | Clear overload signal for latency |

### Readout

- Server 3 is functioning correctly for the worker-backed graph path once the session has valid Neo4j credentials.
- The practical operating ceiling for this path is about `12 VUs` under the current `p95 < 1.5s` rule.
- Latency, not outright request failure, is the first limiting factor on this worker-backed path.
- The measured limit in this pass was driven by latency growth, not request failure.

## Server 2 Stress Results

Server 2 was benchmarked as the control-data host for PostgreSQL and Redis. The PostgreSQL and Redis runs were launched in separate terminals during the same test window, so this section records them as a simultaneous Server 2 stress sample while still keeping the PostgreSQL and Redis figures separate.

### Server 2 Specification

- Host: `node-20`
- Role: `linkx-control-data`
- Address: `172.27.23.106`
- Runtime: Docker Compose + systemd wrapper
- Services under test:
  - PostgreSQL on `5432`
  - Redis on `6379`

### Service Surface Under Test

Server 2 is also not a browser-facing API host, so there is no Flask route inventory here. The meaningful stress interfaces are:

- PostgreSQL wire protocol on `172.27.23.106:5432`
- Redis authenticated access on `172.27.23.106:6379`
- Upstream callers expected by design:
  - Server 1 API
  - Server 3 worker
  - Server 4 cleanup services

### Stress Setup

- Runner: external Linux machine on the same private network
- Tools:
  - `pgbench` for PostgreSQL
  - `redis-benchmark` for Redis
- PostgreSQL test style: built-in TPC-B style benchmark
- Redis test style: command mix benchmark covering ping, get/set, list, set, hash, sorted-set, and range reads
- Security note: Redis authentication remained enabled during testing and unauthenticated access continued to return `NOAUTH`

### Round 1

- PostgreSQL pgbench:
  - 10 clients, 2 threads
  - 5249 transactions processed
  - 0 failed transactions
  - average latency: 114.880 ms
  - throughput: 87.047670 tps
- Redis redis-benchmark:
  - simple commands stayed in the healthy multi-thousand to tens-of-thousands req/s range
  - representative results: PING_INLINE 33921.30 rps, PING_MBULK 41000.41 rps, GET 39952.06 rps, MSET 24177.95 rps
  - larger list reads slowed as expected: LRANGE_600 2115.78 rps, p50=7.799 ms

### Round 2

- PostgreSQL pgbench:
  - 20 clients, 4 threads
  - 5469 transactions processed
  - 0 failed transactions
  - average latency: 218.691 ms
  - throughput: 91.453305 tps
- Redis redis-benchmark:
  - simple commands remained strong and slightly higher in the later run
  - representative results: PING_INLINE 45106.00 rps, SET 53447.35 rps, GET 40600.89 rps, MSET 26181.44 rps
  - larger reads again showed the expected size penalty: LRANGE_500 2529.66 rps, p50=17.215 ms; LRANGE_600 2133.24 rps, p50=18.607 ms

### Readout

- PostgreSQL stayed stable with zero failures in both rounds.
- Raising concurrency from 10 to 20 clients roughly doubled average PostgreSQL latency while throughput stayed in the same band, which is a normal saturation shape for a private control-data node.
- Redis remained healthy under benchmark load, with simple operations much faster than large range reads.
- These results are good baseline evidence for Server 2. Read together, they reflect a simultaneous Server 2 stress sample from separate terminals, though a stricter shared-ceiling test would still be useful if we want to measure exact contention.

## Server 4 Stress Results

Server 4 was validated as the Neo4j and cleanup-services host behind the existing Server 1 and Server 3 flow. The graph stress path still entered through Server 1, but the live Bolt target was Server 4 at `172.27.23.85:7687`, so these results reflect the practical graph ceiling of the current Server 4-backed path.

### Server 4 Specification

- Host: `node-22`
- Role: `linkx-graph-maintenance`
- Address: `172.27.23.85`
- Runtime:
  - Neo4j Docker Compose deployment
  - Python venv + systemd cleanup services
- Services verified live:
  - Neo4j Bolt on `7687`
  - Neo4j Browser on `7474` for admin-only access
  - `linkx-cleanup-worker`
  - `linkx-cleanup-scheduler`

### Security And Service Posture

- UFW restricts `7687/tcp` to Server 1 `172.27.23.95`, Server 3 `172.27.23.18`, and Server 4 itself `172.27.23.85`.
- UFW restricts `7474/tcp` to the admin workstation `172.20.107.14`.
- Docker publishes Neo4j on `0.0.0.0`, but the firewall posture keeps exposure aligned with the security handoff.
- Cleanup services were active during this pass and continued to process scheduled jobs successfully.

### Graph Stress Setup

- Runner: external desktop machine on the same network
- Tool: `k6`
- Entry target: `http://172.27.23.95:8000`
- Graph target configured in session: `bolt://172.27.23.85:7687`
- Request flow exercised:
  - `POST /auth/login`
  - `POST /init`
  - `POST /connect_to_tool`
  - `POST /graph_link`
  - `POST /get_graph`
- Measured outcome: practical ceiling of the full API -> worker -> Neo4j path with Server 4 as the graph host

### Graph Route Results With Server 4 Neo4j

| VUs | p95 latency | Failure rate | Notes |
|---|---:|---:|---|
| 5 | 649.66 ms | 0.00% | Healthy baseline |
| 10 | 1.43 s | 0.00% | Passes the current `1.5s` threshold |
| 11 | 1.44 s | 0.00% | Highest passing point under the current threshold |
| 12 | 1.58 s | 0.00% | First threshold crossing |
| 13 | 1.58 s | 0.00% | Still above threshold |
| 15 | 1.97 s | 0.00% | Clear overload signal for latency |

### Cleanup-Service Validation

The cleanup enqueue path was also validated directly on Server 4 after loading the service `.env` into the shell. Manual enqueue calls succeeded and returned real cleanup run ids:

- `session_tree` -> `3caec53e-4dca-4b74-8414-adf27656727e`
- `neo4j_session` -> `163b0530-cace-4547-b3cb-432b421beae9`
- `metadata_prune` -> `df64ebc1-8c25-47eb-b3d6-f235af3df88f`
- `artifacts_expired` -> `3b43f979-019c-4ea8-97dd-14daefa59a2f`

Additional live evidence from the worker logs during this session:

- scheduled `artifacts_expired`, `metadata_prune`, `abandoned_sessions`, and `neo4j_residue_scan` runs continued to finish successfully
- a prior `window` cleanup completed successfully using `source=managed_secret`, proving the managed-secret Neo4j credential path works on this host
- the manual enqueue command initially failed until the service environment was loaded, which confirms these maintenance commands depend on the same `.env` contract as the running systemd services

### Readout

- Server 4 is healthy and properly fenced from a network perspective under the current UFW rules.
- The practical graph ceiling of the current Server 4-backed path is about `11 VUs` under the existing `p95 < 1.5s` rule.
- Latency rises before outright request failure, so the first limit is responsiveness rather than correctness.
- Cleanup services are functioning and can accept manual maintenance jobs when run with the service environment loaded.

### Interpretation

- Server 1 is functionally healthy for auth and control-plane traffic.
- The session/control-plane routes stay acceptable up to about 15 VUs and start crossing the `1.5s` threshold at 16 VUs.
- The graph routes are slightly heavier and cross the `1.5s` threshold at 12 VUs.
- The STR route is not currently a valid capacity benchmark because the linking logic is unavailable in the deployed environment, so the high failure rate there is expected and should not be treated as a server-wide regression.
- For now, use the control-plane and graph-route runs as the meaningful capacity signals for Server 1.

### Practical Operating Range

- Comfortable baseline: `10-15 VUs` for auth/session/control-plane traffic
- Useful stress check: `11-15 VUs` for graph routes
- Above `15 VUs`, both control-plane and graph latency rise enough that the API becomes noticeably slow for interactive use

### Test Notes

- Do not use the login rate limiter as the main stress signal.
- If a future run reintroduces login failures, verify the live `linkx-api` environment file and rate-limit settings before treating the result as a capacity regression.
- The STR search/dataframe tests need the linking logic to be present before they can be used as a real performance benchmark.
- After each test, restore any temporary auth or rate-limit changes back to the hardened production values.

## Recommended Operating Rhythm

The initial priority implementation is complete. Future security work should now run as continuous assurance rather than a time-based roadmap.

1. Run role-specific drift checks after every backend deploy or manual server change.
2. Keep GitHub `security-regression`, `dependency-audit`, and `secret-scan` green before merging.
3. Re-run backup verification after backup config, SSH key, retention, or target changes.
4. Capture restore-drill evidence after meaningful data shape changes, especially for Neo4j and artifacts.
5. Treat TLS/mTLS, stronger secrets management, off-host backup encryption, and monitoring/alerting as the next maturity candidates rather than emergency blockers.

## Residual Risk Register

| Area | Current state | Remaining risk | Next maturity move |
|---|---|---|---|
| East-west transport | Parent/JWKS HTTPS enforcement and transport validator are implemented; private Redis/Postgres/Neo4j may still use plaintext private-network protocols | A private-network observer or compromised host could inspect or tamper with internal traffic | Add TLS for Postgres/Redis/Neo4j and consider mTLS/service identity for backend services |
| Secrets management | `.env` and managed-secret handling are hardened, redaction works, and secret decrypt proof is verified | Manual `.env` distribution and rotation still depend on operator discipline | Move highest-risk secrets to systemd credentials, SOPS, Vault, KMS, or another auditable secret mechanism |
| Backup recovery | Scheduled local backups and off-host sync are live; PostgreSQL restore and secret decrypt proof passed | Artifact restore evidence and representative non-empty Neo4j restore evidence still need capture | Repeat isolated restore drills after representative data exists and record evidence in this handoff |
| Infrastructure policy | Drift scripts verify deployed app/security state on all four roles | Firewall, package baseline, and host policy are not fully managed as code | Add host firewall checks, package update checks, and eventually IaC/policy-managed deployment |
| Monitoring and response | Security audit events and CI checks exist | Failed backups, failed drift checks, suspicious auth events, and secret-scan failures need stronger alerting paths | Add alert routing and incident-response runbooks tied to the checks already implemented |

## Session Handoff Guidance

For the next engineering session:

- Start from the residual risk register rather than the old P0-P3 implementation list.
- Confirm live servers still match this file by running `verify-linkx-server.py` on Server 1-4 and `verify-linkx-backups.py` on backup-owning roles.
- Make future secure states default in code/config, not just operational notes.
- After each completed security change, update this file with evidence, commands used, residual risk, and whether the 7.5/10 maturity rating should change.
