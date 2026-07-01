# LinkX Security Hardening Handoff

Last updated: 2026-07-01

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

- Findings below are based on repository code and deployment artifacts, not live server inspection.
- Where the existing handoff document says UFW or ops restrictions are already applied, treat those as operational mitigations only if the running servers still match that state.
- A few issues are already partially mitigated operationally, but still remain fragile because the secure state is not fully encoded into deployment defaults.

## Priority Summary Table

| Priority Group | Security feature | Condition | Potential risks | Servers |
|---|---|---|---|---|
| P0 | Redis network isolation and authentication | Partially implemented | Queue tampering, job manipulation, state poisoning, unauthorized internal access if firewall drifts or service is exposed | Server 2 |
| P0 | Secret redaction in STR analysis and related logs | Partially implemented | Plaintext Neo4j credentials or sensitive payload fragments leaking into stdout, journald, log shipping, support bundles | Server 1, Server 3 |
| P0 | AI partner least-privilege scoping | Partially implemented | Cross-session metadata access, artifact enumeration, excessive partner visibility if service token is misused or stolen | Server 1 |
| P1 | Token revocation and post-logout invalidation | Partially implemented | Stolen JWTs remain usable until expiry after logout, lock, or forced session termination | Server 1 |
| P1 | Internal transport protection for JWKS, partner, and east-west sensitive channels | Partially implemented | Credential interception, token interception, trust downgrade, internal MITM on flat/shared networks | Server 1, Server 2, Server 3, Server 4 |
| P1 | Deployable gateway configuration integrity | Partially implemented | Misconfigured nginx rollout, header stripping, auth breakage, accidental exposure during manual deployment | Server 1 |
| P2 | Service-account permission segmentation and audit depth | Partially implemented | Over-broad service capabilities, weak forensic visibility into partner reads, harder blast-radius control | Server 1 |
| P2 | Security configuration drift detection | Not implemented | UFW/systemd/compose drift can silently undo hardening without repo visibility | Server 1, Server 2, Server 3, Server 4 |
| P2 | Backup automation and recovery assurance for hardened state | Partially implemented | Recovery gaps for secrets, artifacts, graph state, or control-plane state during incident response | Server 2, Server 4 |
| P3 | Continuous security verification in CI/CD | Not implemented | Reintroduction of logging leaks, invalid configs, unsafe defaults, and auth regressions | Server 1, Server 2, Server 3, Server 4 |

## Priority Details

### P0

#### 1. Redis network isolation and authentication

Condition: Partially implemented

Why it is P0:

- The repo deployment artifact still publishes Redis on the host and the checked-in config does not enforce a password.
- Existing firewall rules described in the handoff reduce exposure only if they are still correctly applied on the live host.

Verified evidence:

- `service_factory/services/linkx-control-data/docker-compose.yml` publishes `6379:6379`
- `service_factory/services/linkx-control-data/redis/redis.conf` binds `0.0.0.0`
- `service_factory/services/linkx-control-data/redis/redis.conf` only comments that `requirepass` should be set in production

What to fix in this priority:

- Require Redis authentication by default in deployed configs or move to a managed private Redis.
- Bind Redis only to the required internal interface.
- Remove host port publishing if local host exposure is unnecessary.
- Add startup or deployment validation so Redis cannot start in production without an authenticated/private configuration.

Primary server concerns:

- Server 2

#### 2. Secret redaction in STR analysis and related logs

Condition: Partially implemented

Why it is P0:

- The codebase has a redaction model, but one analysis path still logs an analyzer payload that contains runtime tool credentials.
- This turns a hardening improvement elsewhere into a log-exposure problem.

Verified evidence:

- `service_factory/services/linkx-api/src/api/STR_link_analysis.py` builds `tool_credentials`
- The same file prints the full analyzer payload before execution

What to fix in this priority:

- Remove raw payload printing from STR analysis routes.
- Replace it with structured redacted logging if debugging is still needed.
- Review worker-side equivalents and adjacent analysis/job paths for similar direct prints.
- Rotate any credentials that may already have been exposed through retained logs.

Primary server concerns:

- Server 1
- Server 3

#### 3. AI partner least-privilege scoping

Condition: Partially implemented

Why it is P0:

- The AI service is correctly forced through Server 1 and no longer has direct DB/Neo4j access, which is good.
- But the current API permission model still gives the AI service broad read visibility across sessions and artifacts rather than session-scoped access.

Verified evidence:

- `service_factory/services/linkx-api/src/api/ai_service.py` exposes session, artifact, cleanup-run, and graph metadata reads under `ai:read`
- `service_factory/services/linkx-api/src/auth/repository.py` seeds an `ai` service account role with broad read permissions
- `docs/service_split_handoff_and_load_audit.md` states the AI partner should only use Server 1 APIs, which is already the right trust boundary

What to fix in this priority:

- Split `ai:read` into narrower permissions or policy checks.
- Scope AI reads to explicitly authorized sessions, owners, or request contexts.
- Add audit records for object-level `/ai/*` reads.
- Decide whether graph metadata and artifact metadata should require separate permissions.

Primary server concerns:

- Server 1

### P1

#### 4. Token revocation and post-logout invalidation

Condition: Partially implemented

Why it is P1:

- JWT issuance and validation are working, but access tokens remain valid until expiry even after logout or idle timeout.
- This is a meaningful control gap, but lower urgency than exposed infra or secret leakage.

Verified evidence:

- `service_factory/services/linkx-api/src/auth/routes.py` explicitly reports that current access tokens are not invalidated on logout or idle timeout
- `service_factory/services/linkx-api/src/auth/tokens.py` implements stateless HMAC-signed tokens without revocation tracking

What to fix in this priority:

- Add `jti` and token revocation or session-version checks.
- Separate service-token lifetime policy from browser/user token lifetime policy.
- Ensure admin disable, logout, and forced lock can revoke or supersede issued tokens.

Primary server concerns:

- Server 1

#### 5. Internal transport protection for JWKS, partner, and east-west sensitive channels

Condition: Partially implemented

Why it is P1:

- The design assumes internal trust, but several examples and env templates still use plaintext transport.
- This is less urgent than P0 if the network is tightly isolated, but it remains a real defense-in-depth gap.

Verified evidence:

- `docs/service_split_handoff_and_load_audit.md` references HTTP JWKS and HTTP API usage examples
- `service_factory/deploy/env/linkx-api.env.example` uses plaintext DSN examples for Postgres and Redis
- `service_factory/services/linkx-api/src/auth/jwks_client.py` retrieves JWKS via whatever URL is configured

What to fix in this priority:

- Move parent/CTMS JWKS and partner API traffic to HTTPS.
- Prefer encrypted internal transport for Postgres, Neo4j, and Redis where operationally feasible.
- Document explicit exceptions where private VLAN trust is accepted and why.

Primary server concerns:

- Server 1
- Server 2
- Server 3
- Server 4

#### 6. Deployable gateway configuration integrity

Condition: Partially implemented

Why it is P1:

- The repo already contains one cleaner gateway template, but it also contains a malformed server config that should not be promoted accidentally.

Verified evidence:

- `service_factory/services/linkx-api/deploy/nginx/linkx-api-server.conf` contains blank proxy header values and an invalid `map` block
- `service_factory/deploy/nginx/linkx-api-gateway.conf` is the stronger canonical-looking template

What to fix in this priority:

- Remove, replace, or clearly mark the malformed file as non-deployable.
- Keep one canonical gateway artifact.
- Add nginx config validation to deployment verification.

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

1. Finish all P0 items.
2. Re-verify the repo and live deployment state for the P0 changes.
3. Move to P1 only after P0 is closed.
4. Treat P2 as hardening depth and resilience work.
5. Treat P3 as continuous assurance once the higher-priority controls are in place.

## Session Handoff Guidance

For the next engineering session:

- Start by choosing one row from the priority summary table.
- Confirm whether the live servers still match the assumptions in `docs/service_split_handoff_and_load_audit.md`.
- Make the secure state the default in code/config, not just an operational note.
- After each completed priority item, update this file by changing its `Condition`, evidence notes, and residual risk.
