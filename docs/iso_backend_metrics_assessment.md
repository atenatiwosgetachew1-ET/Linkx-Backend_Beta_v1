# LinkX Backend ISO Metrics Assessment

Last updated: 2026-07-10

This assessment covers the backend-side ISO metrics that can be evaluated from the current LinkX repository and the deployed backend services. Frontend-only rendering metrics are excluded.

## Scope

Backend roles under assessment:

- Server 1: `linkx-api`
- Server 2: `linkx-control-data`
- Server 3: `linkx-worker`
- Server 4: `linkx-graph-maintenance`

Frontend is separate and is not included in this backend assessment except where explicitly marked as frontend-side.

## Observed Results

| Category | Performance Metric | Acceptance Criteria | Observed Result | Status |
|---|---|---|---|---|
| Graph Database | Cypher Query Response Time | `< 300 ms` | Full session-scoped metadata Cypher query completed in `243 ms`; node count query completed in `64 ms`; relationship count query completed in `280 ms`. | `Pass under tested condition` |
| Relationship Analysis | End-to-End Analysis Time | `≤ 2 sec` | Graph-route `iteration_duration p95` was `820.75 ms` at `1 VU`, `1.48 s` at `5 VUs`, and `2.95 s` at `10 VUs`. | `Pass at 1 VU and 5 VUs; not met at 10 VUs` |
| Relationship Analysis | Graph Analysis Throughput | `≥ 5,000 analyses/sec` | Re-verified graph-route throughput was `1.47724 analyses/sec` at `1 VU`, `4.473422 analyses/sec` at `5 VUs`, and `4.94063 analyses/sec` at `10 VUs`. | `Not met under tested condition` |
| Relationship Analysis | Multi-hop Relationship Discovery (5 hops) | `< 1 sec` | A valid `5-hop` session-scoped path was returned in `257 ms`. | `Pass under tested condition` |
| Relationship Analysis | Relationship Mapping Accuracy | `100%` | Backend correctness evidence passed in `8/8` unit tests covering exact session/batch/run metadata and relationship status scoping. | `Pass under backend correctness tests` |
| Detection Accuracy | Circular Money Flow Detection Rate | `≥ 99%` | Raw-data circular pair query returned `475` expected circular pairs; detected `CIRCULAR_FLOW` relationships were `950`, which equals `475` detected pairs after directional normalization. | `Pass under tested condition` |
| Detection Accuracy | Hidden Relationship Discovery Rate | `≥ 95%` | `FUND_FLOW` detection was evidenced with `3099` detected relationships in session `3_553796`, but `SHARED_IDENTIFIER` cases were not present in the tested sessions and no labeled truth set was available. | `Detection evidenced; final accuracy not formally validated` |
| Detection Accuracy | High-Risk Cluster Detection Accuracy | `≥ 98%` | `HIGH_RISK_LINK` and `HUB_AND_SPOKE` cluster-like structures were observed during active ingestion, but the graph state was still changing and no labeled ground truth was available. | `Detection observed during active ingestion; final accuracy not yet stable for evaluation` |
| Detection Accuracy | PEP Relationship Detection Accuracy | `≥ 99%` | Risk-entity-driven `HIGH_RISK_LINK` detections were observed, with surrounding `FUND_FLOW` and `HUB_AND_SPOKE` structure, but no labeled PEP truth set was available. | `Detection evidenced through HIGH_RISK_LINK; final accuracy not formally validated` |
| Graph Traversal | Maximum Graph Traversal Depth | `≥ 10 hops` | A valid exact `10-hop` session-scoped path was returned in `132 ms`, and a supporting query confirmed `max_hops_reached = 10`. | `Pass under tested condition` |
| Visualization | Investigation Graph Rendering Time | `≤ 2 sec` | Frontend-side metric. Backend can only prove payload delivery and graph metadata availability. | `Frontend-only` |
| API Performance | API Response Time | `< 300 ms` | Control-plane `http_req_duration avg` was `135.95 ms` at `1 VU`, `271.91 ms` at `5 VUs`, and `583.87 ms` at `10 VUs`. Graph-route `http_req_duration avg` was `209.54 ms` at `1 VU`, `442.91 ms` at `5 VUs`, and `995.13 ms` at `10 VUs`. | `Pass at 1 VU; mixed at 5 VUs; not met at 10 VUs` |
| Reliability | Service Availability | `≥ 99.95%` | No service availability issues were observed during recent usage over approximately `1-2 months`, but no formal uptime measurement window was recorded. | `Operationally observed pass` |
| Reliability | Successful Analysis Rate | `≥ 99.99%` | `0.00%` request failures and `100.00%` check success were observed at `1 VU`, `5 VUs`, and `10 VUs` in the current control-plane and graph-route k6 runs. | `Pass at 1 VU, 5 VUs, and 10 VUs` |
| Reliability | Horizontal Scaling Efficiency | `≥ 90%` | Re-verified scaling efficiency from the graph-route baseline was `60.6%` at `5 VUs` and `33.4%` at `10 VUs` relative to `1 VU`. | `Not met under tested condition` |

## Supporting Evidence

### Re-run backend correctness checks

These unit checks were re-run successfully in the service virtualenv:

- `service_factory/.venv/bin/python -m unittest service_factory/services/linkx-api/src/tests/test_graph_metadata.py`
- `service_factory/.venv/bin/python -m unittest service_factory/services/linkx-api/src/tests/test_graph_status_events.py`

Combined result:

- `8 tests OK`

### Latest Server 1 workload rechecks

The latest rechecks used the dedicated k6 scripts:

- `tests/stress/k6/server1_session_control_plane.js`
- `tests/stress/k6/server1_graph_routes.js`

Observed control-plane results:

- `1 VU`: `avg 135.95 ms`, `p95 202.93 ms`, `iteration p95 797.19 ms`, `0.00% failures`
- `5 VUs`: `avg 271.91 ms`, `p95 448.07 ms`, `iteration p95 1.46 s`, `0.00% failures`
- `10 VUs`: `avg 583.87 ms`, `p95 928.24 ms`, `iteration p95 2.76 s`, `0.00% failures`

Observed graph-route results:

- `1 VU`: `avg 209.54 ms`, `p95 306.97 ms`, `iteration p95 820.75 ms`, `0.00% failures`
- `5 VUs`: `avg 442.91 ms`, `p95 694.67 ms`, `iteration p95 1.48 s`, `0.00% failures`
- `10 VUs`: `avg 995.13 ms`, `p95 1.48 s`, `iteration p95 2.95 s`, `0.00% failures`

Re-verified graph-route throughput:

- `1 VU`: `1.47724 analyses/sec`
- `5 VUs`: `4.473422 analyses/sec`
- `10 VUs`: `4.94063 analyses/sec`

### Graph-query and traversal proofs

Cypher timing proof:

- Session-scoped node count query: `64 ms`
- Session-scoped relationship count query: `280 ms`
- Full metadata hot-path query: `243 ms`

Traversal proof:

- Exact `5-hop` path returned in `257 ms`
- Exact `10-hop` path returned in `132 ms`
- `max_hops_reached = 10`

### Detection rule checks

Circular-flow proof for session `1_478697`:

- `expected_circular_pairs = 475`
- `detected_circular_relationships = 950`
- `detected_circular_pairs = 475`

Hidden/high-risk/PEP-oriented evidence:

- Session `3_553796` returned `3099` `FUND_FLOW` relationships
- Tested sessions returned `0` `SHARED_IDENTIFIER` relationships and `0` expected shared-identifier groups
- High-risk cluster-like activity was observed through `HIGH_RISK_LINK` and `HUB_AND_SPOKE`
- Risk-entity-driven `HIGH_RISK_LINK` detections were observed and surrounded by `FUND_FLOW` and `HUB_AND_SPOKE` patterns

## Interpretation

- The backend passes the isolated Cypher hot-path timing target under the tested query condition.
- The backend meets the end-to-end graph path target at lighter workloads, but not under the tested `10 VU` load.
- API response time is acceptable at `1 VU`, mixed at `5 VUs`, and not within the stated target at `10 VUs`.
- Successful request execution remained stable across the tested `1 / 5 / 10 VU` workloads.
- Throughput and horizontal scaling efficiency do not meet the stated target values under the tested conditions.
- Circular detection was directly validated against expected raw-data pairs.
- Hidden/high-risk/PEP-style detections are evidenced, but some accuracy rows remain partially constrained by missing labeled truth sets and, for high-risk cluster observations, active ingestion state.
- Frontend rendering time remains outside backend scope.

## Related Handoff Documents

- [docs/server1_stress_visuals.md](/var/www/linkx-backend/docs/server1_stress_visuals.md)
- [docs/security_hardening_handoff.md](/var/www/linkx-backend/docs/security_hardening_handoff.md)
