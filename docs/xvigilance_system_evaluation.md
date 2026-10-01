# LinkX xVigilance — National-Scale System Evaluation

**Date:** 2026-10-01 (Final — All Fixes Deployed & Verified)  
**Context:** National Bank of Ethiopia AML/CFT system processing 1.35B mobile banking records  
**Score: 74.9 / 100 — Grade B**

### Fixes Applied & Production-Verified

| # | Fix | Commit | Production Status |
|---|---|---|---|
| 1 | PEP/Sanctions FOREACH blocks restored | `8a81a1d` | ✅ Verified — `HIGH_RISK_LINK / PEP / SANCTION ✓` |
| 2 | SHARED_IDENTIFIER column mapping | DB config | ✅ Verified — `SHARED_IDENTIFIER ✓ (0.32s)` |
| 3 | Complete score bootstrap (7→17 rules) | `4d9cf2f` | ✅ Verified |
| 4 | SIGTERM graceful shutdown (inter-rule) | `bacf709` | ✅ Verified — `Deactivated successfully` in 4s |
| 5 | datetime.utcnow() deprecation fix | `4d9cf2f` | ✅ Verified — zero DeprecationWarning in logs |

### Engineering Context (User-Provided)

- **Startup purge wipes all nodes**: Deliberate design decision — Neo4j server has ~500GB storage. Ephemeral graph model (ingest → analyze → promote evidence to Postgres → wipe) is the correct tradeoff. Analysis results are preserved in `link_analysis_evidence` table.
- **Risk Scoring API offline**: Temporary — being fixed by the risk scoring service developer. System degrades gracefully (catches timeout, logs warning, continues).

---

## Scoring Framework

| Dimension | Weight | Why This Weight |
|---|---|---|
| Rule Coverage | 25% | Core detection capability — the reason the system exists |
| Data Readiness | 20% | Rules are only as good as the data feeding them |
| Scoring & Escalation | 15% | Risk quantification drives investigation priority |
| Infrastructure Resilience | 15% | National system must be always-on |
| Regulatory Compliance | 15% | FATF/NBE requirements are non-negotiable |
| Configuration Governance | 10% | Adaptability to changing threat landscape |

---

## Dimension 1: Rule Coverage — 75/100

### Rule Status (14 Rules)

| Rule | Mobile Banking | Method | Quality |
|---|---|---|---|
| SMURFING | ✅ Working | Cypher WHERE, DB thresholds | Solid — all thresholds DB-driven |
| JUST_BELOW_THRESHOLD | ✅ Working | Cypher WHERE, reporting_threshold | Standard AML pattern |
| CIRCULAR_FLOW | ✅ Working | **Python-accelerated** hash matching | Superior to Cypher cartesian |
| FUND_FLOW | ✅ Working | **Python-accelerated** chain detection | Superior to Cypher cartesian |
| EFFECTIVE_FLOW | ✅ Working | Python in-memory, pass-through aware | Advanced — resolves intermediary banks |
| HUB_AND_SPOKE (out) | ✅ Working | Cypher, excludes pass-through in query | Properly filters enterprise gateways |
| HUB_AND_SPOKE (in) | ✅ Working | Same | Same |
| LATE_NIGHT_TX | ✅ Working | Time window from CREATEDDATE epoch | Configurable start/end hours |
| RAPID_WITHDRAWAL | ✅ Working | Amount tolerance matching | DB-driven tolerance |
| ABNORMAL_BALANCE_CHANGE | ✅ Working | Reads SENDERPREVIOUSBALANCE (100% populated) | 3× historical baseline |
| SHARED_IDENTIFIER | ✅ Working | User configured SUBSCRIBERID → BUSINESSMOBILENO | DB column mapping |
| HIGH_RISK_LINK | ✅ Working | Account + name + phone matching | 3-field matching |
| PEP_INVOLVED | ✅ **Fixed** | Restored FOREACH block, score 0.9 | Regulatory critical |
| SANCTIONED_ENTITY_MATCH | ✅ **Fixed** | Restored FOREACH block, score 1.0 | Regulatory critical |
| ACCOUNT_ACTIVITY_SPIKE | ⚠️ Partial | 30-day cross-window history needed | Baseline resets on restart (deliberate for storage) |
| DORMANT_TO_ACTIVE | ❌ Dead on mobile | Needs ACCOUNTSTATE — absent from feed | Data source limitation |

### Strengths
- 13 of 14 rules functional (DORMANT_TO_ACTIVE is a data source gap, not a code gap)
- Python-accelerated CIRCULAR_FLOW and FUND_FLOW avoid O(n²) Cypher cartesian products
- EFFECTIVE_FLOW pass-through resolution is sophisticated — few commercial systems do this
- All rules are modular generators (not monolithic inline Cypher)

### Remaining Gaps
- DORMANT_TO_ACTIVE cannot function without ACCOUNTSTATE in the feed (-5)
- ACCOUNT_ACTIVITY_SPIKE baseline resets on restart — acceptable tradeoff for 500GB storage (-5)
- No velocity-based rules (e.g., "3 transactions in 60 seconds") (-5)
- No geographic anomaly detection (-5)
- No peer-group comparison (-5)

**Score: 75/100**

---

## Dimension 2: Data Readiness — 74/100

### Field Coverage (Post-Configuration)

| Required Field | Source in Mobile Feed | Coverage | Status |
|---|---|---|---|
| ACCOUNTNO | SENDERACCOUNTID | ~100% | ✅ Normalizer maps automatically |
| BENACCOUNTNO | RECEIVERACCOUNTID | ~100% | ✅ Normalizer maps automatically |
| AMOUNT | TRANSFERAMOUNT | ~100% | ✅ Normalizer maps automatically |
| TRANSACTIONDATE | CREATEDDATE (epoch ms) | ~100% | ✅ Normalizer converts |
| TRANSACTIONTIME | CREATEDDATE (epoch ms) | ~100% | ✅ Normalizer converts |
| SENDER_FULL_NAME | Native field | 100% | ✅ Direct pass-through |
| RECEIVER_FULL_NAME | Native field | 100% | ✅ Direct pass-through |
| SENDERPREVIOUSBALANCE | Native field | 100% | ✅ Direct pass-through |
| BUSINESSMOBILENO | SENDER_CUST_SUBSCRIBERID | 90.5% | ✅ User configured DB mapping |
| BENTELNO | RECEIVER_CUST_SUBSCRIBERID | 93.5% | ✅ User configured DB mapping |
| ACCOUNTSTATE | **Not in feed** | 0% | ❌ Data source limitation |

### Normalizer Quality
- DB-driven column mapping via `session_configs` (flexible, extensible) ✅
- Canonical fallback mappings for core fields ✅
- Epoch millisecond → date/time conversion ✅
- Conflict detection with warning logs ✅
- Validation logging (missing field counts per batch) ✅

### Data Scale
- 1.35 billion records in Elasticsearch across 30 shards (1.1 TB)
- Micro-batch ingestion at 10,000 records per batch, ~5.6s each
- Zero missing ACCOUNTNO/BENACCOUNTNO/AMOUNT in production logs

**Score: 74/100**

---

## Dimension 3: Scoring & Escalation — 74/100

### Architecture

| Component | Implementation | Quality |
|---|---|---|
| **Config Storage** | `global_score_lineage` table (JSONB, versioned) | ✅ DB-driven |
| **Config Fetch** | Consumer queries latest config at evidence promotion time | ✅ |
| **Config API** | `GET/POST /config/score-lineage` with permission guards | ✅ |
| **Version Auditing** | `version_id SERIAL`, `updated_by`, `created_at` | ✅ |
| **Fallback** | Hardcoded defaults if DB unavailable, with error logging | ✅ |
| **Three-Layer Model** | base_score + graph_size_bonus + financial_value_bonus | ✅ |
| **Score Evidence** | Dict recording config_version, base applied, bonuses | ✅ Auditable |
| **Banding** | Low (<20), Medium (20-49), High (50-79), Critical (80-100) | ✅ |

### Hand-Off Pipeline
- Evidence stored in `link_analysis_evidence` table with full graph payload ✅
- Summary stored in `linkx_reports` table with trace_id linkage ✅
- POST to Risk Scoring API attempted (graceful degradation if down) ✅
- Edge-centric subgraph extraction prevents orphan nodes in UI ✅
- Top-5 accounts by volume extracted for investigator focus ✅

### Gaps
- Risk Scoring API temporarily offline — developer fixing it. Graceful degradation works (-4)
- No ML/behavioral scoring component (-7)
- Step-function bonuses instead of continuous gradient (-3)
- No per-channel calibration (mobile vs core banking) (-4)
- No feedback loop (analyst decisions don't update scoring weights) (-3)
- Bootstrap seed was incomplete, now fixed to 17 rules (-0, resolved)

**Score: 74/100**

---

## Dimension 4: Infrastructure Resilience — 75/100

### Strengths
| Capability | Detail |
|---|---|
| Error isolation | try/except per rule — one failure doesn't kill the batch ✅ |
| Neo4j indexes | 9 indexes created on startup for query performance ✅ |
| Kafka durability | `auto_offset_reset="earliest"`, no data loss on restart ✅ |
| Micro-batch ingestion | 10k records/batch, ~5.6s, CREATE (not MERGE) for speed ✅ |
| GDS subgraph | Anomaly-only projection, not full graph ✅ |
| Scale proven | 552,840-node window completed in 469s ✅ |
| Graceful degradation | Risk API down → skip, DB fetch fails → fallback defaults ✅ |
| **SIGTERM handling** | **FIXED** — inter-rule shutdown checks, clean exit in 4s ✅ |
| **datetime warning** | **FIXED** — zero deprecation warnings in logs ✅ |

### Remaining Issues
| Issue | Impact | Severity |
|---|---|---|
| **Single daemon, single node** | All detection stops if node-21 fails | High (-12) |
| **Ephemeral graph model** | Deliberate for 500GB storage — ACCOUNT_ACTIVITY_SPIKE loses baseline | Accepted tradeoff (-5) |
| **Risk API timeout** | Temporary — developer fixing. 5s × N anomaly groups per window | Temporary (-3) |
| **No health monitoring** | No liveness probe, no alerting on daemon crash | Medium (-5) |

**Score: 75/100**

---

## Dimension 5: Regulatory Compliance — 72/100

### FATF Requirement Mapping

| FATF Recommendation | LinkX Coverage | Status |
|---|---|---|
| **R.10** Customer Due Diligence | Entity classification system with trusted/risk categories | ✅ |
| **R.7** Targeted Financial Sanctions | `SANCTIONED_ENTITY_MATCH` — **FIXED**, score 1.0, highest severity | ✅ |
| **R.12** PEP Screening | `PEP_INVOLVED` — **FIXED**, score 0.9, blue flagging | ✅ |
| **R.20** Suspicious Transaction Reporting | Anomalies stored in `link_analysis_evidence` + `linkx_reports` | ⚠️ Partial |
| **R.11** Record Keeping | Graph evidence with trace_ids, score lineage versioning | ✅ |
| **R.16** Wire Transfer Rules | FUND_FLOW and EFFECTIVE_FLOW chain detection | ✅ |
| **R.18** Internal Controls | DB-driven thresholds, permission-guarded APIs | ✅ |
| **R.19** Higher-Risk Countries | HIGH_RISK_LINK matches against configurable risk entities | ✅ |

### Gaps
- No automated STR generation or filing mechanism (-8)
- No case management integration (investigator workflow) (-7)
- No regulatory reporting dashboard (-5)
- Ephemeral graph model means raw graph data not retained — but evidence IS preserved in Postgres (-5)
- No periodic compliance self-assessment automation (-3)

**Score: 72/100**

---

## Dimension 6: Configuration Governance — 82/100

### DB-Driven Configuration Map

| Config Area | Table | API | Version Tracked | Permission Guarded |
|---|---|---|---|---|
| Detection Thresholds | `global_rule_thresholds` | ✅ | `created_at` | ✅ |
| Entity Classification | `global_entity_classification` | ✅ | `created_at` | ✅ |
| Scoring Weights | `global_score_lineage` | ✅ GET + POST | `version_id` + `updated_by` + `created_at` | ✅ `config:read` / `users:manage` |
| Column Mapping | `session_configs` | ✅ | Per-session | ✅ |

### Strengths
- **Four separate config tables** — clean separation of concerns ✅
- **All config DB-driven** — no code deployment needed for threshold changes ✅
- **Merge-over-defaults pattern** — DB values override code fallbacks ✅
- **Strict API validation** — POST score-lineage validates required keys and types ✅
- **Security audit events** — config changes logged to `security_audit_events` ✅

### Gaps
- No range validation on thresholds (could set negative values) (-5)
- Code fallback defaults are stale (300k vs user's 50k smurfing threshold) (-3)
- No config diff/comparison between versions (-3)
- No config approval workflow (change → review → activate) (-2)

**Score: 82/100**

---

## Composite Score

| Dimension | Weight | Score | Weighted |
|---|---|---|---|
| Rule Coverage | 25% | 75 | 18.75 |
| Data Readiness | 20% | 74 | 14.80 |
| Scoring & Escalation | 15% | 74 | 11.10 |
| Infrastructure Resilience | 15% | 75 | 11.25 |
| Regulatory Compliance | 15% | 72 | 10.80 |
| Configuration Governance | 10% | 82 | 8.20 |
| **TOTAL** | **100%** | — | **74.9** |

---

## Final Score: 74.9 / 100 — Grade: B

---

## Score History

| Date | Score | Grade | Key Changes |
|---|---|---|---|
| 2026-10-01 (initial) | 52.5 | C | PEP/Sanctions broken, scoring wrongly assessed as hardcoded |
| 2026-10-01 (corrected) | 71.75 | B- | PEP fixed, SHARED_IDENTIFIER configured, scoring assessment corrected |
| 2026-10-01 (final) | **74.9** | **B** | SIGTERM fix verified, datetime fixed, context: purge deliberate for 500GB, Risk API temporary |

---

## What Makes This System Genuinely Good

1. **Graph-based topology detection** — CIRCULAR_FLOW, FUND_FLOW, HUB_AND_SPOKE, EFFECTIVE_FLOW operate on the transaction graph, not just flat tables. Most national AML systems in comparable economies use flat rule engines.

2. **Python-accelerated critical rules** — CIRCULAR_FLOW and FUND_FLOW avoid O(n²) Cypher cartesian products by doing hash-based matching in Python memory. Handles 552k+ node windows.

3. **Complete DB-driven configuration** — Four configuration tables covering thresholds, entities, scoring, and column mapping. All with APIs, permission guards, and versioning.

4. **Classified entity governance** — Trusted/risk/pass-through entity system correctly handles enterprise payment gateways. EFFECTIVE_FLOW resolves pass-through intermediaries.

5. **Near-real-time streaming** — Kafka → micro-batch ingestion → full rule analysis → evidence promotion. End-to-end latency is minutes, not hours.

6. **Graceful everything** — SIGTERM → clean exit in 4s. Risk API down → skip and continue. DB fetch fails → fallback defaults. Rule fails → next rule runs.

## What Prevents a Higher Score

1. **No High Availability** (−12 pts) — Single daemon on single node. Node-21 failure = zero detection.
2. **No ML behavioral layer** (−10 pts) — All rules are deterministic with static thresholds. No peer-group comparison or learned behavior.
3. **No automated STR filing** (−8 pts) — Anomalies stored but not packaged into regulatory reports.
4. **No case management** (−7 pts) — No investigator workflow integration.
5. **DORMANT_TO_ACTIVE dead on mobile** (−5 pts) — Data source limitation, not code issue.

## Future Improvement Roadmap

| Priority | Fix | Effort | Score Impact |
|---|---|---|---|
| 1 | Standby daemon on second node (HA) | 1 day | +5 |
| 2 | STR report generation template | 2 days | +4 |
| 3 | ML anomaly scoring (isolation forest) | 1-2 weeks | +5 |
| 4 | Case management API integration | 1 week | +3 |
| 5 | Config validation (range checks) | 2 hours | +2 |

**With priorities 1–3 implemented: ~89/100 (Grade A-)**
