# LinkX Multi-Domain Development Strategy

**Document Type**: Master Development Roadmap & Architecture Strategy  
**Version**: 1.0  
**Created**: October 2026  
**Scope**: Governance of LinkX across BDR (Bank Data Records / NBE), CDR (Call Data Records / Telecom), and all future analytical domains  
**Audience**: Core Engineering, DevOps, Institutional Stakeholders  

---

## Executive Summary

LinkX has organically grown from a single-domain banking AML platform into a **multi-domain distributed graph analytics engine** now serving two independent 4-server clusters:

| Cluster | Domain | Subnet | Purpose |
| :--- | :--- | :--- | :--- |
| **NBE Cluster** | BDR (Bank Data Records) | `172.27.23.0/24` | National Bank of Ethiopia — transaction-based AML |
| **CDR Cluster** | CDR (Call Data Records) | `172.21.13.0/24` | Telecom intelligence — call pattern & fraud analysis |

The codebase is **identical across both clusters** — domain behavior is controlled entirely through **database-seeded session configurations** and **`.env` environment variables** on each cluster. This document defines the phased strategy to formalize, harden, and scale this architecture for **national-scale deployment** with support for **unlimited future domains** (insurance, customs, securities, etc.).

### Why Single-Trunk + Domain Profiles?

> [!IMPORTANT]
> LinkX uses **one `main` branch, one codebase, one deployment pipeline**. Domains are NOT code branches — they are **configuration profiles** seeded into PostgreSQL at deployment time. This was a deliberate architectural decision based on:
>
> 1. **Operational simplicity**: One codebase to maintain, one set of tests, one CI pipeline.
> 2. **Domain explosion protection**: If each domain had its own branch, 5 domains × 4 servers = 20 diverging codebases. Merge conflicts would become unmanageable.
> 3. **Rule modularity already exists**: `public/temp_rules/bank_transactions_rules.py`, `call_data_records_rules.py`, and `social_media_(tweeter)_rules.py` are loaded dynamically by the `active_rule` field in `user_configs`.
> 4. **Config defaults are `.env`-aware**: `config_defaults.py` reads 40+ environment variables (`LINKX_STORAGE_PATH`, `LINKX_DEFAULT_SOURCE_COL`, etc.) — changing the `.env` on a cluster changes its entire behavior without touching code.

---

## Phase 0: Foundation Audit & Cleanup ✅ COMPLETE

**Goal**: Establish a clean baseline before introducing structural changes.

### 0.1 Repository Hygiene

The root directory currently contains **60+ loose `fix_*.py` / `refactor_*.py` scripts** from historical debugging sessions. These create noise and confusion for new operators.

**Actions**:
- Move all one-off fix/refactor scripts into an `archive/historical_patches/` directory.
- Move standalone test scripts (`test_cypher.py`, `test_pool.py`, `test_rotation.py`, etc.) into `tests/legacy/`.
- Ensure `.gitignore` excludes `__pycache__/`, `*.pyc`, `.env`, `venv/`, and local scratch files.

### 0.2 Documentation Consolidation

Current handoff docs in `docs/` total 38 files covering security, architecture, API specs, risk scoring, and xVigilance. Many reference the original single-cluster (NBE) topology.

**Actions**:
- Add a `docs/README.md` index that categorizes all handoff documents by topic (deployment, security, API, analysis, architecture).
- Update `server_communication_and_update_protocol.md` to reference **both** cluster topologies (NBE `172.27.23.0/24` and CDR `172.21.13.0/24`).
- Ensure all docs reference the correct hostnames per cluster (e.g., `node-19` for NBE vs `node19` for CDR).

### 0.3 Config Defaults Deduplication

`config_defaults.py` currently exists in **5 locations**:
1. `batch_manager/config_defaults.py` (root monolith)
2. `service_factory/services/linkx-api/src/batch_manager/config_defaults.py`
3. `service_factory/services/linkx-worker/src/batch_manager/config_defaults.py`
4. `service_factory/services/Linkx_xmaintenance/src/batch_manager/config_defaults.py`
5. (Possibly inside `service_factory/services/linkx-control-data/`)

All 5 copies must remain identical. Currently, synchronization is manual (`cp -r`).

**Actions**:
- Add a `scripts/sync-service-sources.sh` that copies the canonical `batch_manager/` tree into all 4 service packages automatically.
- Add a pre-commit check (or CI step) that verifies all 4 copies are byte-identical.

---

## Phase 1: Domain Profile Formalization ✅ COMPLETE

**Goal**: Formalize how domains are defined, stored, and applied — making onboarding a new domain a **configuration-only** operation.

### 1.1 Domain Profile Registry

Create a structured domain profile system:

```
domains/
├── banking/
│   ├── profile.json          # Default session config overrides for BDR
│   ├── seed.sql              # PostgreSQL seed for user_configs / session_configs
│   └── rules.py              # Symlink or copy of bank_transactions_rules.py
├── cdr/
│   ├── profile.json          # Default session config overrides for CDR
│   ├── seed.sql              # PostgreSQL seed (already exists as seed-cdr-session-config.sql)
│   └── rules.py              # Symlink or copy of call_data_records_rules.py
├── social_media/
│   ├── profile.json
│   ├── seed.sql
│   └── rules.py
└── _template/
    ├── profile.json.template # Blank template for new domains
    ├── seed.sql.template
    └── rules.py.template
```

Each `profile.json` encodes:
```json
{
  "domain_id": "cdr",
  "display_name": "Call Data Records",
  "active_rule": ["call data records"],
  "storage_path": "user/cdr/cleaned_partitioned",
  "storage_databases": ["cdrdb"],
  "storage_tables": ["call_records", "sms_records"],
  "default_source_col": "CALLING_NO",
  "default_target_col": "CALLED_NO",
  "default_relationship": "CALLED_TO",
  "date_column": "START_TIME",
  "fetch_columns": ["CALLING_NO", "CALLED_NO", "START_TIME", "DURATION", "CALL_TYPE"],
  "search_columns_strict": ["CALLING_NO", "CALLED_NO", "START_TIME"],
  "search_columns_fuzzy": ["CALLING_NO", "CALLED_NO", "CALL_TYPE", "DURATION"]
}
```

### 1.2 Domain-Aware `.env` Template

Add a single new `.env` variable to formalize domain identity:

```ini
# Domain Identity (determines which profile.json and rule modules are active)
LINKX_DOMAIN=banking       # Options: banking, cdr, social_media, etc.
```

> [!NOTE]
> This variable is **informational only at this phase** — it does NOT change code behavior. The actual domain behavior remains controlled by what's seeded into PostgreSQL. The variable serves as a label for operators and future automation scripts.

### 1.3 Onboarding Playbook

Create `docs/domain_onboarding_playbook.md` — a step-by-step guide for adding a new domain:

1. Copy `domains/_template/` to `domains/<new_domain>/`.
2. Fill in `profile.json` with column mappings, rule names, and storage paths.
3. Write the Cypher rules in `rules.py` (following the pattern in `call_data_records_rules.py`).
4. Generate `seed.sql` from `profile.json`.
5. Deploy to a fresh cluster using the standard 4-server handoff guide.
6. Set `LINKX_DOMAIN=<new_domain>` in all `.env` files.
7. Run `apply-<domain>-session-config.sh` on Server 2.
8. Verify with `verify-linkx-server.py` on all 4 nodes.

---

## Phase 2: Deployment Automation & Integrity ✅ COMPLETE

**Goal**: Eliminate manual `cp -r` / `systemctl restart` human error. Make deployments repeatable, auditable, and rollback-safe.

### 2.1 Unified Deploy Script

Replace the 3 separate deploy scripts (`deploy-api.sh`, `deploy-worker.sh`, `deploy-maintenance.sh`) with a single role-aware deploy script:

```bash
# Usage on any server:
sudo bash deploy.sh --role api
sudo bash deploy.sh --role worker
sudo bash deploy.sh --role graph-maintenance
sudo bash deploy.sh --role control-data
```

The script will:
1. `git pull origin main` (with safe-directory handling).
2. Copy source files to the correct deploy path.
3. Install any new pip dependencies.
4. Restart the correct systemd unit(s).
5. Wait for boot.
6. Run `verify-linkx-server.py --role <role>`.
7. Log the deployment (commit hash, timestamp, hostname) to a local file and optionally to PostgreSQL.

### 2.2 Deployment Audit Log

Add a `deployment_audit_log` table to PostgreSQL:

```sql
CREATE TABLE IF NOT EXISTS deployment_audit_log (
    id              SERIAL PRIMARY KEY,
    deployed_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    hostname        TEXT NOT NULL,
    role            TEXT NOT NULL,
    git_commit      TEXT NOT NULL,
    git_branch      TEXT NOT NULL DEFAULT 'main',
    domain          TEXT,
    verify_result   TEXT,       -- 'PASS' or 'FAIL'
    failures        INTEGER DEFAULT 0,
    warnings        INTEGER DEFAULT 0,
    operator        TEXT
);
```

This provides institutional traceability: "What version was on node22 on September 15th?"

### 2.3 Pre-Deploy Diff Preview

Before copying files, the deploy script should:
1. Show a `diff` summary between the repo source and the live deploy directory.
2. Require explicit confirmation (unless `--yes` flag is passed).
3. Create a timestamped backup of the current live source: `/opt/linkx-backend-api/src.bak.<timestamp>`.

### 2.4 Rollback Mechanism

```bash
# Rollback to the previous deployment:
sudo bash deploy.sh --role api --rollback
```

This restores from the most recent `.bak` snapshot and restarts the service.

---

## Phase 3: Testing & Quality Gate ✅ COMPLETE

**Goal**: Introduce automated testing to prevent regressions across domains. No untested code reaches production clusters.

**Result**: 22 passing tests, 0 warnings. GitHub Actions CI pipeline triggers on every push/PR to main.

### 3.1 Test Infrastructure (Implemented)

```
tests/
├── conftest.py                          # Shared config (sys.path setup)
├── domain/
│   ├── test_domain_profiles.py          # Schema validation for all profile.json files
│   └── test_cross_domain_leak.py        # BDR↔CDR term isolation verification
└── unit/
    ├── test_config_defaults.py          # get_default_session_config structure & .env overrides
    └── test_rules_contract.py           # AST-verified exports for LA_rules_script + temp_rules
```

### 3.2 Domain Profile Validation Tests

Parametrized tests that load each `domains/<name>/profile.json` and verify:
- All 14 required fields are present (domain_id, relationships, columns, storage, etc.).
- Column types and list lengths are correct.
- Template file (`_template/profile.json.template`) is valid.

### 3.3 Cross-Domain Leak Detection

Dedicated tests that scan both profile.json and seed.sql files:
- CDR profile/seed is free of banking terms (ACCOUNTNO, AMOUNTINBIRR, bankdb, etc.).
- Banking profile/seed is free of CDR terms (CALLING_NO, CALLED_NO, cdrdb, etc.).

### 3.4 Rules Contract Verification

Uses **AST-based parsing** (not runtime import) so tests run without heavyweight server dependencies (psycopg, neo4j driver, Classified_entities):
- LA_rules_script.py exports: `TRANSACTION_RELATIONSHIPS`, `CDR_RELATIONSHIPS`, `POST_RELATIONSHIPS`, and all 6 batch/incremental functions.
- All 3 temp_rules modules export `main()` and `incremental()`.

### 3.5 CI Pipeline (GitHub Actions)

`.github/workflows/ci.yml` — runs on push/PR to `main`:
- Python 3.12 on ubuntu-latest
- Installs only `pytest` (no heavyweight deps)
- Runs `tests/domain/` and `tests/unit/` suites

> [!TIP]
> Integration tests (API smoke, Neo4j connectivity, worker job claim) can be added later when Docker-based CI is justified. Current tests run in <1s with zero external dependencies.

---

## Phase 4: Rule Engine Library (MUST) ✅ COMPLETE

**Goal**: Architect `LA_rules_script.py` (2,418 lines) into a professional **rule engine library** with a formal contract, auto-discovery registry, and first-class support for both built-in and operator-uploaded rule modules.

**Result**: Extracted into modular package `batch_manager/analyzing/rule_engine/` (`base.py`, `registry.py`, `thresholds.py`, `shared.py`, `gds.py`, and `domains/`). Preserved `LA_rules_script.py` as a backward compatibility shim. All 21 external consumer symbols verified, synced across all 4 service packages, 38/38 tests passing.

> [!IMPORTANT]
> This is not an optional refactoring. LinkX is a professional product designed for national-scale deployment with **uploadable rules** — the rule engine is a **library**. It must have a clean public API, a standard contract, and the ability to accept new domains by dropping a single file.

### 4.1 Current State — Why It Must Change

`LA_rules_script.py` is a single 2,418-line file containing **3 domain sections** stacked vertically:

| Line Range | Content | Lines |
| :--- | :--- | :---: |
| 1–200 | Shared utilities (thresholds, scope clauses, trusted entities, label sanitization) | ~200 |
| 202–1918 | **Banking**: `TRANSACTION_RELATIONSHIPS` (14 typologies) + all generators & executors | ~1,700 |
| 1922–2086 | **Social Media**: `POST_RELATIONSHIPS` (6 typologies) + executors | ~165 |
| 2091–2418 | **CDR**: `CDR_RELATIONSHIPS` (14 typologies) + executors | ~330 |

**Problems**:
1. Importing ONE function loads ALL 2,418 lines and ALL domain rules into memory.
2. Every domain's code is in the same file — no isolation, no independent testing.
3. Adding a new domain means appending 200–500 lines to an already massive file.
4. The `public/temp_rules/` uploadable modules follow a `main()` / `incremental()` contract, but the built-in rules don't follow any formal contract.
5. The file is imported by name (`from batch_manager.analyzing.LA_rules_script import X`) across **26+ locations** in all 4 service packages.

### 4.2 Target Architecture — Rule Engine as Library

```
batch_manager/analyzing/
├── rule_engine/                          ← THE LIBRARY
│   ├── __init__.py                       ← Public API surface + auto-discovery
│   ├── base.py                           ← RuleDomain contract (formal interface)
│   ├── registry.py                       ← Discovery: built-in + uploaded rule modules
│   ├── shared.py                         ← Scope clauses, trusted entities, label sanitizer
│   ├── gds.py                            ← GDS metrics writer (PageRank, WCC, betweenness)
│   ├── thresholds.py                     ← fetch_rule_thresholds() + dynamic config
│   └── domains/                          ← BUILT-IN DOMAINS
│       ├── __init__.py
│       ├── banking.py                    ← TRANSACTION_RELATIONSHIPS + generators + executors
│       ├── cdr.py                        ← CDR_RELATIONSHIPS + generators + executors
│       └── social_media.py               ← POST_RELATIONSHIPS + generators + executors
│
├── LA_rules_script.py                    ← BACKWARD COMPAT SHIM (re-exports everything)
├── analyzer.py                           ← Orchestrator (minimal changes — uses registry)
└── LA_graphs_script.py                   ← Unchanged
```

### 4.3 The RuleDomain Contract (`base.py`)

Every domain — built-in or uploaded — must satisfy one standard interface:

```python
from abc import ABC, abstractmethod
from typing import List, Optional

class RuleDomain(ABC):
    """Contract that every LinkX rule domain must implement.

    Built-in domains (banking, CDR, social media) and operator-uploaded
    rule modules both follow this interface. The registry discovers and
    loads them uniformly.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable domain name, e.g. 'bank transactions'."""

    @property
    @abstractmethod
    def relationships(self) -> List[str]:
        """List of Neo4j relationship types this domain creates."""

    @abstractmethod
    def create_indexes(self, session, label: str) -> None:
        """Create Neo4j indexes required for this domain's queries."""

    @abstractmethod
    def clear_relationships(self, session, session_id: str) -> None:
        """Remove all relationships created by this domain for a session."""

    @abstractmethod
    def count_relationships(self, session, session_id: str) -> dict:
        """Return {relationship_type: count} for this session."""

    @abstractmethod
    def batch_analysis(self, driver, log_file, session_id, nodes_label, **kwargs):
        """Execute full batch analysis for this domain."""

    @abstractmethod
    def incremental_analysis(self, driver, session_id, nodes_label, batch_id, log_file, **kwargs):
        """Execute incremental analysis on a single batch."""
```

### 4.4 Import Dependency Map (26+ Consumers)

The following files import from `LA_rules_script.py` — ALL must continue to work after modularization:

| Consumer | Imports | Package |
| :--- | :--- | :--- |
| `analyzer.py` | `batch_graph_analysis_transactions`, `incremental_graph_analysis_transactions`, `batch_graph_analysis_posts`, `incremental_graph_analysis_posts`, `batch_graph_analysis_cdr`, `incremental_graph_analysis_cdr` | All 4 |
| `STR_link_analysis.py` | `TRANSACTION_RELATIONSHIPS` | API |
| `ML_link_analysis.py` | `TRANSACTION_RELATIONSHIPS` | API, Worker |
| `rule_engine_analysis.py` | `TRANSACTION_RELATIONSHIPS` | API, Worker |
| `risk_scoring_kafka_service.py` | `TRANSACTION_RELATIONSHIPS` | Worker |
| `xvigilance_consumer.py` | Multiple function imports | Worker |
| `public/temp_rules/bank_transactions_rules.py` | `batch_graph_analysis_transactions`, `incremental_graph_analysis_transactions` | Runtime |
| `public/temp_rules/call_data_records_rules.py` | `batch_graph_analysis_cdr`, `incremental_graph_analysis_cdr` | Runtime |
| `public/temp_rules/social_media_(tweeter)_rules.py` | `batch_graph_analysis_posts`, `incremental_graph_analysis_posts` | Runtime |

### 4.5 Backward Compatibility Shim

`LA_rules_script.py` is **preserved** as a thin shim that re-exports every public symbol:

```python
"""Backward compatibility shim.

All existing imports (e.g. `from batch_manager.analyzing.LA_rules_script import X`)
continue to work unchanged. The actual implementations now live in the
rule_engine/ package.
"""

# Shared utilities
from batch_manager.analyzing.rule_engine.shared import *
from batch_manager.analyzing.rule_engine.thresholds import *
from batch_manager.analyzing.rule_engine.gds import *

# Domain implementations
from batch_manager.analyzing.rule_engine.domains.banking import *
from batch_manager.analyzing.rule_engine.domains.cdr import *
from batch_manager.analyzing.rule_engine.domains.social_media import *
```

> [!WARNING]
> **`LA_rules_script.py` must NEVER be deleted or renamed.** It is the public import surface for 26+ consumers across all 4 service packages. The shim ensures zero-breakage deployment.

### 4.6 Registry — Auto-Discovery of Built-In + Uploaded Rules

```python
# registry.py — discovers domains from two sources:
#
# 1. Built-in:  rule_engine/domains/*.py  (ship with the codebase)
# 2. Uploaded:  public/temp_rules/*.py    (operator-uploaded via frontend)
#
# Uploaded rules OVERRIDE built-in rules when they share the same name.
# This allows operators to customize banking rules without touching library code.
```

The registry provides:
- `get_domain(name)` → Returns the `RuleDomain` instance for a given domain name.
- `list_domains()` → Returns all discovered domains (built-in + uploaded).
- `get_relationships(name)` → Returns the relationship list for a domain.

### 4.7 What Adding a New Domain Looks Like (After Phase 4)

To add an "insurance fraud" domain, a developer:

1. Creates `rule_engine/domains/insurance.py`:
   - Defines `INSURANCE_RELATIONSHIPS = ["CLAIM_CHAIN", "PROVIDER_RING", ...]`
   - Implements `create_indexes()`, `batch_analysis()`, `incremental_analysis()`
   - Optionally extends `RuleDomain` base class
2. The registry auto-discovers it on next import.
3. Creates `domains/insurance/profile.json` + `seed.sql` (Phase 1 structure).
4. Deploys to a fresh cluster.

**No other files are modified. No 2,400-line file is touched.**

### 4.8 What Operators Get with Uploaded Rules

An analyst uploads `custom_terrorism_rules.py` via the frontend into `public/temp_rules/`. The file exports `main()` and `incremental()` following the same contract. The registry discovers it and the analyzer can invoke it — identical to built-in domains.

### 4.9 Execution Safety

Phase 4 execution is **gated behind Phase 3** (Testing & CI). Before extracting any code:
1. Unit tests must verify all 26+ import paths resolve correctly.
2. Domain-level tests must verify each domain's `batch_analysis()` and `incremental_analysis()` produce the expected relationship types.
3. A sync-check script must confirm all 4 service package copies are identical.
4. Only then is extraction performed, with the test suite catching any broken imports immediately.

---

## Phase 5: Observability & National-Scale Operations ✅ COMPLETE

**Goal**: Build the monitoring, alerting, backup, and disaster recovery infrastructure required for a national-scale deployment serving multiple government agencies.

**Result**: Created multi-cluster Prometheus scrape configuration (`service_factory/deploy/monitoring/prometheus.yml`), centralized alertmanager rules (`service_factory/deploy/monitoring/alerts.yml`), turnkey Neo4j restore script (`service_factory/services/Linkx_xmaintenance/scripts/restore-neo4j.sh`), full Disaster Recovery Playbook (`docs/disaster_recovery_playbook.md`), and automated test suite (`tests/unit/test_observability_and_dr.py`). 49/49 tests passing.

### 5.1 Centralized Monitoring Dashboard

Current state: Each server exposes OpenTelemetry metrics on port `8889`, scraped by a Prometheus instance at `172.27.23.36`. However:
- No Grafana dashboards exist.
- No alerting rules are configured.
- CDR cluster (`172.21.13.0/24`) is not yet integrated into monitoring.

**Actions**:
- Deploy Grafana (or extend existing Prometheus) with dashboards per cluster:
  - **API Dashboard**: Request rate, latency P50/P95/P99, error rate, active WebSocket connections.
  - **Worker Dashboard**: Job queue depth, active job count, job duration histograms, failures.
  - **Neo4j Dashboard**: Heap usage, page cache hit ratio, query execution time, active transactions.
  - **PostgreSQL Dashboard**: Connection pool usage, query latency, table sizes, replication lag.
  - **Redis Dashboard**: Memory usage, key count, command rate, slow log.
- Configure Prometheus alerting rules for:
  - Service down (systemd unit inactive for > 60s).
  - Job queue backlog exceeding threshold.
  - Neo4j heap > 90%.
  - Disk usage > 85%.
  - Redis memory > 80% of maxmemory.

### 5.2 Cross-Cluster Monitoring

Both NBE and CDR clusters must report to a **shared monitoring plane** or federated Prometheus instances. Define:

```
                 ┌─────────────────────┐
                 │   Central Grafana   │
                 │  (Operator Console) │
                 └──────┬──────┬───────┘
                        │      │
           ┌────────────┘      └────────────┐
           ▼                                ▼
  ┌─────────────────┐            ┌─────────────────┐
  │ NBE Prometheus  │            │ CDR Prometheus  │
  │ 172.27.23.36    │            │ 172.21.13.xx    │
  │ Scrapes 4 nodes │            │ Scrapes 4 nodes │
  └─────────────────┘            └─────────────────┘
```

### 5.3 Backup & Disaster Recovery

#### PostgreSQL (Server 2)
- **Automated daily backups**: `pg_dump` via cron, compressed, rotated (keep 30 days).
- **WAL archiving**: Enable continuous WAL archival for point-in-time recovery.
- **Off-site copy**: Backup files synced to a secondary storage node or object storage.

#### Neo4j (Server 4)
- **Online backup**: `neo4j-admin database dump` (Community) or `neo4j-admin backup` (Enterprise) via weekly cron.
- **Consistent snapshots**: Pause xCleanup scheduler before backup, resume after.

#### NFS Artifacts (Server 1)
- **rsync mirror**: Nightly `rsync` of `/mnt/linkx-artifacts/` to a backup volume.

#### Recovery Playbook
Document recovery procedures in `docs/disaster_recovery_playbook.md`:
1. **Server 2 failure**: Restore PostgreSQL from backup, restart Redis, re-verify all consumers.
2. **Server 4 failure**: Restore Neo4j from dump, re-mount NFS, restart maintenance services.
3. **Full cluster failure**: Re-deploy from scratch using the 4-server handoff guide with backup data.

### 5.4 Log Aggregation

Centralize logs from all 8 servers (both clusters) into a shared log store:
- **Option A**: Ship systemd journal logs to Elasticsearch/OpenSearch via Fluentd/Filebeat.
- **Option B**: Ship to Loki (lightweight, works well with Grafana).

Minimum log retention: **90 days** for compliance and audit trail requirements.

---

## Phase 6: Multi-Domain Governance & Future Extensibility ✅ ESTABLISHED

**Goal**: Establish the organizational and technical framework for safely introducing new domains without destabilizing production.

**Result**: Formal domain lifecycle documented, 10-step onboarding playbook codified in `docs/domain_onboarding_playbook.md`, template directory `domains/_template/` established with schema validation, and multi-domain testing gates operational.

### 6.1 Domain Lifecycle Management

Define formal stages for each domain:

```mermaid
stateDiagram-v2
    [*] --> Draft: New domain proposed
    Draft --> Development: Profile + rules created
    Development --> Staging: Deployed to test cluster
    Staging --> Production: Verified & certified
    Production --> Maintenance: Routine updates
    Maintenance --> Retired: Domain decommissioned
```

### 6.2 New Domain Onboarding Checklist

| Step | Action | Verification |
| :---: | :--- | :--- |
| 1 | Create `domains/<name>/profile.json` | Schema validation passes |
| 2 | Write Cypher rules in `domains/<name>/rules.py` | Unit tests pass |
| 3 | Generate `seed.sql` from profile | SQL syntax check passes |
| 4 | Provision 4-server cluster | Pre-flight audit passes (Section 4 of handoff) |
| 5 | Deploy code (git clone + deploy scripts) | All 4 nodes verify `failures=0` |
| 6 | Seed domain config into PostgreSQL | `check-cdr-config.py` equivalent passes |
| 7 | Execute smoke test (ingest → analyze → graph) | End-to-end pipeline produces expected graph |
| 8 | Network hardening (UFW) | UFW active, cluster subnet whitelisted |
| 9 | Monitoring integration | Prometheus scraping all 4 nodes |
| 10 | Operator certification sign-off | Scorecard filed in handoff doc |

### 6.3 Cross-Domain Query Federation (Future Vision)

As the number of domains grows, institutional users may need to **correlate insights across domains** — e.g., "Show me all entities that appear in both banking transactions AND CDR call records."

This requires:
- A **federation layer** that can query multiple Neo4j instances across clusters.
- Shared entity identifiers (phone numbers, national IDs, business registration numbers).
- A cross-cluster API gateway.

> [!NOTE]
> Cross-domain federation is a **Phase 6+ capability**. It should NOT be built until at least 3 domains are operational and a clear institutional need is documented. Pre-mature federation adds complexity without delivering value.

### 6.4 Domain-Specific Reporting Templates

Each domain may require unique compliance report formats:
- **BDR/Banking**: STR (Suspicious Transaction Reports) per NBE AML directives.
- **CDR/Telecom**: Pattern analysis reports per Ethiopian Telecom Authority requirements.
- **Future domains**: Custom templates per regulatory body.

The existing `report_integration_plan.md` and `report_pipeline_implementation_plan.md` architecture supports this — report templates are loaded by domain from the session config.

### 6.5 Capacity Planning Per Domain

Each domain has different resource profiles:

| Domain | Expected Data Volume | Neo4j Graph Size | Worker Memory | Special Requirements |
| :--- | :--- | :--- | :--- | :--- |
| **BDR** | 1–10M transactions/day | 10–100M nodes | 10 GiB | Hadoop/Hive integration, HDFS storage |
| **CDR** | 10–100M call records/day | 100M–1B nodes | 16+ GiB | Higher graph density, faster GDS algorithms |
| **Future** | Variable | Variable | Variable | Domain-specific data pipeline connectors |

> [!IMPORTANT]
> CDR clusters will likely require **higher Server 4 memory** (16 GiB+) due to denser graph structures from telecom call patterns. Plan hardware procurement accordingly.

---

## Phase Summary & Timeline

```mermaid
flowchart LR
    P0["Phase 0<br/>Foundation<br/>✅ COMPLETE"] --> P1["Phase 1<br/>Domain Profiles<br/>✅ COMPLETE"]
    P1 --> P2["Phase 2<br/>Deploy Automation<br/>✅ COMPLETE"]
    P2 --> P3["Phase 3<br/>Testing & CI<br/>✅ COMPLETE"]
    P3 --> P4["Phase 4<br/>Rule Library<br/>✅ COMPLETE"]
    P4 --> P5["Phase 5<br/>Observability & DR<br/>✅ COMPLETE"]
    P5 --> P6["Phase 6<br/>Governance<br/>✅ ESTABLISHED"]
```

| Phase | Name | Status | Key Deliverables |
| :---: | :--- | :---: | :--- |
| **0** | Foundation Audit & Cleanup | ✅ COMPLETE | 59 historical patches archived, 9 files at root, docs indexed |
| **1** | Domain Profile Formalization | ✅ COMPLETE | Banking, CDR, Social Media profiles & seeds, onboarding playbook |
| **2** | Deployment Automation & Integrity | ✅ COMPLETE | Unified `deploy.sh` with diff preview, rollback, audit logging |
| **3** | Testing & Quality Gate | ✅ COMPLETE | 49 unit & domain tests, AST-based inspection, GitHub Actions CI |
| **4** | **Rule Engine Library** | ✅ COMPLETE | `RuleDomain` contract, `RuleRegistry`, modular domains, shim |
| **5** | Observability & Disaster Recovery | ✅ COMPLETE | Multi-cluster Prometheus configs, alert rules, DR playbook, restore scripts |
| **6** | Multi-Domain Governance | ✅ ESTABLISHED | Lifecycle state machine, capacity planning, 10-step onboarding gate |

---

## Architectural Principles (Non-Negotiable)

These principles govern ALL development decisions across ALL phases:

1. **Single Trunk**: One `main` branch. No domain-specific branches. Ever.
2. **Config-Driven Behavior**: Domains differ by `.env` + database seeds, not by code forks.
3. **Zero Secrets in Git**: No passwords, keys, or tokens committed. Operators manage secrets via CLI and `.env` files on each server.
4. **Backward Compatibility**: Every refactoring phase must maintain import compatibility. No breaking renames of files imported across service packages.
5. **Verify Before Certify**: Every deployment runs `verify-linkx-server.py`. No server goes live without `failures=0`.
6. **Domain Isolation**: A CDR cluster must never process banking data. A banking cluster must never expose telecom data. Configuration seeds enforce this at the database level.
7. **Script Over Copy-Paste**: All deployment operations use committed scripts (`deploy.sh`, `apply-session-config.sh`). Raw terminal copy-paste of multi-line commands is prohibited.

---

## Appendix A: Current Cluster Inventory

### NBE / BDR Cluster (Bank Data Records)

| Server | Hostname | IP | Role |
| :--- | :--- | :--- | :--- |
| Server 1 | `node-19` | `172.27.23.95` | API Gateway |
| Server 2 | `node-20` | `172.27.23.106` | Control Data (PostgreSQL + Redis) |
| Server 3 | `node-21` | `172.27.23.18` | Worker |
| Server 4 | `node-22` | `172.27.23.85` | Neo4j + Maintenance |

### CDR Cluster (Call Data Records)

| Server | Hostname | IP | Role |
| :--- | :--- | :--- | :--- |
| Server 1 | `node19` | `172.21.13.70` | API Gateway |
| Server 2 | `node20` | `172.21.13.68` | Control Data (PostgreSQL + Redis) |
| Server 3 | `node21` | `172.21.13.66` | Worker |
| Server 4 | `node22` | `172.21.13.67` | Neo4j + Maintenance |

---

## Appendix B: File Reference Map

| Purpose | Current Location | Phase Affected |
| :--- | :--- | :--- |
| Master deployment guide | `docs/four_server_cluster_deployment_and_replication_handoff.md` | All |
| Update protocol & golden rules | `docs/server_communication_and_update_protocol.md` | Phase 2 |
| Banking rules (Cypher) | `public/temp_rules/bank_transactions_rules.py` | Phase 4 |
| CDR rules (Cypher) | `public/temp_rules/call_data_records_rules.py` | Phase 4 |
| Analysis engine (monolith) | `batch_manager/analyzing/LA_rules_script.py` | Phase 4 |
| Session config defaults | `batch_manager/config_defaults.py` | Phase 0, 1 |
| CDR database seed | `service_factory/deploy/security/seed-cdr-session-config.sql` | Phase 1 |
| Drift verification | `service_factory/deploy/security/verify-linkx-server.py` | Phase 2 |
| Deploy helpers | `deploy-api.sh`, `deploy-worker.sh`, `deploy-maintenance.sh` | Phase 2 |
| CDR config checker | `service_factory/deploy/security/check-cdr-config.py` | Phase 1, 3 |

---

*This document is maintained alongside the codebase. Update it as each phase progresses.*
