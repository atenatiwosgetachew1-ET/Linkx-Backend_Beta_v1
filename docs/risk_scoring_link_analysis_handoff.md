# Risk Scoring Link Analysis Pipeline & Kafka Integration Handoff

**Date**: 2026-08-18  
**Scope**: LinkX Worker (`service_factory/services/linkx-worker`), Risk Scoring Kafka Pipeline (`batch_manager/services/risk_scoring_kafka_service.py`), PostgreSQL Evidence Ledger, and Cluster Deployment (`node-19` to `node-22`).

---

## 1. Executive Summary

The Risk Scoring Link Analysis service was enhanced, hardened, and automated to connect LinkX directly to the Risk Decision Platform Kafka ecosystem.

### Key Capabilities Delivered:
1. **Automated End-to-End Pipeline**: Consumes incoming `score.calculated` events from Kafka, runs targeted storage queries, executes Neo4j graph typology rules, prepares standardized response envelopes, and publishes responses automatically.
2. **Platform Topic & Header Alignment**:
   * **Inbound Topic**: `dev.scoring.score.calculated.v1`
   * **Outbound Topic**: `dev.analysis.link.mapped.v1` (carries both normal `link.mapped` and flagged `link.flagged` events).
   * **Partition Key**: Pinned to `accountno` (or `aggregation_key.value`) to guarantee message ordering for downstream Aggregator.
   * **Transport Headers**: Emits W3C `traceparent`, `X-Correlation-ID`, and `content-type: application/json` on every Kafka record.
3. **Optimized Flagging & Linked Entities Formatting**:
   * **`flagged_rules`**: Emits only the names of rules that were actually triggered (e.g. `["HUB_AND_SPOKE"]`).
   * **`linked_entities`**: Includes only counterparties participating in the flagged rule relationship.
   * **Risk-Sorted Entities**: Entities are strictly sorted by `risk_contribution` DESCENDING so critical high-risk entities are never dropped.
   * **Incremental IDs & Dynamic Key**: Zero-padded sequential IDs (`"01"`, `"02"`, ...) and dynamic key binding (e.g. `"accountno": "<value>"` replacing generic `entity_type`).
   * **Configurable Cap**: Reads `LINKX_RISK_SCORING_MAX_LINKED_ENTITIES` from environment/`.env` (default `50`), overridable via `data.max_linked_entities`.
4. **PostgreSQL Evidence Persistence & Idempotency**:
   * Stores complete request and response snapshots in `link_analysis_evidence` table.
   * Deduplication check avoids re-running expensive graph analysis for duplicate/replayed events.
5. **Direct In-Memory Processing**: Eliminates disk serialization churn for small transaction sets (≤ 1,000 rows).

---

## 2. Modified & Created Files

| File Path | Description of Changes |
| :--- | :--- |
| [`service_factory/services/linkx-worker/src/batch_manager/services/risk_scoring_kafka_service.py`](file:///var/www/linkx-backend/service_factory/services/linkx-worker/src/batch_manager/services/risk_scoring_kafka_service.py) | Full risk scoring service implementation: Step 0 sanitization, exact-match storage lookups, Neo4j typology analysis, evidence persistence, Kafka producer with transport headers, and background consumer loop. |
| [`service_factory/deploy/env/linkx-worker.env.example`](file:///var/www/linkx-backend/service_factory/deploy/env/linkx-worker.env.example) | Added `LINKX_RISK_SCORING_MAX_LINKED_ENTITIES=50` setting. |
| [`tests/test_risk_scoring_sanitization.py`](file:///var/www/linkx-backend/tests/test_risk_scoring_sanitization.py) | Unit tests verifying sanitization, response formatting, dynamic columns, and evidence idempotency. |
| [`docs/risk_scoring_link_analysis_handoff.md`](file:///var/www/linkx-backend/docs/risk_scoring_link_analysis_handoff.md) | Comprehensive engineering handoff document. |

---

## 3. Standard Message Envelopes

### 3.1 Inbound Request (`dev.scoring.score.calculated.v1`)
```json
{
  "schema_version": "1.0",
  "success": true,
  "event_type": "score.calculated",
  "data": {
    "transaction_id": "ft2518312526;1",
    "entity_id": "1000558269034",
    "is_entity": false,
    "max_linked_entities": 50
  },
  "meta": {
    "trace_id": "4a8b79c3d1e2f4a5b6c7d8e9f0123456",
    "span_id": "c71a39f048e21ba0",
    "traceparent": "00-4a8b79c3d1e2f4a5b6c7d8e9f0123456-c71a39f048e21ba0-01",
    "correlation_id": "corr-1000558269034-test",
    "timestamp": "2026-08-18T07:35:00.000Z",
    "aggregation_key": {
      "type": "accountno",
      "value": "1000558269034"
    }
  }
}
```

### 3.2 Outbound Flagged Response (`dev.analysis.link.mapped.v1`)
```json
{
  "schema_version": "1.0",
  "success": true,
  "event_type": "link.flagged",
  "message": "Link flagged for account 1000558269034: 388 linked",
  "data": {
    "accountno": "1000558269034",
    "entity_id": "1000558269034",
    "linked_accounts_count": 388,
    "flagged_entity_links": 1,
    "beneficiary_blacklisted": true,
    "flagged_rules": [
      "HUB_AND_SPOKE"
    ],
    "linked_entities": [
      {
        "entity_id": "01",
        "accountno": "1000582849138",
        "relationship": "HUB_AND_SPOKE",
        "risk_contribution": 0.85,
        "flagged": true,
        "flag_reason": "flagged rule relationship (HUB_AND_SPOKE)"
      },
      {
        "entity_id": "02",
        "accountno": "1000350224924",
        "relationship": "HUB_AND_SPOKE",
        "risk_contribution": 0.85,
        "flagged": true,
        "flag_reason": "flagged rule relationship (HUB_AND_SPOKE)"
      }
    ],
    "network_centrality_score": 0.88,
    "max_path_length": 2
  },
  "meta": {
    "trace_id": "4a8b79c3d1e2f4a5b6c7d8e9f0123456",
    "span_id": "f83a21b490c37de1",
    "traceparent": "00-4a8b79c3d1e2f4a5b6c7d8e9f0123456-f83a21b490c37de1-01",
    "correlation_id": "corr-1000558269034-test",
    "timestamp": "2026-08-18T07:35:01.240Z",
    "service": {
      "name": "link-analysis-ms",
      "version": "1.0.0",
      "namespace": "risk-decision-platform"
    },
    "messaging": {
      "system": "kafka",
      "destination_name": "dev.analysis.link.mapped.v1",
      "operation_name": "publish"
    },
    "source_id": "link",
    "aggregation_key": {
      "type": "accountno",
      "value": "1000558269034"
    },
    "processing": {
      "duration_ms": 68.2
    }
  },
  "error": null
}
```

---

## 4. PostgreSQL Evidence Schema (`link_analysis_evidence`)

```sql
CREATE TABLE IF NOT EXISTS link_analysis_evidence (
    id BIGSERIAL PRIMARY KEY,
    trace_id TEXT NOT NULL,
    correlation_id TEXT,
    transaction_id TEXT,
    entity_id TEXT NOT NULL,
    entity_type TEXT NOT NULL DEFAULT 'accountno',
    session_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    is_flagged BOOLEAN NOT NULL DEFAULT FALSE,
    flagged_rules JSONB,
    linked_accounts_count INT NOT NULL DEFAULT 0,
    network_centrality_score NUMERIC(5, 2),
    max_path_length INT,
    duration_ms NUMERIC(10, 2),
    request_payload JSONB NOT NULL,
    response_payload JSONB NOT NULL,
    analyzed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_evidence_trace_entity UNIQUE (trace_id, entity_id)
);

CREATE INDEX IF NOT EXISTS idx_link_evidence_entity ON link_analysis_evidence (entity_id, analyzed_at DESC);
CREATE INDEX IF NOT EXISTS idx_link_evidence_flagged ON link_analysis_evidence (is_flagged, analyzed_at DESC);
CREATE INDEX IF NOT EXISTS idx_link_evidence_tx ON link_analysis_evidence (transaction_id);
```

---

## 5. Cluster Deployment Verification Commands

### Server 1 (`node-19` — `172.27.23.95` API Host):
```bash
sudo git -C /opt/linkx-backend-update pull origin main
sudo cp -r /opt/linkx-backend-update/service_factory/services/linkx-api/src/. /opt/linkx-backend-api/src/
sudo systemctl restart linkx-api
sudo systemctl status linkx-api --no-pager -n 5
```

### Server 3 (`node-21` — `172.27.23.18` Worker Host):
```bash
sudo git -C /opt/linkx-backend-update pull origin main
sudo cp -r /opt/linkx-backend-update/service_factory/services/linkx-worker/src/. /opt/linkx-worker/src/
sudo systemctl restart linkx-worker
sudo systemctl status linkx-worker --no-pager -n 5
```
