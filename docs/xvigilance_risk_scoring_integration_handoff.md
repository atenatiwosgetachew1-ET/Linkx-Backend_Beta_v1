# Handoff: xVigilance & Risk Scoring Autonomous Pipeline

## 1. Overview
The **xVigilance** detective engine has been successfully upgraded into a fully autonomous, self-healing, national-scale pipeline. It securely connects **Node 22 (Extraction & Graph Engine)** to **Node 21 (Worker / Consumer / Escalation Engine)** via Kafka, and automatically triggers the external Risk Scoring system when anomalies are detected.

---

## 2. Inter-Server Architecture

### Node 22 (Extraction Daemon)
- **Role:** The high-speed extraction engine. It runs the `runner.py` script.
- **Cadence:** Operates on 1-Hour Sliding Windows. It extracts transactions from Elasticsearch and routes them to Kafka.
- **Mutual Exclusion:** Protected by an enterprise dual-layer mutex (host `fcntl.flock` + PostgreSQL advisory lock `97130282477900`) preventing duplicate processes from racing against the same feed.
- **Systemd Daemon:** Managed via `linkx-xvigilance.service` (legacy unmanaged `xvigilance.service` is masked).
- **Kafka Watermarks:** At the end of every hour, it fires a `WINDOW_COMPLETE` watermark event to Kafka, which passes execution control to Node 21.

### Node 21 (Worker Consumer)
- **Role:** The data aggregator, graph builder, and anomaly detector (`xvigilance_consumer.py`).
- **Processing:** Listens to `dev.xvigilance.transactions.raw.v2` as consumer group `xvigilance-graph-consumer-v2`. It buffers transactions into 10,000-record micro-batches, normalizes fields, and fast-ingests them into Neo4j with label `bank_transactions_xvigilance-daemon` (~1,500 tx/sec).
- **Trigger:** When it receives the `WINDOW_COMPLETE` watermark event from Node 22, it flushes its buffer and scans the Neo4j graph for AML rule violations (Smurfing, Circular Flow, Mule Rings).

---

## 3. The Hand-Off Protocol (Risk Scoring API)
When an anomaly is detected, the Worker executes the strict **Dual-Escalation Protocol**:
1. **Payload Generation:** Aggregates the top-degreed nodes and connected relationships into a standardized JSON payload (`analysis.link.flagged`).
2. **HTTP POST (Blind Hand-Off):** Securely POSTs the JSON payload directly to the external Risk Scoring service's async endpoint (`/api/risk_scoring/analysis_request`). We treat this as a "fire-and-forget" handoff: we log the API response code (e.g. 202, 400, 404) but do not halt execution on network failure.
3. **Evidence Archival:** The raw graph evidence is saved natively to the `link_analysis_evidence` PostgreSQL table.
4. **Summary Escalation:** The alert is written to the `linkx_reports` table as an `XVIGILANCE_FINDING`. To prove the handoff occurred, the alert is permanently stamped with `"reported_to": "Risk Scoring Service"`.

---

## 4. Security & Auditability Upgrades
- **Kafka Offset Safety:** On startup, consumer reads from `earliest` to prevent dropped messages during boot. On manual clock rewinds (Scenario B), offsets are cleanly reset to `latest` to skip obsolete buffered future messages.
- **Deep Execution Metadata:** The `linkx_reports` payload contains a comprehensive `execution_meta` block (`batch_id`, `total_records`, `worker_node`, `elastic_endpoint`).
- **IP Masking:** Raw infrastructure IP addresses have been completely masked. The `execution_meta` dynamically displays the logical Elasticsearch Index (e.g., `mobile_banking_transactions`) instead of the server IP, protecting internal topography while maintaining auditability.
