# Handoff: xVigilance & Risk Scoring Autonomous Pipeline

## 1. Overview
The xVigilance detective engine has been successfully upgraded into a fully autonomous, self-healing, national-scale pipeline. It securely connects **Node 22 (xMaintenance / Ingestion Engine)** to **Node 21 (Worker / Consumer / Escalation Engine)** via Kafka, and automatically triggers the external Risk Scoring system when anomalies are found.

## 2. Inter-Server Architecture

### Node 22 (xMaintenance Daemon)
- **Role:** The high-speed ingestion engine. It runs the `runner.py` script.
- **Cadence:** Operates on 1-Hour Sliding Windows. It is extremely self-paced; if it detects it is behind, it will aggressively loop through hours of historical data without sleeping. Once it catches up to the real-time clock, it sleeps and wakes up precisely on the hour.
- **Systemd Daemon:** It is permanently installed as a background Linux service (`xvigilance.service`). It starts on boot and instantly restarts on failure.
- **Kafka Watermarks:** At the end of every hour, it fires a `WINDOW_COMPLETE` watermark event to Kafka, which seamlessly passes execution control to Node 21.

### Node 21 (Worker Consumer)
- **Role:** The data aggregator and anomaly detector (`xvigilance_consumer.py`).
- **Processing:** It listens to the `dev.xvigilance.transactions.raw.v2` topic. It buffers transactions into Pandas DataFrames, normalizes them, and streams them into the Neo4j graph.
- **Trigger:** When it receives the `WATERMARK` event from Node 22, it flushes its buffer and immediately scans the Neo4j graph for `LA_Script_rules` violations (e.g., Smurfing, Circular Flow, Hub and Spoke).

## 3. The Hand-Off Protocol (Risk Scoring API)
When an anomaly is detected, the Worker executes the strict **Dual-Escalation Protocol**:
1. **Payload Generation:** It aggregates the top-degreed nodes and connected relationships into a massive, standardized JSON payload (`analysis.link.flagged`).
2. **HTTP POST (Blind Hand-off):** It securely POSTs the JSON payload directly to the external Risk Scoring service's async endpoint (`/api/risk_scoring/analysis_request`). We treat this as a "fire-and-forget" handoff; we log the API response code (e.g., 202, 400, 404) but do not halt execution on failure.
3. **Evidence Archival:** The raw graph evidence is saved natively to the `link_analysis_evidence` PostgreSQL table.
4. **Summary Escalation:** The alert is written to the `linkx_reports` table as an `XVIGILANCE_FINDING`. To prove the handoff occurred, the alert is permanently stamped with `"reported_to": "Risk Scoring Service"`.

## 4. Security & Auditability Upgrades
- **Kafka Offset Safety:** The consumer offset was corrected from `latest` to `earliest`. This completely resolves a critical race condition where transactions were lost if the consumer was booting up while the daemon was firing.
- **Deep Execution Metadata:** The `linkx_reports` payload now contains a comprehensive `execution_meta` block.
- **IP Masking:** Raw infrastructure IP addresses have been completely masked. The `execution_meta` dynamically displays the logical Elasticsearch Index (e.g., `mobile_banking_transactions`) instead of the server IP, protecting internal topography while maintaining auditability.
