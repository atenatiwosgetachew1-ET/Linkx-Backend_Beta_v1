# Handoff: xVigilance & Risk Scoring Integration and Pipeline Architecture

## 1. Overview & System Clarifications
This document details the architecture, boundaries, and interaction protocols between the **External Risk Scoring Service** and LinkX's internal **xVigilance Detective Engine**.

### Clarifying the Service Distinction
- **External Risk Scoring Service**: An external consumer and analytical platform that queries LinkX for network risk indicators (centrality scores, path lengths, flagged counterparties) to compute aggregate credit/AML risk scores.
- **LinkX xVigilance Engine**: LinkX's internal, autonomous 24/7 background detective engine. It continuously streams and inspects live banking transaction batches from Elasticsearch/Kafka into Neo4j.
- **Why xVigilance Escalates to Risk Scoring**: When xVigilance autonomously discovers severe AML typologies (Smurfing, Circular Fund Flows, Mule Rings), it escalates these findings to the external Risk Scoring system. This ensures the Risk Scoring system stays informed of LinkX's proactive autonomous findings, even if no explicit on-demand query was initiated.

---

## 2. Infrastructure & Domain Separation

### BDR Cluster (Bank Data Records / NBE AML — Subnet `172.27.23.0/24`)
**All Risk Scoring operations and bank data transactions run strictly on the BDR cluster:**
- **Node 19 (`172.27.23.95`)**: LinkX API Gateway (`linkx-api.service`). Exposes `POST /api/v1/risk-score/analyze`.
- **Node 20 (`172.27.23.106`)**: Message Bus (Kafka on port `9092`) and Control Plane (PostgreSQL + Redis).
- **Node 21 (`172.27.23.18`)**: Worker Processing Engine (`linkx-worker.service`) and xVigilance Consumer (`linkx-xvigilance-consumer.service`).
- **Node 22 (`172.27.23.85`)**: Extraction Runner (`linkx-xvigilance.service`) and Graph Database Host (Neo4j Bolt port `7687`).
- **Storage Node (`172.27.23.43`)**: Hosts dual Elasticsearch endpoints and HDFS (`hdfs://172.27.23.43:9000`):
  - **Port `5000` (`bank_ctr_individual`)**: Casual Bank CTR transaction search (`/api/search/uii`). Uses exact `[term]` keyword matching on `accountno` and `benaccountno`. No native Elasticsearch range filtering (filtered in-memory).
  - **Port `5007` (`mobile_banking_transactions`)**: Mobile Banking & Microfinance search (`/api/search/uii` via `mb_search_api.py`, 1.35B+ records). Supports native server-side time-window querying via `date_from` and `date_to` on `CREATEDDATE`, plus amount filtering (`min_amount`, `max_amount`). Key account columns are `SENDERACCOUNTID` and `RECEIVERACCOUNTID`.

### CDR Cluster Isolation (Call Data Records / Telecom — Subnet `172.21.13.0/24`)
The CDR cluster (`172.21.13.66` - `172.21.13.70`) is physically and logically distinct from BDR. Previous telecom session configuration cleanups (`seed-cdr-session-config.sql`) occurred exclusively on the CDR cluster and have zero effect on BDR or the Risk Scoring service.

---

## 3. Communication Protocols: Current Ingress vs. Future Kafka Roadmap

### 3.1 Current Ingress Protocol: HTTP / Webhook (v2.0 Schema)
The external Risk Scoring developer currently invokes LinkX via HTTP:
1. **Endpoint**: `POST /api/v1/risk-score/analyze`
2. **Dual Query Modes**:
   - **Full-History Flow (Default when `time_window` is omitted)**:
     - Directly queries Elasticsearch `/api/search/uii` on `172.27.23.43` across all historical transaction data.
     - Bypasses artificial date filtering; all transactions retrieved are ingested into Neo4j without row mismatches.
   - **Dynamic Scoped Flow (When `time_window` is provided)**:
     - Scopes transactions to `[start_date, end_date]`.
     - Intelligently handles both Unix millisecond epoch timestamps and formatted ISO strings.
     - Dynamically synchronizes `active_dataframe_rows` to ensure Neo4j integrity checks pass.
     - Gracefully returns a 0-score success payload when zero transactions fall within the requested window.
3. **Caching & Asynchronous Delivery**:
   - **Fast Lane (Cached Single Account)**: Instant HTTP 200 return if analyzed within the 2-hour cache window with matching time parameters.
   - **Worker Async Flow**: Returns HTTP 202 (`ACCEPTED`) and enqueues background processing on Node 21. Upon completion, the worker dispatches the v2.0 payload via webhook to `https://risk-platform.local/api/v1/aggregate/callback`.

### 3.2 Autonomous Escalation Protocol (xVigilance -> Risk Scoring)
When xVigilance independently detects AML typologies on Node 21:
1. **Payload Generation**: Aggregates top-degree nodes and flagged relationships into `analysis.link.flagged`.
2. **HTTP Escalation Hand-Off**: Dispatches the payload to `/api/risk_scoring/analysis_request` with fire-and-forget fault tolerance.
3. **Audit Ledger**: Stores raw evidence in `link_analysis_evidence` and logs an `XVIGILANCE_FINDING` in `linkx_reports` stamped with `"reported_to": "Risk Scoring Service"`.

### 3.3 Future Kafka Roadmap (Preserved Async Architecture)
As part of the roadmap to transition external Risk Scoring to fully asynchronous event-driven streaming, the following Kafka architecture remains designated for future implementation:
- **Inbound Analysis Requests**: Topic `dev.scoring.score.calculated.v1` (Broker `172.27.23.106:9092`).
- **Outbound Link Analysis Findings**: Topic `dev.analysis.link.mapped.v1` carrying mapped and flagged graph typologies.
- **Internal xVigilance Stream**: Topic `dev.xvigilance.transactions.raw.v2` streaming extraction batches from Node 22 to Node 21.
- **Partition Key**: Partitioned by `accountno` to guarantee downstream chronological ordering.

---

## 4. Verification and Security
- **IP Masking**: External reports and execution metadata mask internal server IPs, referencing logical data indices (e.g. `mobile_banking_transactions`).
- **Mutex Concurrency**: Node 22 extraction is guarded by an enterprise dual-layer mutex (`fcntl.flock` + PostgreSQL advisory lock `97130282477900`).
- **Auditability**: Complete request, response, and graph metrics are permanently archived in the PostgreSQL `link_analysis_evidence` table.

---

## 5. Multi-Tenant Service Account Storage & Dynamic Schema Routing

### 5.1 Service Accounts Registry & Storage Binding
Sibling services authenticate via Bearer JWT or static `X-API-Key`. Each service account in PostgreSQL `service_accounts` maintains an assigned storage target in its `config` JSONB column:

```json
{
  "domain": "microfinance",
  "storage_host": "172.27.23.43",
  "storage_port": "5007",
  "storage_endpoint": "api/search/uii",
  "auth_token": "optional-bearer-token"
}
```

Registered sibling accounts on Node 20:
- **`ai` (ID: 1)**: AI Partner Service (`domain: bank`)
- **`ml_service_account` (ID: 3)**: ML Service Account
- **`risk_scoring_service` (ID: 6)**: Risk Scoring Service (`domain: microfinance`, port `5007`)
- **`xvigilance` (ID: 999)**: xVigilance Daemon (`domain: bank`, port `5000`)

### 5.2 Two-Tier Schema Resolution Architecture
To eliminate hardcoded banking fallbacks for headless/service-account sessions, the worker resolves schemas using a two-tier hierarchy:

1. **Custom Overrides (Tier 1 - Highest Priority)**:
   If `service_accounts.config` defines explicit `"fetch_columns"` or `"column_mapping"`, these immediately override defaults.
2. **Domain-Profile Driven (Tier 2 - Automatic)**:
   When `domain` is specified (e.g. `"microfinance"`, `"banking"`, `"cdr"`), the worker automatically loads the matching domain profile:
   - **Microfinance (`domains/microfinance/profile.json`)**:
     - `date_column`: `"CREATEDDATE"`
     - `fetch_columns`: `["TRANSFERID", "CREATEDDATE", "TRANSFERAMOUNT", "SENDERACCOUNTID", "RECEIVERACCOUNTID", "SENDER_FULL_NAME", "RECEIVER_FULL_NAME", "NAME"]`
     - `column_mapping`:
       - `SENDERACCOUNTID -> ACCOUNTNO`
       - `RECEIVERACCOUNTID -> BENACCOUNTNO`
       - `TRANSFERAMOUNT -> AMOUNTINBIRR`
       - `CREATEDDATE -> TRANSACTIONDATE`
       - `TRANSFERID -> TRANSACTIONID`
   - **Banking (`domains/banking/profile.json`)**: Standard 13-column bank CTR schema.
   - **CDR (`domains/cdr/profile.json`)**: Telecom call data records schema.

### 5.3 Bidirectional Column Mapping in `elastic_utils.py`
When raw Elasticsearch documents are converted to DataFrames, the worker filters against `fetch_columns` using a bidirectional mapping check:
```python
if column_mapping:
    for src_col, tgt_col in column_mapping.items():
        if src_col.lower() in normalized_fetch or tgt_col.lower() in normalized_fetch:
            normalized_fetch.add(src_col.lower())
            normalized_fetch.add(tgt_col.lower())
```
This ensures that raw source columns (e.g. `senderaccountid`) are preserved through the initial filter and subsequently renamed to standard canonical names (`accountno`) for Neo4j graph generation and rule evaluation.

