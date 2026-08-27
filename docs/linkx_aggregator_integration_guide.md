# LinkX Webhook Aggregation Integration Guide

**Version:** 1.0  
**Status:** Implemented & Tested  
**Scope:** Integration between LinkX Analysis Service and the Risk Decision Aggregator.

---

## 1. Architectural Overview

This integration utilizes a high-performance **Asynchronous Webhook Model**. 
To prevent long HTTP timeouts while waiting for complex graph analysis to complete, the interaction is split into two phases:

1. **The Trigger:** The Aggregator sends a batch of accounts to LinkX. LinkX accepts the batch, queues it in a background thread, and immediately returns a `202 Accepted` response.
2. **The Callback:** Once LinkX finishes analyzing an account in the background, it actively pushes the structured findings back to the Aggregator's webhook endpoint.

---

## 2. Authentication

LinkX utilizes a streamlined API Key authentication flow for this integration to reduce overhead.

* **Header Required:** `X-API-Key`
* **Security:** The API key is statically injected via the server's `.env` configuration (`LINKX_RISK_SCORING_API_KEY`).
* **Fallback:** The endpoint still inherently supports standard JWT Bearer tokens (`Authorization: Bearer <token>`) for legacy compatibility.

---

## 3. Phase 1: Triggering the Analysis

The Aggregator initiates the process by sending a batch of accounts to the LinkX analysis endpoint.

* **Endpoint:** `POST http://<linkx_api_ip>/api/risk_scoring/analysis_request`
* **Required Headers:**
  * `Host: linkx-api.local` *(Required by Nginx routing)*
  * `X-API-Key: <your_secure_api_key>`
  * `Content-Type: application/json`

### Request Payload Schema
```json
{
  "job_id": "job-12345678",
  "account_numbers": [
    "1000000001008",
    "ACC_TEST_002"
  ]
}
```

### Synchronous Response
LinkX validates the key and payload, pushes the job to a background thread, and immediately terminates the connection.

* **Status Code:** `202 Accepted`
```json
{
  "success": true,
  "message": "Analysis request accepted and queued for processing"
}
```

---

## 4. Phase 2: Webhook Callback

As LinkX finishes the analysis for *each individual account*, it fires a request back to the Aggregator.

* **Endpoint:** `POST https://risk-platform.local/api/v1/aggregate/callback`
* **Required Headers:**
  * `Content-Type: application/json`

### Callback Payload Schema
```json
{
  "job_id": "job-12345678",
  "schema_version": "1.0",
  "account_no": "1000000001008",
  "source": "link",
  "data": {
     "is_flagged": true,
     "linked_accounts_count": 388,
     "flagged_rules": ["HUB_AND_SPOKE"]
  },
  "processing": {
      "duration_ms": 85.3
  }
}
```
*(Note: LinkX will fire one distinct callback request for every account provided in the initial `account_numbers` array).*

---

## 5. Security & Network Considerations

### Internal Domain Mapping (SNI)
The Risk Aggregator strictly enforces Server Name Indication (SNI) validation for its webhook. It refuses connections made directly to its IP address.
To accommodate this without relying on external DNS, the LinkX API Server maps the Aggregator's IP to the required domain internally:
* **Host Mapping:** `172.27.23.46 -> risk-platform.local` *(Configured in `/etc/hosts`)*

### SSL Certificate Verification
Because the Aggregator utilizes internal, self-signed SSL certificates, the LinkX callback publisher explicitly bypasses strict SSL verification (`verify=False`) to ensure seamless delivery over the internal network.
