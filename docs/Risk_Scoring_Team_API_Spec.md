# LinkX Risk Scoring Sync API

A blocking, real-time HTTP endpoint that executes the full graph transaction analysis engine (querying Elasticsearch, building a dataframe, and analyzing in Neo4j) and returns the aggregated metrics and graph representation in a single response.

---

## Endpoint Configuration
**URL:** `POST http://172.27.23.95/api/risk_scoring/sync_analysis`  
*(In production, use your configured LinkX API gateway URL)*

**Headers:**
```
Content-Type: application/json
Host: linkx-api.local
X-API-Key: <your_provided_api_key>
```
*(Alternatively, you can provide an `Authorization: Bearer <JWT>` header instead of the API key).*

---

## 1. Request Payload

You can pass standard explicit properties, or simply pass the dynamic entity key directly (e.g. `{"transactionID": "123"}`).

| Field | Type | Required | Description |
|---|---|---|---|
| `entity_id` | string | ✅ Yes* | The ID to search for (e.g., account number, transaction ID). |
| `entity_type` | string | ❌ No | The type of entity (default: `"accountno"`). |
| `<dynamic_key>` | string | ✅ Yes* | *Instead of `entity_id` and `entity_type`, you can pass a dynamic key like `"account_no": "123"` or `"transactionID": "999"` directly.* |
| `response_type` | string | ❌ No | Determines graph output scope. Options: `"flagged"` (default) or `"full"`. |

### Example Requests

**Option A (Dynamic fallback — Recommended):**
```json
{
  "transactionID": "99999812",
  "response_type": "full"
}
```

**Option B (Explicit):**
```json
{
  "entity_id": "99999812",
  "entity_type": "transactionID",
  "response_type": "full"
}
```

---

## 2. Response Payload

### Success (200 OK)

| Field | Type | Description |
|---|---|---|
| `success` | boolean | Indicates successful completion of the pipeline. |
| `<dynamic_key>` | string | The requested key (e.g., `account_no` or `transactionID`) is echoed back. |
| `source` | string | The engine source (always `"link"`). |
| `processing.duration_ms` | float | Time taken by the server to compute the graph (in ms). |
| `data.linked_accounts_count` | integer | Total number of discrete accounts mapped in the transaction graph. |
| `data.all_relationships` | integer | Total number of transaction edges processed in the graph. |
| `data.max_path_length` | integer | The maximum hop distance of linked accounts from the source. |
| `data.network_centrality_score` | float | Aggregated graph centrality score (0.0 to 1.0). Higher indicates higher risk/centrality. |
| `data.beneficiary_blacklisted` | boolean | True if any connected node is explicitly blacklisted. |
| `data.flagged_relationships` | array (string) | List of risky patterns detected (e.g. `["HUB_AND_SPOKE", "SMURFING"]`). Empty if clean. |
| `data.graph_entities` | object | The subgraph representation (Nodes & Edges). Behavior depends on `response_type`. |

#### How `graph_entities` works:
- If `response_type` is `"flagged"` (or omitted):
  - Returns **only** the nodes and edges that triggered a risk rule.
  - If no rules were triggered, returns an empty object: `{}`.
- If `response_type` is `"full"`:
  - Returns the **entire** transaction graph (both clean and flagged nodes/edges).

---

## 3. Error Responses

**Validation Error (400 Bad Request)**
```json
{
  "success": false,
  "message": "Missing search entity (e.g. account_no or entity_id)"
}
```

**Timeout (504 Gateway Timeout)**
*The LinkX worker queue may be busy. The endpoint waits a maximum of 120 seconds before timing out.*
```json
{
  "success": false,
  "transactionID": "99999812",
  "message": "Analysis timed out after 120s. The worker may still be processing."
}
```
