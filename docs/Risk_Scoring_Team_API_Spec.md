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

| Field | Type | Required | Description |
|---|---|---|---|
| `account_no` | string | ✅ Yes | The bank account number to run link analysis on. |
| `response_type` | string | ❌ No | Determines graph output scope. Options: `"flagged"` (default) or `"full"`. |

### Example Request
```json
{
  "account_no": "1007900232134",
  "response_type": "full"
}
```

---

## 2. Response Payload

### Success (200 OK)

| Field | Type | Description |
|---|---|---|
| `success` | boolean | Indicates successful completion of the pipeline. |
| `account_no` | string | The account number that was analyzed. |
| `source` | string | The engine source (always `"link"`). |
| `processing.duration_ms` | float | Time taken by the server to compute the graph (in ms). |
| `data.accountno` | string | The target account number. |
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

### Example Response (`response_type="full"`)
```json
{
  "success": true,
  "account_no": "1007900232134",
  "source": "link",
  "data": {
    "accountno": "1007900232134",
    "entity_id": "1007900232134",
    "beneficiary_blacklisted": false,
    "linked_accounts_count": 12,
    "all_relationships": 24,
    "flagged_relationships": ["SMURFING"],
    "graph_entities": {
      "nodes": [
        {
          "id": "5210",
          "ACCOUNTNO": "ACC10030",
          "ACCOUNTSTATE": "dormant",
          "label": "ACC10030"
        }
      ],
      "edges": [
        {
          "id": "5210_5406",
          "from": "5210",
          "to": "5406",
          "label": "SMURFING",
          "bgcolor": "#d8a822",
          "reason": "beneficiary later acts as sender",
          "width": 1
        }
      ]
    },
    "max_path_length": 2,
    "network_centrality_score": 0.15
  },
  "processing": {
    "duration_ms": 12163.89
  }
}
```

---

## 3. Error Responses

**Validation Error (400 Bad Request)**
```json
{
  "success": false,
  "message": "Missing account_no"
}
```

**Timeout (504 Gateway Timeout)**
*The LinkX worker queue may be busy. The endpoint waits a maximum of 120 seconds before timing out.*
```json
{
  "success": false,
  "account_no": "1007900232134",
  "message": "Analysis timed out after 120s. The worker may still be processing."
}
```

**Analysis Failure (500 Internal Server Error)**
```json
{
  "success": false,
  "account_no": "1007900232134",
  "message": "Analysis failed",
  "error": "neo4j_ingestion_failed"
}
```
