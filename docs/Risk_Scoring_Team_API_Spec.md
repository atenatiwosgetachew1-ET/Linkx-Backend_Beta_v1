# LinkX Risk Scoring Sync API

A blocking, real-time HTTP endpoint that executes the full graph transaction analysis engine (querying Elasticsearch, building a dataframe, and analyzing in Neo4j) and returns the aggregated metrics and graph representation in a single response.

---

## Endpoint Configuration
**URL:** `POST http://172.27.23.95/api/risk_scoring/sync_analysis`  
*(In production, use your configured LinkX API gateway URL)*

**Headers:**
```text
Content-Type: application/json
Host: linkx-api.local
X-API-Key: <your_provided_api_key>
```
*(Alternatively, you can provide an `Authorization: Bearer <JWT>` header instead of the API key).*

---

## 1. Request Payload (Dynamic Searching)

The API supports **Dynamic Search Keys**. You do not have to strictly search by `account_no`. You can pass any supported Elasticsearch column as the key (e.g., `transactionID`, `businessmobileno`, `banname`).

| Field | Type | Required | Description |
|---|---|---|---|
| `<Search Key>` | string | ✅ Yes | The dynamic entity you want to search by (e.g., `"account_no": "100790..."` or `"transactionID": "ft2522..."`). |
| `response_type` | string | ❌ No | Determines graph output scope. Options: `"flagged"` (default) or `"full"`. |

### Example Request (By Account)
```json
{
  "account_no": "1007900232134",
  "response_type": "full"
}
```

### Example Request (By Transaction ID)
```json
{
  "transactionID": "ft25228zdhz6",
  "response_type": "full"
}
```

---

## 2. Response Payload

The API dynamically echoes back your search key in the response payload.

### Success (200 OK)

| Field | Type | Description |
|---|---|---|
| `success` | boolean | Indicates successful completion of the pipeline. |
| `<Search Key>` | string | The exact search key and value you requested. |
| `source` | string | The engine source (always `"link"`). |
| `processing.duration_ms` | float | Time taken by the server to compute the graph (in ms). |
| `data.<Search Key>` | string | The target entity value. |
| `data.linked_accounts_count` | integer | Total number of discrete nodes mapped in the transaction graph. |
| `data.all_relationships` | integer | Total number of transaction edges processed in the graph. |
| `data.max_path_length` | integer | The maximum hop distance of linked entities from the source. |
| `data.network_centrality_score` | float | Aggregated graph centrality score (0.0 to 1.0). Higher indicates higher risk/centrality. |
| `data.beneficiary_blacklisted` | boolean | True if any connected node is explicitly blacklisted. |
| `data.flagged_relationships` | array (string) | List of risky patterns detected (e.g. `["HUB_AND_SPOKE", "SMURFING"]`). Empty if clean. |
| `data.graph_entities` | object | The subgraph representation (Nodes & Edges). Behavior depends on `response_type`. |

#### How `graph_entities` works:
- If `response_type` is `"flagged"` (or omitted):
  - Returns **only** the nodes and edges that triggered a risk rule.
  - If no rules were triggered, returns an empty object: `{}`.
- If `response_type` is `"full"`:
  - Returns the **entire** transaction graph exactly as represented in the LinkX UI, including all standard graph metadata.

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
    "all_relationships": 11,
    "flagged_relationships": [],
    "graph_entities": {
      "nodes": [
        {
          "id": 1785,
          "label": "1785",
          "accountno": "1007900232134",
          "amountinbirr": 1700000.0,
          "transactiontype": "transfer",
          "node_identity": "Source Node",
          "linkx_managed": true
        }
      ],
      "edges": [
        {
          "from": 1785,
          "to": 1786,
          "label": "TRANSACTS_TO",
          "bgcolor": "#750b8c",
          "textcolor": "#ffffff",
          "weight": 1,
          "linkx_managed": true
        }
      ]
    },
    "max_path_length": 2,
    "network_centrality_score": 0.15
  },
  "processing": {
    "duration_ms": 15600.58
  }
}
```

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
*The API gateway enforces a hard 60-second limit. If the account graph is exceedingly large (e.g. thousands of transactions), the graph processing may exceed this limit and return a 504.*
```html
<html>
<head><title>504 Gateway Time-out</title></head>
...
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
