# National-Scale Architecture Plan: xVigilance Detective Pipeline

This architecture relies on **Event-Driven Streaming**. `xvigilance` acts as the autonomous data broker, feeding the heavy lifting entirely to the governed Worker server. 

### Phase 1: Ingestion & Routing (The Kafka Firehose)
`xvigilance` operates entirely autonomously, reading the strict 1-hour sliding windows from Elasticsearch. 

1. **Micro-Batching:** It fetches up to 150,000 transactions per hour and loops through them rapidly.
2. **Kafka Streaming:** It pushes the transactions to the Worker’s standard input topic (e.g., `dev.scoring.score.calculated.v1`). 
3. **The Governance Badge:** Crucially, it stamps every payload with the badge `session_id = 'XVIGILANCE_FINDINGS'`.

*Why this is optimal:* It bypasses the REST API completely, prevents PostgreSQL from choking on raw data, and utilizes Kafka’s infinite streaming scalability.

---

### Phase 2: Governed Execution & Ingestion (The Worker Server)
The Worker server acts as the blind, highly-governed processing engine. It handles all database insertions exactly as it does for frontend REST API requests.

1. **Layer 1 Mapping (Ingestion):** The Worker consumes the Kafka topic and handles the heavy ingestion of inserting the raw transactions into the Neo4j "Detection Window" graph.
2. **Layer 2 & 3 Algorithms:** The Worker executes the official `LA_Script_rules` (e.g., Smurfing, Circular Flow) on the newly ingested graph nodes. 

*Why this is optimal:* We don't write rogue ingestion or analysis scripts. The governed Worker handles all graph mapping securely.

---

### Phase 3: Automatic Promotion (The Evidence Table)
Because `xvigilance` pushed the data with the `XVIGILANCE_FINDINGS` badge, the Worker's native promotion logic automatically handles the rest.

1. **Evidence Logging:** When the Worker detects an anomaly (e.g., a Smurfing ring), it automatically logs the result into the PostgreSQL `link_analysis_evidence` table.
2. **The Frontend Link:** Because the logged evidence carries the `session_id: 'XVIGILANCE_FINDINGS'`, the Admin Dashboard can instantly query this table to display the autonomous alerts to human analysts.

*Why this is optimal:* `xvigilance` doesn't have to poll or write back to PostgreSQL itself. The Worker natively promotes the evidence for us.

---

### Phase 4: Ephemeral Graph Hygiene (The Safe Clean-up)
To prevent graph bloat while maintaining a safe auditing buffer, we strictly delegate graph sweeping to the dedicated cleanup daemon.

1. **The Safety Buffer:** Innocent transactions are NOT deleted immediately. They remain in Neo4j for a safe retention period (determined by the cleanup service's default cleaning schedule) to allow for manual investigations, delayed historical context matching, and rule replays.
2. **Dedicated Sweeping:** The `linkx-xcleanup` daemon runs on its scheduled cadence. It executes Cypher queries to safely delete unflagged background transactions that have gracefully aged past the retention period, ensuring the graph remains performant without risking accidental data loss.

*Why this is optimal:* Separation of concerns. The Worker focuses entirely on catching fraud, while the Cleanup daemon ensures database health with a massive safety net for human auditors.

## The Ephemeral Graph Strategy (Single-Server Optimization)
Because the pipeline must process **1.4 Billion transactions** sequentially on a single Neo4j server, leaving all data in the graph would cause catastrophic Out-Of-Memory (OOM) crashes.

To achieve maximum throughput and safety:
1. The daemon loads exactly 1 chronological time window into Neo4j.
2. It executes all algorithmic anomaly rules (`LA_Script_rules`) in memory.
3. It securely exports all anomalous evidence and JSON reports to PostgreSQL.
4. **It immediately wipes Neo4j clean (`MATCH (n) DETACH DELETE n`)**.

**Note for Analysts:** If you check the Neo4j database directly, it will always appear mostly empty. This is intentional. Neo4j acts purely as a high-speed computational engine. All permanent, long-term graph evidence is safely stored in PostgreSQL (`linkx_reports` and `link_analysis_evidence`).
