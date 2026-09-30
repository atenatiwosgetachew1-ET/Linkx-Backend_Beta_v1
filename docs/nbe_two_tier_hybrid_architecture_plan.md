# National Bank of Ethiopia (NBE) Fraud Detection Platform
## Two-Tier Hybrid Architecture (Decoupled Compute & Knowledge Graph) Implementation Plan

**Target System:** LinkX National Financial Fraud & Anti-Money Laundering Platform  
**Target Scale:** 500,000 to 5,000,000+ transactions per ingestion window  
**Author:** Google Deepmind Advanced Agentic Engineering Team  
**Date:** September 2026  
**Status:** Approved for Phased Implementation  

---

## 1. Executive Summary & Architectural Vision

The National Bank of Ethiopia (NBE) monitoring mandate requires real-time and scheduled batch surveillance of all transactions traversing the national payment rails (commercial banks, EthSwitch, Telebirr, CBE Birr). 

### The Core Problem
Under the legacy architecture, the platform loaded raw transaction nodes into Neo4j and then used complex Cypher queries with Cartesian-like self-joins (`MATCH (a), (b) WHERE a.account = b.benaccount ...`) to evaluate tabular fraud rules. On realistic national transaction volumes (e.g., **468,447 records** in a single window), Cypher's query planner is forced into billions of comparisons, stalling rules like `CIRCULAR_FLOW` for 30+ minutes and threatening pipeline SLAs.

### The Solution: Two-Tier Hybrid Architecture
In alignment with world-class financial intelligence engines (Palantir Foundry, Quantexa, Feedzai):
- **Tier 1 (High-Speed In-Memory Vector Engine):** Compute tabular, mathematical, and temporal pairwise logic in memory (Python / Pandas / NumPy / Polars) in linear time ($O(N)$). Benign data (99.9% of transactions) is filtered out in milliseconds.
- **Tier 2 (Enterprise Knowledge Graph - Neo4j + GDS):** Neo4j is relieved of flat join operations and utilized exclusively for what graph databases excel at: deep multi-hop traversal ($A \to B \to C \to D$), Louvain community clustering, PageRank centrality, and rendering investigative evidence graphs for human analysts.

```
       National Transaction Streams (Kafka / ES / Uploads)
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  TIER 1: In-Memory Vector Compute Engine (Python/Pandas)    │
│  • Effective Flow & Intermediary Pass-through Resolution    │
│  • Circular Flow Hash Matching (O(1) lookups)               │
│  • Smurfing & Aggregation Thresholding                      │
│  • Rapid Withdrawal & Velocity Windowing                    │
│  Execution Time: < 2 seconds for 500,000 records            │
└─────────────────────────────┬───────────────────────────────┘
                              │
              Selective Materialization of Anomaly Edges
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  TIER 2: Enterprise Knowledge Graph (Neo4j + GDS on Node-22)│
│  • Stores Ingested Transactions & Entity Master Data        │
│  • Materializes Evidence Edges (:CIRCULAR_FLOW, etc.)       │
│  • Executes Graph Data Science Algorithms (471 procedures): │
│    - Weakly Connected Components (WCC)                      │
│    - PageRank Centrality & Node Degree Distribution         │
│    - Betweenness & Eigenvector Risk Propagation             │
│  • Serves Visual Investigation Canvas for NBE Analysts     │
└─────────────────────────────────────────────────────────────┘
```

---

## 2. Top-Layer Implementation Phases

The rollout is divided into five rigorous, testable phases designed to maintain zero downtime and ensure zero regressions across both automated (`xVigilance`) and manual (`analyzer.py`) workflows:

* **PHASE 1: Pilot Modernization — Vectorized `CIRCULAR_FLOW` Engine**
* **PHASE 2: High-Velocity Rules Modernization (`RAPID_WITHDRAWAL` & `SMURFING`)**
* **PHASE 3: xVigilance Autonomous Daemon Integration & Live Stream Verification**
* **PHASE 4: Manual Analyst Flow Modernization & Unified Engine Convergence**
* **PHASE 5: National-Scale Hardening, GDS Centrality & Benchmark Stress-Testing**

---

## 3. Phase-by-Phase Detailed Roadmap

### Phase 1: Pilot Modernization — Vectorized `CIRCULAR_FLOW` Engine
**Objective:** Replace the Cypher bottleneck that currently stalls at 468k records with an in-memory hash matcher that runs in < 500 milliseconds.

#### 1.1 In-Memory Mathematical Design
- **Key Hash Set:** Build a set of active legs $S = \{(A, B, \text{Date})\}$ from the normalized DataFrame.
- **Reverse Lookup:** Query $S$ for $(B, A, \text{Date})$ in $O(1)$ time.
- **Financial Validation:**
  - Enforce amount tolerance: $|amt_A - amt_B| \le \min(amt_A, amt_B) \times \text{tolerance}$ (retrieved dynamically from PostgreSQL `global_rule_thresholds`).
  - Filter out pass-through entities (`$pt`) and trusted counterparties (`$trusted_entries`).
- **Deduplication:** Sort endpoints so $(A, B)$ and $(B, A)$ generate a single canonical pair.

#### 1.2 Targeted Neo4j Edge Materialization
Instead of running a giant Cartesian `MATCH (a), (b)` across all 468k nodes, Python passes only the verified anomaly node ID pairs into a lightweight parameterized Cypher query:
```cypher
UNWIND $anomaly_pairs AS pair
MATCH (a) WHERE elementId(a) = pair.id_a
MATCH (b) WHERE elementId(b) = pair.id_b
MERGE (a)-[r1:CIRCULAR_FLOW {session_id: $session_id}]->(b)
SET r1 += pair.props_a
MERGE (b)-[r2:CIRCULAR_FLOW {session_id: $session_id}]->(a)
SET r2 += pair.props_b
```

#### 1.3 Acceptance & Verification Criteria
1. Verification against the identical 468,447 record batch.
2. Runtime drops from $> 30\text{ minutes}$ to $< 2\text{ seconds}$.
3. Zero false positives or false negatives compared to the mathematical definition.
4. Edge attributes (`anomaly_score`, `financial_flow`, `logical_sender`, `reason`) match existing UI contracts identically.

---

### Phase 2: High-Velocity Rules Modernization (`RAPID_WITHDRAWAL` & `SMURFING`)
**Objective:** Eliminate secondary Cypher bottlenecks for rules involving time-window sequence checks and aggregation counts.

#### 2.1 Vectorized `RAPID_WITHDRAWAL`
- **Logic:** Account $A$ receives funds (as beneficiary) and subsequently disburses funds (as sender) within a rolling window $\Delta t$ with amount matching within tolerance.
- **Vector Algorithm:** Group DataFrame by `ACCOUNTNO`, sort chronologically by `(TRANSACTIONDATE, TRANSACTIONTIME)`, and apply a vectorized forward rolling window (`pandas.merge_asof` or NumPy searchsorted).
- **Speedup:** From $O(N^2)$ Cypher scans down to $O(N \log N)$ sorting in memory (< 1s for 500k rows).

#### 2.2 Vectorized `SMURFING`
- **Logic:** Account initiates $\ge K$ transactions just below reporting thresholds ($< 300,000$ ETB) whose cumulative sum exceeds the structuring ceiling within the evaluation window.
- **Vector Algorithm:** Single-pass Pandas groupby aggregation (`count`, `sum`) with condition masks. Instantaneous ($< 100\text{ms}$).

#### 2.3 Acceptance Criteria
1. Full parity of detected smurfing accounts with historical audit benchmarks.
2. Complete parameterization with PostgreSQL `global_rule_thresholds`.

---

### Phase 3: xVigilance Autonomous Daemon Integration & Live Stream Verification
**Objective:** Deploy the modernized Tier-1 compute layer directly into `xvigilance_consumer.py` on `node-21`.

#### 3.1 Integration Architecture
- Extract the vectorized rules into a shared module: `service_factory/services/linkx-worker/src/batch_manager/analyzing/vector_rules.py`.
- In `xvigilance_consumer.py`, call the in-memory screening functions immediately following `EFFECTIVE_FLOW`.
- Maintain strict transaction-level error isolation (`try...except` per rule) so that a failure in one rule never interrupts continuous streaming.

#### 3.2 Live Verification on Node-21
- Re-run the consumer against the 468,447 backfill window.
- Monitor log output via `journalctl -u linkx-xvigilance-consumer -f`.
- **Target Metric:** Full 16-rule batch analysis suite on 468,447 records completes in **under 20 seconds** total (down from over 1 hour).

---

### Phase 4: Manual Analyst Flow Modernization & Unified Engine Convergence
**Objective:** Eliminate code duplication by having the manual investigator flow in `analyzer.py` use the exact same Tier-1 vector engine.

#### 4.1 Convergence in `analyzer.py`
- When an analyst loads an investigation from an uploaded Excel/CSV file or fetches records from Elasticsearch, `analyzer.py` normalizes the DataFrame and calls `vector_rules.py`.
- The manual analysis canvas benefits from the exact same sub-second execution speed.
- The UI retains 100% backward compatibility: edge styles, colors, badges, and node properties remain completely unchanged.

#### 4.2 Single Source of Truth
- Eliminate legacy duplicate monolithic Cypher scripts (`batch_graph_analysis_transactions`).
- Dynamic threshold changes made in the Admin Settings UI instantly affect both xVigilance and manual investigations simultaneously.

---

### Phase 5: National-Scale Hardening, GDS Centrality & Benchmark Stress-Testing
**Objective:** Prepare the system for 24/7 sovereign operation at national volume (up to 5 million transactions per day).

#### 5.1 Neo4j Graph Data Science (GDS) Tuning on Node-22
- Configure in-memory graph projections in Neo4j GDS to run exclusively on the **detected evidence subgraph** rather than projecting the entire unflagged database.
- Automate Louvain community detection and PageRank on the anomalous component clusters to identify ringleaders and high-value mule accounts.

#### 5.2 System Stress Testing & Verification Protocol
- Run a 1,000,000 transaction synthetic national clearing load test through Kafka.
- Verify memory stability on `node-21` (Python worker) and `node-22` (Neo4j container).
- Verify that Ephemeral Graph Wipes and audit logging persist accurately without database locking or memory leaks.

---

## 4. Safety Constraints & Verification Matrix

Every step in this implementation plan strictly adheres to the **2x/3x Rule**:
- **Design Review ($\ge 2\times$):** Mathematical logic and edge semantics verified twice before code generation.
- **Verification ($\ge 3\times$):** 
  1. Synthetic edge-case unit test (boundary conditions, null values, reverse timestamps).
  2. Batch equivalence replay against historical 468k dataset.
  3. Live end-to-end trace from Kafka message to Risk Scoring and Postgres Alert records.
- **Rollback Guarantee:** All legacy Cypher query generators remain available as fallbacks until Phase 5 acceptance is officially signed off.
