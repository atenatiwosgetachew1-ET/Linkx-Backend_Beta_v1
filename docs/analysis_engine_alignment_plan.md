# LinkX Analysis Engine Alignment & Hardening Plan

> **Purpose:** Fix all inconsistencies between the Manual Analysis engine (`LA_rules_script.py`) and the xVigilance autonomous engine (`xvigilance_consumer.py`), ensuring identical anomaly detection behavior across both systems.
>
> **Constraint:** Raw transaction nodes must never be deleted or distorted. The whitelist remains a suppression mechanism, not a data-deletion mechanism.
>
> **Reference:** This plan is based on the findings documented in the [xVigilance Analysis Deep Review](file:///home/linkx/.gemini/antigravity-cli/brain/b1a276e2-58c0-4f22-b3f6-9220cd40ffd3/xvigilance_analysis_deep_review.md).

---

## Phase 1: Unify the Trusted Entity Matching Logic

**Problem:** The `_trusted_entry_match()` function differs between the two engines. The Manual Analysis version has a `category/type/reason` key escape clause that the xVigilance version lacks. This means the same trusted entity list can produce different filtering behavior depending on which engine processes it.

**Files to modify:**
- `service_factory/services/linkx-worker/src/linkx_worker/xvigilance_consumer.py` (lines 431-435)

**Changes:**
1. Update `_trusted_entry_match()` in `xvigilance_consumer.py` to match the Manual Analysis version exactly:
   ```python
   def _trusted_entry_match(alias):
       return f'all(k IN keys(entry) WHERE toLower(k) IN [\'category\', \'type\', \'reason\'] OR toString(coalesce({alias}[k], "")) = toString(entry[k]))'
   ```
2. Add the missing `_trusted_pair_clause()` helper function to `xvigilance_consumer.py`:
   ```python
   def _trusted_pair_clause(left_alias, right_alias):
       return (
           "NOT any(entry IN $trusted_entries WHERE "
           f"({_trusted_entry_match(left_alias)} OR {_trusted_entry_match(right_alias)}))"
       )
   ```

**Verification:** After this change, passing the same `trusted_entries` array to both engines must produce identical Cypher WHERE clauses.

---

## Phase 2: Add Missing Rules to xVigilance

**Problem:** The xVigilance daemon only runs 8 of the 14 anomaly rules. It is missing: `LATE_NIGHT_TX`, `JUST_BELOW_THRESHOLD`, `RAPID_WITHDRAWAL`, `ACCOUNT_ACTIVITY_SPIKE`, `HIGH_RISK_LINK`, and `PEP_INVOLVED` / `SANCTIONED_ENTITY_MATCH`.

**File to modify:**
- `service_factory/services/linkx-worker/src/linkx_worker/xvigilance_consumer.py` — inside `run_full_graph_analysis()`

**Changes:**
Add the following 6 rule blocks to `run_full_graph_analysis()`, each wrapped in its own `try/except` block (matching the existing error-isolation pattern), and using the same Cypher logic as `LA_rules_script.py`:

### 2a. LATE_NIGHT_TX
- Copy the Cypher from `LA_rules_script.py` lines 390-402
- Apply `_trusted_node_clause('t')`
- Add `edge_semantic`, `financial_flow`, `directed_display` properties to match xVigilance's metadata convention

### 2b. JUST_BELOW_THRESHOLD
- Copy the Cypher from `LA_rules_script.py` lines 407-420
- Apply `_trusted_node_clause('t')`
- Use the same amount fallback: `coalesce(toFloat(t.AMOUNT), toFloat(t.amount), toFloat(t.LOCAL_AMOUNT), 0.0)`
- Pass `single_tx_threshold` parameter (default 10000)
- Add edge metadata properties

### 2c. RAPID_WITHDRAWAL
- Copy the Cypher from `LA_rules_script.py` lines 425-445
- Apply `_trusted_pair_clause('a', 'b')` (available after Phase 1)
- Use the index-assisted pattern (match on `BENACCOUNTNO = ACCOUNTNO`) consistent with the optimized xVigilance style
- Add edge metadata properties

### 2d. ACCOUNT_ACTIVITY_SPIKE
- Copy the Cypher from `LA_rules_script.py` lines 450-471
- Apply `_trusted_node_clause('t')`
- Add edge metadata properties

### 2e. HIGH_RISK_LINK
- Copy the Cypher from `LA_rules_script.py` lines 225-237
- This requires the `risk_entries` parameter (already fetched but unused by xVigilance rules)
- Add edge metadata properties

### 2f. PEP_INVOLVED / SANCTIONED_ENTITY_MATCH
- Copy the Cypher from `LA_rules_script.py` lines 239-259
- Uses `risk_entries` with the `category/type` dispatch logic (PEP → PEP_INVOLVED, SANCTION → SANCTIONED_ENTITY_MATCH, else → HIGH_RISK_LINK)
- Add edge metadata properties

**Verification:** After this phase, `rules_completed` list should report all 14 rule names (8 existing + 6 new) on a successful run.

---

## Phase 3: Fix SHARED_IDENTIFIER Trusted Filtering Inconsistency

**Problem:** The xVigilance `SHARED_IDENTIFIER` rule does not apply any trusted-entity filtering, while the Manual Analysis version applies `_trusted_pair_clause('a', 'b')`.

**File to modify:**
- `service_factory/services/linkx-worker/src/linkx_worker/xvigilance_consumer.py` — SHARED_IDENTIFIER rule block (lines 676-707)

**Change:**
Add a `WHERE` clause after the `UNWIND range(...)` step, before the `MERGE`:
```cypher
WITH txns[i] AS a, txns[i+1] AS b, identifier_type, identifier_value, accounts
WHERE NOT any(entry IN $trusted_entries WHERE ...)
MERGE (a)-[r:SHARED_IDENTIFIER ...]->(b)
```

This aligns the behavior with the Manual Analysis version.

**Verification:** After this change, whitelisted entities (like Feres Wallet Kaafi) should no longer trigger SHARED_IDENTIFIER alerts in xVigilance.

---

## Phase 4: Fix FUND_FLOW Branching Bug (Both Engines)

**Problem:** Both engines use `collect(b)[0]` or `LIMIT 1` to select only the earliest downstream transaction. If money fans out from account B to accounts C, D, and E, only one path is preserved. The other branches are silently dropped.

**Files to modify:**
- `service_factory/services/linkx-api/src/batch_manager/analyzing/LA_rules_script.py` — batch FUND_FLOW rule (lines 182-205) and incremental FUND_FLOW rule (lines 571-593)
- `service_factory/services/linkx-worker/src/batch_manager/analyzing/LA_rules_script.py` — same locations (worker copy)
- `service_factory/services/linkx-worker/src/linkx_worker/xvigilance_consumer.py` — FUND_FLOW rule (lines 524-561)

**Changes:**

### 4a. Manual Analysis — Batch FUND_FLOW
Remove `collect(b)[0]` and replace with direct MERGE, adding a guard to prevent explosive cartesian products:
```cypher
MATCH (a:{label}), (b:{label})
WHERE ...
  AND a.BENACCOUNTNO = b.ACCOUNTNO
  ...temporal ordering...
  AND {_trusted_pair_clause('a', 'b')}
WITH a, b
ORDER BY b.TRANSACTIONDATE ASC, b.TRANSACTIONTIME ASC
WITH a, collect(b) AS downstream
WITH a, downstream[..5] AS limited_downstream   // Cap at 5 branches per upstream node
UNWIND limited_downstream AS b
MERGE (a)-[r:FUND_FLOW {session_id:$session_id}]->(b)
SET r.bgcolor = '#d8a822', ...
```

The `[..5]` slice prevents runaway cartesian products while preserving multi-branch visibility. The number 5 is a reasonable default that captures the primary fan-out without exploding on super-nodes.

### 4b. xVigilance — FUND_FLOW
Apply the same fix inside the `CALL () {}` subquery block, replacing `LIMIT 1` with a slice-based cap.

### 4c. Manual Analysis — Incremental FUND_FLOW
The incremental version (lines 571-593) does not use `collect(b)[0]` — it creates relationships for ALL matching pairs. However, this means it can produce cartesian explosions. Add the same `[..5]` downstream cap for safety.

**Verification:** After a test run, FUND_FLOW relationships should show multiple branches when money fans out, up to 5 per upstream transaction.

---

## Phase 5: Add Edge Metadata to Manual Analysis Rules

**Problem:** The xVigilance engine stamps every relationship with `edge_semantic`, `financial_flow`, and `directed_display` properties. The Manual Analysis engine does not. This causes visual inconsistencies on the frontend graph.

**File to modify:**
- `service_factory/services/linkx-api/src/batch_manager/analyzing/LA_rules_script.py` — all rule MERGE/SET blocks in both `batch_graph_analysis_transactions()` and `incremental_graph_analysis_transactions()`
- `service_factory/services/linkx-worker/src/batch_manager/analyzing/LA_rules_script.py` — same (worker copy)

**Changes:**
Add three properties to every `SET` clause across all rules:

| Rule | `edge_semantic` | `financial_flow` | `directed_display` |
|------|-----------------|-------------------|---------------------|
| SMURFING | `'TEMPORAL_SEQUENCE'` | `false` | `true` |
| CIRCULAR_FLOW | `'OBSERVED_FLOW'` | `true` | `true` |
| FUND_FLOW | `'TEMPORAL_SEQUENCE'` | `false` | `true` |
| DORMANT_TO_ACTIVE | `'NODE_FLAG'` | `false` | `false` |
| ABNORMAL_BALANCE_CHANGE | `'TEMPORAL_SEQUENCE'` | `false` | `true` |
| HUB_AND_SPOKE | `'GROUPING'` | `false` | `false` |
| SHARED_IDENTIFIER | `'GROUPING'` | `false` | `false` |
| HIGH_RISK_LINK | `'NODE_FLAG'` | `false` | `false` |
| PEP_INVOLVED | `'NODE_FLAG'` | `false` | `false` |
| SANCTIONED_ENTITY_MATCH | `'NODE_FLAG'` | `false` | `false` |
| LATE_NIGHT_TX | `'NODE_FLAG'` | `false` | `false` |
| JUST_BELOW_THRESHOLD | `'NODE_FLAG'` | `false` | `false` |
| RAPID_WITHDRAWAL | `'OBSERVED_FLOW'` | `true` | `true` |
| ACCOUNT_ACTIVITY_SPIKE | `'NODE_FLAG'` | `false` | `false` |

**Verification:** After this change, every anomaly relationship created by either engine will carry the same three metadata properties, and the frontend will render them identically regardless of which engine created them.

---

## Phase 6: Standardize Amount Field Fallback Chain

**Problem:** Different rules use different amount field fallback chains. SMURFING uses `AMOUNTINBIRR → AMOUNT → amount`, while JUST_BELOW_THRESHOLD and RAPID_WITHDRAWAL use `AMOUNT → amount → LOCAL_AMOUNT`.

**Files to modify:**
- `service_factory/services/linkx-api/src/batch_manager/analyzing/LA_rules_script.py`
- `service_factory/services/linkx-worker/src/batch_manager/analyzing/LA_rules_script.py`
- `service_factory/services/linkx-worker/src/linkx_worker/xvigilance_consumer.py`

**Change:**
Define a single canonical amount expression and use it everywhere:
```cypher
coalesce(toFloat(t.AMOUNTINBIRR), toFloat(t.AMOUNT), toFloat(t.amount), toFloat(t.LOCAL_AMOUNT), 0.0)
```

Apply this expression to:
- SMURFING (already close, just add `LOCAL_AMOUNT` fallback)
- JUST_BELOW_THRESHOLD (add `AMOUNTINBIRR` as first priority)
- RAPID_WITHDRAWAL (add `AMOUNTINBIRR` as first priority)
- `calculate_fraud_score()` (already uses a Python-side fallback, align order)

**Verification:** All rules use the identical 4-level fallback chain. The first non-null value wins.

---

## Phase 7: Align xVigilance CIRCULAR_FLOW and SMURFING Trusted Filtering Style

**Problem:** In xVigilance, SMURFING and CIRCULAR_FLOW apply `_trusted_node_clause` (single-node filter) in the initial `MATCH`. In Manual Analysis, they use `_trusted_pair_clause` (pair filter) after the `UNWIND`. The pair-clause approach is more precise because it checks both sides of the relationship.

**File to modify:**
- `service_factory/services/linkx-worker/src/linkx_worker/xvigilance_consumer.py`

**Changes:**

### 7a. SMURFING
Move the trusted filter from the initial `MATCH` to after the `UNWIND`:
```cypher
-- Remove from initial MATCH:
--   AND NOT any(entry IN $trusted_entries WHERE ...)

-- Add after UNWIND:
WITH txns[i] AS a, txns[i+1] AS b, acc, beneficiary, tx_day, tx_count, total_amount
WHERE NOT any(entry IN $trusted_entries WHERE
  (all(k IN keys(entry) WHERE ...) OR all(k IN keys(entry) WHERE ...)))
MERGE (a)-[r:SMURFING ...]->(b)
```

### 7b. CIRCULAR_FLOW
The xVigilance version only checks `_trusted_node_clause('a')`. Update to check both sides, matching the Manual Analysis `_trusted_pair_clause('a', 'b')` behavior.

**Verification:** Both engines now filter trusted entities at the same point in the query pipeline, producing identical results.

---

## Phase 8: Deployment & Verification

**Deployment sequence (strict order):**

1. **Push updated code to the repository** from this development server
2. **Deploy to Server 3 (node-21)** — Worker copy of `LA_rules_script.py`
   ```bash
   # SSH to node-21
   sudo cp /opt/linkx-backend-update/service_factory/services/linkx-worker/src/batch_manager/analyzing/LA_rules_script.py /opt/linkx-worker/src/batch_manager/analyzing/LA_rules_script.py
   sudo systemctl restart linkx-worker
   ```
3. **Deploy to Server 1 (node-19)** — API copy of `LA_rules_script.py`
   ```bash
   # SSH to node-19
   sudo cp /opt/linkx-backend-update/service_factory/services/linkx-api/src/batch_manager/analyzing/LA_rules_script.py /opt/linkx-backend-api/src/batch_manager/analyzing/LA_rules_script.py
   sudo systemctl restart linkx-api
   ```
4. **Deploy to Server 4 (node-22)** — xVigilance consumer
   ```bash
   # SSH to node-22
   sudo cp /opt/linkx-backend-update/service_factory/services/linkx-worker/src/linkx_worker/xvigilance_consumer.py /opt/Linkx_xmaintenance/src/linkx_worker/xvigilance_consumer.py
   sudo systemctl restart linkx-xvigilance-consumer
   ```

**Post-deployment verification:**

1. **xVigilance rule count:** Check `journalctl -u linkx-xvigilance-consumer` for `Analysis summary: 14 passed` (previously 8)
2. **Manual Analysis parity:** Run a manual analysis session on a known dataset and compare the flagged relationship types against xVigilance findings for the same time window
3. **FUND_FLOW branching:** Query Neo4j for FUND_FLOW relationships and verify that accounts with multiple downstream transfers show multiple outgoing FUND_FLOW edges (up to 5)
4. **Edge metadata:** Query `MATCH ()-[r]->() WHERE r.session_id = '...' RETURN r.edge_semantic, r.financial_flow, r.directed_display LIMIT 10` and verify all three properties are populated
5. **Trusted entity consistency:** Confirm that whitelisted accounts (e.g., `338766`) no longer appear in any anomaly relationship from either engine

---

## Risk Assessment

| Phase | Risk Level | Rollback Strategy |
|-------|-----------|-------------------|
| Phase 1 (Trusted match unification) | **Low** | Revert the single function. No data impact. |
| Phase 2 (Add 6 missing rules) | **Low** | New rules are additive. Remove the try/except blocks to revert. |
| Phase 3 (SHARED_IDENTIFIER fix) | **Low** | Remove the WHERE clause to revert. |
| Phase 4 (FUND_FLOW branching) | **Medium** | The `[..5]` cap prevents explosions, but monitor Neo4j memory on first run. If relationships explode, reduce cap to `[..3]` or revert to `LIMIT 1`. |
| Phase 5 (Edge metadata) | **Low** | Adding properties to relationships is non-destructive. |
| Phase 6 (Amount standardization) | **Low** | Fallback chain is backwards-compatible. |
| Phase 7 (Trusted filter position) | **Medium** | Moving the filter changes which transactions get flagged. Expect more flags on partially-trusted pairs initially. |
| Phase 8 (Deployment) | **Low** | Standard service restart. Keep old files as `.bak` before overwriting. |

---

## IMPLEMENTATION COMPLETE (September 2026)

All alignment phases have been completed. Furthermore, the core **intermediary blind-spot problem** identified during the review has been explicitly resolved by implementing the **EFFECTIVE_FLOW** pass-through rule as Rule 0 in both engines.

**Key Achievements:**
1. Both engines now share identical trusted entity filtering code and amount standardization logic.
2. A new `_extract_pass_through_accounts` helper exists in both engines.
3. The `EFFECTIVE_FLOW` rule automatically detects when funds route through an entity marked `pass_through: true` and connects the actual sender and receiver directly.
4. Downstream rules (SMURFING, FUND_FLOW, etc.) successfully evaluate these effective-flow paths without requiring modifications to the core rules themselves.
5. The `EFFECTIVE_FLOW` anomaly is officially scored with a base score of 25.
6. 46 core pass-through entities (Wallets, Banks, Telecoms, Agents) have been officially seeded in the production database.
