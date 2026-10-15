# Banking Domain (BDR — Bank Data Records)

**Cluster**: NBE (National Bank of Ethiopia)  
**Subnet**: `172.27.23.0/24`  

## Files

| File | Purpose |
| :--- | :--- |
| `profile.json` | Column mappings, storage config, rule definitions |
| `seed.sql` | PostgreSQL seed for `user_configs` with banking defaults |

## Active Rule Module

The banking Cypher rules are implemented in:
- **Built-in**: `batch_manager/analyzing/LA_rules_script.py` → `TRANSACTION_RELATIONSHIPS` (lines 202–1918)
- **Uploadable**: `public/temp_rules/bank_transactions_rules.py`

## Relationships (15 typologies)

`EFFECTIVE_FLOW`, `SMURFING`, `CIRCULAR_FLOW`, `FUND_FLOW`, `DORMANT_TO_ACTIVE`,
`HIGH_RISK_LINK`, `ABNORMAL_BALANCE_CHANGE`, `HUB_AND_SPOKE`, `SHARED_IDENTIFIER`,
`PEP_INVOLVED`, `SANCTIONED_ENTITY_MATCH`, `LATE_NIGHT_TX`, `JUST_BELOW_THRESHOLD`,
`RAPID_WITHDRAWAL`, `ACCOUNT_ACTIVITY_SPIKE`

## Deploying to a New Banking Cluster

1. Provision a 4-server cluster per `docs/four_server_cluster_deployment_and_replication_handoff.md`.
2. Set `LINKX_DOMAIN=banking` in all `.env` files.
3. Apply seed: `sudo bash service_factory/deploy/security/apply-session-config.sh domains/banking/seed.sql`
4. Verify with `verify-linkx-server.py` on all 4 nodes.
