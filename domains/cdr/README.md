# CDR Domain (Call Data Records)

**Cluster**: Telecom Intelligence  
**Subnet**: `172.21.13.0/24`  

## Files

| File | Purpose |
| :--- | :--- |
| `profile.json` | Column mappings, storage config, rule definitions |
| `seed.sql` | PostgreSQL seed for `user_configs` with CDR telecom defaults |

## Active Rule Module

The CDR Cypher rules are implemented in:
- **Built-in**: `batch_manager/analyzing/LA_rules_script.py` → `CDR_RELATIONSHIPS` (lines 2091–2418)
- **Uploadable**: `public/temp_rules/call_data_records_rules.py`

## Relationships (14 typologies)

`CALL_SEQUENCE`, `CALLBACK_PATTERN`, `FREQUENT_CONTACT`, `SHORT_DURATION_BURST`,
`LONG_DURATION_CALL`, `MISSED_CALL_SIGNAL`, `CALL_RELAY`, `STAR_PATTERN`,
`LOCATION_JUMP`, `NIGHT_ACTIVITY`, `HIGH_RISK_CONTACT`, `FAN_OUT`, `FAN_IN`,
`SIMULTANEOUS_CALL`

## Deploying to a New CDR Cluster

1. Provision a 4-server cluster per `docs/four_server_cluster_deployment_and_replication_handoff.md`.
2. Set `LINKX_DOMAIN=cdr` in all `.env` files.
3. Apply seed: `sudo bash service_factory/deploy/security/apply-cdr-session-config.sh`
4. Verify with `verify-linkx-server.py` on all 4 nodes.
