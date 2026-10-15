# LinkX Domain Onboarding Playbook

**Purpose**: Step-by-step guide for adding a new analytical domain to LinkX.  
**Last Updated**: October 2026  

---

## Prerequisites

- A running LinkX 4-server cluster (or a fresh cluster to be provisioned).
- Access to the source repository: `https://github.com/atenatiwosgetachew1-ET/Linkx-Backend_Beta_v1.git`
- Understanding of the domain's data schema (column names, relationships, data sources).

---

## Step 1: Create the Domain Profile

Copy the template directory:

```bash
cp -r domains/_template domains/<your_domain>
```

Rename the template files:

```bash
cd domains/<your_domain>
mv profile.json.template profile.json
mv seed.sql.template seed.sql
mv rules.py.template rules.py
```

## Step 2: Fill in `profile.json`

Open `profile.json` and replace all `CHANGE_ME` placeholders:

| Field | What to Fill | Example (CDR) |
| :--- | :--- | :--- |
| `domain_id` | Short lowercase identifier | `cdr` |
| `display_name` | Human-readable name | `Call Data Records` |
| `active_rule` | Rule name as it appears in the UI | `["call data records"]` |
| `default_source_col` | Column representing the "from" entity | `CALLING_NO` |
| `default_target_col` | Column representing the "to" entity | `CALLED_NO` |
| `default_relationship` | Default Neo4j edge type | `CALLED_TO` |
| `date_column` | Primary date/time column | `START_TIME` |
| `storage_databases` | Hive/HDFS database names | `["cdrdb"]` |
| `storage_tables` | Table names within the database | `["call_records", "sms_records"]` |
| `search_columns_strict` | Exact-match search columns | `["calling_no", "called_no"]` |
| `search_columns_fuzzy` | Fuzzy/partial match columns | `["caller_name", "call_type"]` |
| `fetch_columns` | Columns to display in results (UPPERCASE) | `["CALLING_NO", "CALLED_NO", ...]` |
| `column_mapping` | Alias → canonical column name map | `{"CALLINGNUMBER": "CALLING_NO"}` |
| `neo4j_node_label` | Neo4j node label for this domain | `CallDataRecords` |
| `relationships` | List of Neo4j relationship types your rules create | `["CALL_SEQUENCE", ...]` |

## Step 3: Write the Cypher Rules (`rules.py`)

Implement two functions in `rules.py`:

### `main(driver, session_id, nodes_label, log_file, **kwargs)`

Full batch analysis — creates ALL relationships for the entire session. This runs when a user starts a new analysis session.

### `incremental(driver, session_id, nodes_label, batch_id, log_file, **kwargs)`

Incremental analysis — creates relationships only for nodes in a specific `batch_id`. This runs during real-time/streaming ingestion.

**Pattern for each rule**:
```python
session.run(f"""
    MATCH (a:{label}), (b:{label})
    WHERE a.SOURCE_COL = b.TARGET_COL
      AND <your_conditions>
    MERGE (a)-[r:YOUR_RELATIONSHIP {{session_id:$session_id}}]->(b)
    SET r.bgcolor = '#hexcolor'
""", session_id=session_id)
```

> [!TIP]
> Study `public/temp_rules/call_data_records_rules.py` and `bank_transactions_rules.py` for working examples. The CDR rules file has 14 complete Cypher patterns.

## Step 4: Generate the Database Seed (`seed.sql`)

The `seed.sql` file tells PostgreSQL what configuration to inject into `user_configs` so the cluster behaves as this domain.

Replace all `CHANGE_ME` values in the template with your `profile.json` values. The JSON structure in the SQL must match exactly.

> [!IMPORTANT]
> The `seed.sql` completely replaces any previous domain configuration. Running it on a banking cluster will convert it to your new domain. Always provision fresh clusters for new domains unless intentionally switching.

## Step 5: Provision the Cluster

Follow `docs/four_server_cluster_deployment_and_replication_handoff.md` to deploy a 4-server cluster:

1. **Server 1**: API Gateway (Flask + Nginx)
2. **Server 2**: Control Data (PostgreSQL + Redis)
3. **Server 3**: Worker (Celery-like graph analysis engine)
4. **Server 4**: Neo4j Enterprise + Maintenance

## Step 6: Set Domain Identity

On **every server** in the cluster, add to the `.env` file:

```ini
LINKX_DOMAIN=<your_domain_id>
```

This is informational for operators and future automation. The actual behavior is controlled by the database seed.

## Step 7: Apply the Database Seed

On **Server 2** (Control Data — PostgreSQL host):

```bash
cd /opt/linkx-backend-update && git pull origin main
docker exec -i linkx-postgres psql -U linkx -d linkx < domains/<your_domain>/seed.sql
```

Verify the seed took effect:
```bash
docker exec -i linkx-postgres psql -U linkx -d linkx -c \
  "SELECT user_id, config->'active_rule' AS rule, config->'default_source_col' AS source FROM user_configs;"
```

## Step 8: Copy Rule File to Upload Path

If your domain's `rules.py` should be available as an uploadable rule:

```bash
cp domains/<your_domain>/rules.py public/temp_rules/<your_domain>_rules.py
```

## Step 9: Verify All Nodes

Run drift verification on each server:

```bash
# Server 1 (API)
sudo python3 /opt/linkx-backend-update/service_factory/deploy/security/verify-linkx-server.py --role api

# Server 2 (Control Data)
sudo python3 /opt/linkx-backend-update/service_factory/deploy/security/verify-linkx-server.py --role control-data

# Server 3 (Worker)
sudo python3 /opt/linkx-backend-update/service_factory/deploy/security/verify-linkx-server.py --role worker

# Server 4 (Graph/Maintenance)
sudo python3 /opt/linkx-backend-update/service_factory/deploy/security/verify-linkx-server.py --role graph-maintenance
```

All 4 must return `failures=0`.

## Step 10: Smoke Test — End-to-End Pipeline

1. Log into the LinkX frontend.
2. Create a new analysis session.
3. Verify the session config shows your domain's `active_rule`, `default_source_col`, and `default_target_col`.
4. Upload a small sample dataset matching your domain's schema.
5. Start analysis and confirm Neo4j creates the expected relationship types.
6. Verify graph visualization shows nodes with correct labels and edges.

---

## Onboarding Checklist

| # | Step | Verification |
| :---: | :--- | :--- |
| 1 | `domains/<name>/profile.json` created | All CHANGE_ME replaced, valid JSON |
| 2 | `domains/<name>/rules.py` implemented | `main()` and `incremental()` functions exist |
| 3 | `domains/<name>/seed.sql` generated | Valid PostgreSQL, matches profile.json |
| 4 | 4-server cluster provisioned | Pre-flight audit passes |
| 5 | Code deployed (`git clone` + deploy scripts) | All 4 nodes verify `failures=0` |
| 6 | `LINKX_DOMAIN=<name>` set in `.env` files | Present on all 4 servers |
| 7 | Domain config seeded into PostgreSQL | `user_configs` shows correct `active_rule` |
| 8 | Rule file in `public/temp_rules/` | File exists and is importable |
| 9 | Network hardening (UFW) | Active, cluster subnet whitelisted |
| 10 | End-to-end smoke test | Session → upload → analysis → graph verified |

---

## Reference: Existing Domains

| Domain | Profile | Seed | Rule Module | Cluster |
| :--- | :--- | :--- | :--- | :--- |
| **Banking (BDR)** | `domains/banking/profile.json` | `domains/banking/seed.sql` | `public/temp_rules/bank_transactions_rules.py` | NBE `172.27.23.0/24` |
| **CDR** | `domains/cdr/profile.json` | `domains/cdr/seed.sql` | `public/temp_rules/call_data_records_rules.py` | Telecom `172.21.13.0/24` |
| **Social Media** | `domains/social_media/profile.json` | — | `public/temp_rules/social_media_(tweeter)_rules.py` | — |
