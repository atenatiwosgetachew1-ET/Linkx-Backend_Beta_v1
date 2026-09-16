# To Restore Phase 4

If Phase 4 changes to the Source of Truth do not work, please revert the following:

## 1. Database Schema
Drop the column `total_graph_analyzed` from `xvigilance_checkpoints`:
```sql
ALTER TABLE xvigilance_checkpoints DROP COLUMN IF EXISTS total_graph_analyzed;
```

## 2. API Endpoint Revert (`reports_api.py`)
In `reports_api.py` at `/xvigilance/health`, change `cp[2]` or `total_graph_analyzed` back to `total_records_analyzed` in the API response:
```python
cur.execute("SELECT feed_name, last_window_end, total_records_analyzed, status FROM xvigilance_checkpoints LIMIT 1")
```

## 3. Consumer Revert (`xvigilance_consumer.py`)
Remove the PostgreSQL connection and update statement at the bottom of the consumer loop (after `Ephemeral Graph Wipe`).
