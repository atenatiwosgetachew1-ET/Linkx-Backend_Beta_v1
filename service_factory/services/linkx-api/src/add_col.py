import sys, os
from batch_manager.utils.postgres_utils import get_postgres_connection

with get_postgres_connection() as conn:
    with conn.cursor() as cur:
        cur.execute("ALTER TABLE xvigilance_checkpoints ADD COLUMN IF NOT EXISTS total_graph_analyzed BIGINT DEFAULT 0;")
        cur.execute("UPDATE xvigilance_checkpoints SET total_graph_analyzed = total_records_analyzed;")
    conn.commit()
print("Success")
