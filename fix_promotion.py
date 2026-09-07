import re

filepath = "/var/www/linkx-backend/service_factory/services/linkx-worker/src/linkx_worker/xvigilance_consumer.py"
with open(filepath, "r") as f:
    code = f.read()

# Fix the INSERT statement to match the actual schema
old_insert = """                    cur.execute(\"\"\"
                        INSERT INTO link_analysis_evidence (
                            id, session_id, entity_id, is_flagged, 
                            risk_score, evidence_data, request_payload, analyzed_at
                        ) VALUES (
                            %s, %s, %s, true, 
                            99.9, %s::jsonb, '{}'::jsonb, NOW()
                        )
                    \"\"\", (str(uuid.uuid4()), session_id, anomaly["entity_id"], evidence_json))"""

new_insert = """                    cur.execute(\"\"\"
                        INSERT INTO link_analysis_evidence (
                            trace_id, session_id, entity_id, event_type, is_flagged, 
                            risk_score, evidence_data, request_payload, analyzed_at
                        ) VALUES (
                            %s, %s, %s, 'XVIGILANCE_BATCH_ANOMALY', true, 
                            99.9, %s::jsonb, '{}'::jsonb, NOW()
                        )
                    \"\"\", (str(uuid.uuid4()), 'XVIGILANCE_FINDINGS', anomaly["entity_id"], evidence_json))"""

code = code.replace(old_insert, new_insert)

with open(filepath, "w") as f:
    f.write(code)

print("Fixed promotion schema!")
