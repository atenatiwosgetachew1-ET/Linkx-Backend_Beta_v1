import re

file_path = '/var/www/linkx-backend/service_factory/services/linkx-worker/src/linkx_worker/xvigilance_consumer.py'

with open(file_path, 'r') as f:
    content = f.read()

# Add the import
if 'get_fraud_aggregator_query' not in content:
    content = content.replace(
        'from batch_manager.analyzing.LA_rules_script import (',
        'from batch_manager.analyzing.LA_rules_script import (\n    get_fraud_aggregator_query,'
    )

# Find where HIGH_RISK_LINK finishes and append FRAUD_AGGREGATOR
aggregator_block = """
        # ---- 14. FRAUD_AGGREGATOR ----
        try:
            with driver.session() as s:
                query = get_fraud_aggregator_query(
                    label=label,
                    scope_clause_t="$session_id IS NULL OR t.session_id = $session_id",
                    session_id=sp
                )
                s.run(query, session_id=sp)
            rules_completed.append("FRAUD_AGGREGATOR")
            print("  [Rule] FRAUD_AGGREGATOR ✓", flush=True)
        except Exception as e:
            rules_failed.append(("FRAUD_AGGREGATOR", str(e)[:100]))
            print(f"  [Rule] FRAUD_AGGREGATOR ✗ {str(e)[:100]}", flush=True)

"""

if '14. FRAUD_AGGREGATOR' not in content:
    content = content.replace(
        '        # ---- 13. HIGH_RISK_LINK (from risk_entities) ----',
        '        # ---- 13. HIGH_RISK_LINK (from risk_entities) ----'
    )
    # The best way is to insert before "finally:"
    content = content.replace(
        '    finally:\n        driver.close()',
        f'{aggregator_block}\n    finally:\n        driver.close()'
    )

with open(file_path, 'w') as f:
    f.write(content)

print("Modified consumer to include aggregator.")
