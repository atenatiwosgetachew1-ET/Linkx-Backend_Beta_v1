import re

with open('/var/www/linkx-backend/service_factory/services/linkx-worker/src/batch_manager/analyzing/LA_rules_script.py', 'r') as f:
    text = f.read()

with open('/var/www/linkx-backend/generator_funcs_v4.py', 'r') as f:
    new_gens = f.read()

# Replace the block from get_smurfing_query to before def _create_transaction_indexes
pattern_gen = r"def get_smurfing_query.*?def _create_transaction_indexes"
text = re.sub(pattern_gen, new_gens + "\n\ndef _create_transaction_indexes", text, flags=re.DOTALL)

with open('/var/www/linkx-backend/service_factory/services/linkx-worker/src/batch_manager/analyzing/LA_rules_script.py', 'w') as f:
    f.write(text)
